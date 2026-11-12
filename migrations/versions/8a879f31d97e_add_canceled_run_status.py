"""add canceled run status"""

import sqlalchemy as sa
from alembic import op

revision = "8a879f31d97e"
down_revision = "3362e1e1e030"
branch_labels = None
depends_on = None

OLD_STATUSES = "status IN ('PENDING','RUNNING','SUCCEEDED','FAILED')"
NEW_STATUSES = "status IN ('PENDING','RUNNING','SUCCEEDED','FAILED','CANCELED')"


def _recreate_sqlite(statuses):
    columns = [
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("idempotency_key", sa.String(length=100), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("request", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("output", sa.JSON(), nullable=True),
        sa.Column("error", sa.JSON(), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.Column("due_at", sa.DateTime(), nullable=False),
        sa.Column("dispatch_after", sa.DateTime(), nullable=False),
        sa.Column("lease_until", sa.DateTime(), nullable=True),
        sa.Column("claim_token", sa.String(length=36), nullable=True),
        sa.Column("trace_context", sa.JSON(), nullable=False),
        sa.Column("queue_latency_ms", sa.Float(), nullable=True),
        sa.Column("provider_duration_ms", sa.Float(), nullable=True),
    ]
    names = [column.name for column in columns]
    old_runs = sa.table("runs", *(sa.column(name) for name in names))
    replacement = op.create_table(
        "_alembic_tmp_runs",
        *columns,
        sa.CheckConstraint(statuses, name="ck_runs_status"),
        sa.CheckConstraint("attempts >= 0"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key"),
    )
    op.execute(
        replacement.insert().from_select(names, sa.select(*(old_runs.c[name] for name in names)))
    )
    op.drop_table("runs")
    op.rename_table("_alembic_tmp_runs", "runs")
    op.create_index("ix_runs_dispatch", "runs", ["status", "dispatch_after"], unique=False)


def upgrade():
    if op.get_context().dialect.name == "sqlite":
        _recreate_sqlite(NEW_STATUSES)
        return
    op.drop_constraint("runs_status_check", "runs", type_="check")
    op.create_check_constraint("ck_runs_status", "runs", NEW_STATUSES)


def downgrade():
    op.execute(sa.text("UPDATE runs SET status = 'FAILED' WHERE status = 'CANCELED'"))
    if op.get_context().dialect.name == "sqlite":
        _recreate_sqlite(OLD_STATUSES)
        return
    op.drop_constraint("ck_runs_status", "runs", type_="check")
    op.create_check_constraint("runs_status_check", "runs", OLD_STATUSES)
