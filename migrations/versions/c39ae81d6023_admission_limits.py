"""Durable outstanding-work policy and tenant caps."""

import sqlalchemy as sa
from alembic import op

revision = "c39ae81d6023"
down_revision = "b271c8f04e12"
branch_labels = None
depends_on = None


def upgrade():
    if op.get_context().dialect.name == "sqlite":
        # Inline check permits ALTER ADD without rebuilding an FK-referenced parent.
        op.add_column(
            "tenants",
            sa.Column(
                "max_outstanding",
                sa.Integer(),
                sa.CheckConstraint("max_outstanding > 0", name="ck_tenants_capacity"),
                server_default="100",
                nullable=False,
            ),
        )
    else:
        op.add_column(
            "tenants",
            sa.Column(
                "max_outstanding",
                sa.Integer(),
                server_default="100",
                nullable=False,
            ),
        )
        op.create_check_constraint("ck_tenants_capacity", "tenants", "max_outstanding > 0")
    op.create_table(
        "admission_policy",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("max_outstanding", sa.Integer(), server_default="1000", nullable=False),
        sa.CheckConstraint("id = 1", name="ck_admission_singleton"),
        sa.CheckConstraint("max_outstanding > 0", name="ck_admission_capacity"),
    )
    op.execute(sa.text("INSERT INTO admission_policy (id, max_outstanding) VALUES (1, 1000)"))
    op.create_index("ix_runs_tenant_status", "runs", ["tenant_id", "status"])


def downgrade():
    op.drop_index("ix_runs_tenant_status", table_name="runs")
    op.drop_table("admission_policy")
    if op.get_context().dialect.name != "sqlite":
        op.drop_constraint("ck_tenants_capacity", "tenants", type_="check")
    # SQLite >= 3.35 removes the column's inline check with the column.
    op.drop_column("tenants", "max_outstanding")
