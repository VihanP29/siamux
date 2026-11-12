"""Real populated migration paths, isolated from fixture-created application tables."""

from uuid import uuid4

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext

from siamux.db import LEGACY_TENANT_ID, Base, engine


@pytest.fixture
def migration_db(tmp_path):
    if engine().dialect.name == "postgresql":
        schema = "migration_" + uuid4().hex
        with engine().connect() as connection:
            connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
            connection.exec_driver_sql(f'SET search_path TO "{schema}"')
            connection.commit()
            try:
                yield connection
            finally:
                connection.rollback()
                connection.exec_driver_sql("SET search_path TO public")
                connection.exec_driver_sql(f'DROP SCHEMA "{schema}" CASCADE')
                connection.commit()
    else:
        db = sa.create_engine(f"sqlite:///{tmp_path}/migrations.db")
        with db.connect() as connection:
            connection.exec_driver_sql("PRAGMA foreign_keys=ON")
            connection.commit()
            yield connection
        db.dispose()


def migrate(connection, revision, down=False):
    config = Config("alembic.ini")
    config.attributes["connection"] = connection
    (command.downgrade if down else command.upgrade)(config, revision)
    connection.commit()


def seed(connection):
    table = sa.Table("runs", sa.MetaData(), autoload_with=connection)
    from datetime import datetime

    now = datetime(2026, 1, 1)
    data = dict(
        id=str(uuid4()),
        idempotency_key="old-key",
        request_hash="d" * 64,
        request={"provider": "mock", "input": "legacy"},
        status="PENDING",
        attempts=0,
        created_at=now,
        due_at=now,
        dispatch_after=now,
        trace_context={"traceparent": "kept"},
    )
    connection.execute(table.insert().values(**data))
    connection.commit()
    return data


def row(connection):
    return dict(connection.execute(sa.text("SELECT * FROM runs")).mappings().one())


def test_populated_cancellation_roundtrip(migration_db):
    db = migration_db
    migrate(db, "3362e1e1e030")
    seed(db)
    original = row(db)
    indexes = sa.inspect(db).get_indexes("runs")
    migrate(db, "8a879f31d97e")
    assert row(db) == original
    assert sa.inspect(db).get_indexes("runs") == indexes
    assert any(
        c["column_names"] == ["idempotency_key"]
        for c in sa.inspect(db).get_unique_constraints("runs")
    )
    db.execute(sa.text("UPDATE runs SET status = 'CANCELED'"))
    db.commit()
    migrate(db, "3362e1e1e030", down=True)
    assert row(db)["status"] == "FAILED"  # Explicitly lossy downgrade.
    migrate(db, "8a879f31d97e")
    assert row(db)["status"] == "FAILED"
    assert row(db)["id"] == original["id"]


def test_populated_tenants_limits_and_roundtrip(migration_db):
    db = migration_db
    migrate(db, "8a879f31d97e")
    seed(db)
    original = row(db)
    migrate(db, "head")
    migrated = row(db)
    assert migrated.pop("tenant_id") == LEGACY_TENANT_ID
    assert migrated == original
    assert db.scalar(sa.text("SELECT count(*) FROM api_keys")) == 0
    assert db.scalar(sa.text("SELECT max_outstanding FROM tenants")) == 100
    assert db.scalar(sa.text("SELECT max_outstanding FROM admission_policy")) == 1000
    assert compare_metadata(MigrationContext.configure(db), Base.metadata) == []
    db.commit()
    migrate(db, "8a879f31d97e", down=True)
    assert row(db) == original
    migrate(db, "head")
    assert row(db)["tenant_id"] == LEGACY_TENANT_ID


def test_downgrade_refuses_cross_tenant_key_collisions(migration_db):
    db = migration_db
    migrate(db, "8a879f31d97e")
    seed(db)
    migrate(db, "head")
    tenant = str(uuid4())
    db.execute(
        sa.text(
            "INSERT INTO tenants (id, name, created_at) VALUES (:id, 'other', CURRENT_TIMESTAMP)"
        ),
        {"id": tenant},
    )
    table = sa.Table("runs", sa.MetaData(), autoload_with=db)
    data = dict(db.execute(sa.select(table)).mappings().one())
    data.update(id=str(uuid4()), tenant_id=tenant)
    db.execute(table.insert().values(**data))
    db.commit()
    migrate(db, "b271c8f04e12", down=True)
    with pytest.raises(RuntimeError, match="multiple tenants"):
        migrate(db, "8a879f31d97e", down=True)
    db.rollback()
    assert db.scalar(sa.text("SELECT count(*) FROM runs")) == 2
    assert "tenant_id" in row_columns(db)


def row_columns(db):
    return {c["name"] for c in sa.inspect(db).get_columns("runs")}


def test_backfilled_pending_work_executes_after_restart(migration_db, monkeypatch):
    from sqlalchemy.orm import sessionmaker

    import siamux.auth as auth_module
    from siamux import worker
    from siamux.auth import authenticate, issue_key
    from siamux.db import Run
    from siamux.schemas import CreateRun

    db = migration_db
    migrate(db, "8a879f31d97e")
    original = seed(db)
    request = CreateRun(provider="mock", input="legacy", mock={"delay_seconds": 0}).model_dump()
    table = sa.Table("runs", sa.MetaData(), autoload_with=db)
    db.execute(table.update().values(request=request))
    db.commit()
    migrate(db, "head")
    factory = sessionmaker(bind=db, expire_on_commit=False)
    with factory() as session:
        raw = issue_key(session, LEGACY_TENANT_ID)
        session.commit()
    db.commit()
    monkeypatch.setattr(worker, "session_factory", lambda: factory)
    monkeypatch.setattr(auth_module, "session_factory", lambda: factory)
    assert authenticate("Bearer " + raw) == LEGACY_TENANT_ID
    db.commit()
    assert worker.process_run(original["id"])
    with factory() as session:
        run = session.get(Run, original["id"])
        assert run.tenant_id == LEGACY_TENANT_ID
        assert run.status == "SUCCEEDED" and run.attempts == 1
        assert run.request_hash == original["request_hash"]
        assert run.trace_context == original["trace_context"]
    db.commit()
