import os

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import delete

from siamux.api import app
from siamux.auth import issue_key
from siamux.config import get_settings
from siamux.db import AdmissionPolicy, APIKey, Base, Run, Tenant, engine, session_factory


@pytest.fixture(autouse=True)
def database(tmp_path, monkeypatch):
    # CI may point at a dedicated PostgreSQL test database. Never use the app's DB implicitly.
    url = os.environ.get("SIAMUX_TEST_DATABASE_URL", f"sqlite:///{tmp_path}/test.db")
    monkeypatch.setenv("SIAMUX_DATABASE_URL", url)
    monkeypatch.setenv("SIAMUX_PROVIDER_TIMEOUT", "0.05")
    monkeypatch.setenv("SIAMUX_RETRY_BASE_SECONDS", "0.01")
    get_settings.cache_clear()
    engine.cache_clear()
    if engine().dialect.name == "postgresql":
        command.upgrade(Config("alembic.ini"), "head")
    else:
        Base.metadata.create_all(engine())
    with session_factory()() as session:
        session.execute(delete(Run))
        session.execute(delete(APIKey))
        session.execute(delete(Tenant))
        session.execute(delete(AdmissionPolicy))
        session.add(AdmissionPolicy(id=1, max_outstanding=1000))
        session.commit()
    yield
    engine().dispose()
    engine.cache_clear()
    get_settings.cache_clear()


@pytest.fixture
def identity():
    with session_factory()() as session:
        tenant = Tenant(name="test")
        session.add(tenant)
        session.flush()
        raw = issue_key(session, tenant.id)
        session.commit()
        return tenant.id, raw


@pytest.fixture
def anonymous():
    with TestClient(app) as client:
        yield client


@pytest.fixture
def client(identity):
    with TestClient(app, headers={"Authorization": f"Bearer {identity[1]}"}) as client:
        yield client


@pytest.fixture
def submit(client):
    def create(key="test-key", **overrides):
        payload = {"provider": "mock", "input": "hello", "mock": {"delay_seconds": 0.0}}
        payload.update(overrides)
        return client.post("/v1/runs", json=payload, headers={"Idempotency-Key": key})

    return create
