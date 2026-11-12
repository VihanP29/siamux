import json

import pytest
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from siamux.admin import main
from siamux.db import APIKey, Tenant, session_factory


def test_admin_rotation_and_listing(capsys):
    main(["tenant", "create", "--name", "application"])
    tenant_id = json.loads(capsys.readouterr().out)["tenant_id"]
    main(["key", "create", "--tenant", tenant_id])
    first = json.loads(capsys.readouterr().out)["key"]
    main(["key", "create", "--tenant", tenant_id])
    second = json.loads(capsys.readouterr().out)["key"]
    assert first != second
    main(["key", "list"])
    listed = capsys.readouterr().out
    assert first not in listed and second not in listed
    key_id = json.loads(listed)[0]["key_id"]
    main(["key", "revoke", "--key-id", key_id])
    assert first not in capsys.readouterr().out
    with session_factory()() as session:
        assert session.get(APIKey, key_id).revoked_at is not None
        assert session.get(Tenant, tenant_id).name == "application"
    main(["limits", "set", "--global", "--max-outstanding", "7"])
    capsys.readouterr()
    main(["limits", "show"])
    assert json.loads(capsys.readouterr().out)["global"] == 7


def test_failed_commit_never_prints_raw_key(identity, capsys, monkeypatch):
    def broken(self):
        raise OperationalError("INSERT", {}, Exception("private database url"))

    monkeypatch.setattr(Session, "commit", broken)
    with pytest.raises(SystemExit) as failure:
        main(["key", "create", "--tenant", identity[0]])
    assert failure.value.code == 1
    output = capsys.readouterr()
    assert not output.out and "private database url" not in output.err
    with session_factory()() as session:
        assert len(list(session.scalars(select(APIKey)))) == 1
