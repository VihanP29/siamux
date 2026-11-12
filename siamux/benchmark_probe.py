"""Private local benchmark helper. Fixed operations over stdin, no arbitrary SQL.

Invoked only inside the isolated application container; never exposed over HTTP.
"""

import json
import sys
from datetime import datetime

from sqlalchemy import func, or_, select, text

from siamux.admission import locked_policy
from siamux.auth import issue_key
from siamux.config import get_settings
from siamux.db import Run, Tenant, session_factory

FIELDS = (
    "id",
    "status",
    "attempts",
    "created_at",
    "started_at",
    "completed_at",
    "due_at",
    "dispatch_after",
    "lease_until",
    "queue_latency_ms",
    "provider_duration_ms",
)


def perform(data):
    with session_factory()() as session:
        if data["operation"] == "bootstrap":
            with locked_policy(session) as policy:
                tenant = Tenant(name=data["name"], max_outstanding=data["limit"])
                session.add(tenant)
                session.flush()
                policy.max_outstanding = max(policy.max_outstanding, data["limit"])
                raw = issue_key(session, tenant.id)
            session.commit()
            return {
                "tenant_id": tenant.id,
                "key": raw,
                "tenant_limit": tenant.max_outstanding,
                "global_limit": policy.max_outstanding,
            }
        if data["operation"] == "snapshot":
            rows = []
            for intent in data["intents"]:
                query = select(*(getattr(Run, name) for name in FIELDS)).where(
                    Run.tenant_id == data["tenant_id"],
                    or_(Run.id == intent.get("run_id"), Run.idempotency_key == intent["key"]),
                )
                matches = session.execute(query).mappings().all()
                if len(matches) > 1:
                    raise RuntimeError("RECONCILIATION_ID_MISMATCH")
                for match in matches:
                    rows.append(
                        {
                            "intent_id": intent["intent_id"],
                            **{
                                k: v.isoformat() if isinstance(v, datetime) else v
                                for k, v in match.items()
                            },
                        }
                    )
            return rows
        if data["operation"] == "database_state":
            settings = get_settings()
            execution_settings = {
                name: getattr(settings, name)
                for name in (
                    "provider_timeout",
                    "lease_seconds",
                    "max_attempts",
                    "retry_base_seconds",
                    "dispatch_interval",
                    "redispatch_seconds",
                )
            }
            now = session.scalar(text("SELECT statement_timestamp() AT TIME ZONE 'UTC'"))
            return {
                "database_utc": now.isoformat(),
                "execution_settings": execution_settings,
                "connections": session.scalar(text("SELECT count(*) FROM pg_stat_activity")),
                "states": dict(
                    session.execute(select(Run.status, func.count()).group_by(Run.status)).all()
                ),
            }
    raise ValueError("UNKNOWN_OPERATION")


def main():
    try:
        data = json.load(sys.stdin)
        result = perform(data)
    except Exception:
        # No traceback/SQL parameter echo: stdin can contain ephemeral credentials/keys.
        print(json.dumps({"error": "PROBE_FAILED"}))
        raise SystemExit(1) from None
    print(json.dumps(result))


if __name__ == "__main__":
    main()
