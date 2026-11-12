"""Local administration through direct database access: python -m siamux.admin."""

import argparse
import json
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from siamux.admission import locked_policy
from siamux.auth import issue_key
from siamux.db import APIKey, Tenant, session_factory, utcnow


def positive(value):
    value = int(value)
    if value < 1:
        raise argparse.ArgumentTypeError("limit must be positive")
    return value


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="resource", required=True)
    tenant = commands.add_parser("tenant").add_subparsers(dest="action", required=True)
    tenant.add_parser("create").add_argument("--name", required=True)
    tenant.add_parser("list")
    keys = commands.add_parser("key").add_subparsers(dest="action", required=True)
    keys.add_parser("create").add_argument("--tenant", required=True, type=UUID)
    keys.add_parser("revoke").add_argument("--key-id", required=True, type=UUID)
    keys.add_parser("list")
    limits = commands.add_parser("limits").add_subparsers(dest="action", required=True)
    setting = limits.add_parser("set")
    target = setting.add_mutually_exclusive_group(required=True)
    target.add_argument("--tenant", type=UUID)
    target.add_argument("--global", dest="global_limit", action="store_true")
    setting.add_argument("--max-outstanding", required=True, type=positive)
    limits.add_parser("show")
    args = parser.parse_args(argv)
    try:
        with session_factory()() as session:
            if args.resource == "tenant":
                if args.action == "create":
                    if not args.name.strip() or len(args.name) > 100:
                        parser.error("name must contain 1–100 nonblank characters")
                    row = Tenant(name=args.name)
                    session.add(row)
                    session.commit()
                    result = {"tenant_id": row.id, "name": row.name}
                else:
                    result = [
                        {"tenant_id": t.id, "name": t.name} for t in session.scalars(select(Tenant))
                    ]
            elif args.resource == "key":
                if args.action == "create":
                    tenant_id = str(args.tenant)
                    if session.get(Tenant, tenant_id) is None:
                        parser.error("tenant not found")
                    raw = issue_key(session, tenant_id)
                    session.commit()
                    # The raw key is printed only after a successful durable commit.
                    result = {"key": raw}
                elif args.action == "revoke":
                    key = session.get(APIKey, str(args.key_id))
                    if key is None:
                        parser.error("key not found")
                    key.revoked_at = key.revoked_at or utcnow()
                    session.commit()
                    result = {"key_id": key.id, "revoked": True}
                else:
                    result = [
                        {
                            "key_id": k.id,
                            "tenant_id": k.tenant_id,
                            "revoked": k.revoked_at is not None,
                        }
                        for k in session.scalars(select(APIKey))
                    ]
            else:
                with locked_policy(session) as policy:
                    tenants = list(
                        session.scalars(select(Tenant).order_by(Tenant.id).with_for_update())
                    )
                    if args.action == "set":
                        if args.global_limit:
                            policy.max_outstanding = args.max_outstanding
                        else:
                            tenant = next((t for t in tenants if t.id == str(args.tenant)), None)
                            if tenant is None:
                                parser.error("tenant not found")
                            tenant.max_outstanding = args.max_outstanding
                    result = {
                        "global": policy.max_outstanding,
                        "tenants": {t.id: t.max_outstanding for t in tenants},
                    }
                session.commit()
            print(json.dumps(result))
    except SQLAlchemyError:
        parser.exit(1, "Database operation failed; no credential is displayed.\n")


if __name__ == "__main__":
    main()
