"""Republish due work from durable state; Redis loss never loses accepted intent."""

import time
from datetime import timedelta

from sqlalchemy import select, update

from siamux.config import get_settings
from siamux.db import Run, session_factory, utcnow
from siamux.telemetry import configure, event
from siamux.worker import eligible, execute


def dispatch_once(publish=None) -> int:
    config = get_settings()
    publish = publish or (lambda run_id: execute.apply_async(args=[run_id], retry=False))
    now = utcnow()
    with session_factory()() as session:
        candidates = session.execute(
            select(Run.id, Run.dispatch_after)
            .where(
                eligible(now),
                Run.dispatch_after <= now,
            )
            .order_by(Run.dispatch_after)
            .limit(100)
        ).all()
    count = 0
    for run_id, previous in candidates:
        # Reserve a short dispatch window before publishing. If publishing or this process
        # fails, the row becomes eligible again. Duplicate delivery is safe at the worker.
        with session_factory()() as session:
            reserved = session.execute(
                update(Run)
                .where(
                    Run.id == run_id,
                    Run.dispatch_after == previous,
                    eligible(now),
                )
                .values(dispatch_after=now + timedelta(seconds=config.redispatch_seconds))
            )
            session.commit()
        if not reserved.rowcount:
            continue
        try:
            publish(run_id)
            count += 1
        except Exception as exc:
            event("dispatch_failed", run_id=run_id, exception_type=type(exc).__name__)
            break  # Avoid hammering an unavailable broker for the entire batch.
    return count


def main():
    configure("siamux-dispatcher")
    while True:
        try:
            dispatch_once()
        except Exception as exc:
            event("dispatcher_error", exception_type=type(exc).__name__)
        time.sleep(get_settings().dispatch_interval)


if __name__ == "__main__":
    main()
