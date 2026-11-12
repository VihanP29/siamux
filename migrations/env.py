from contextlib import nullcontext

from alembic import context

from siamux.db import Base, engine

if context.is_offline_mode():
    from siamux.config import get_settings

    context.configure(
        url=get_settings().database_url, target_metadata=Base.metadata, literal_binds=True
    )
    with context.begin_transaction():
        context.run_migrations()
else:
    with (
        nullcontext(context.config.attributes["connection"])
        if "connection" in context.config.attributes
        else engine().connect()
    ) as connection:
        context.configure(connection=connection, target_metadata=Base.metadata)
        with context.begin_transaction():
            context.run_migrations()
