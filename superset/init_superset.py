import json
import os
from pathlib import Path
from urllib.parse import quote_plus

from superset.app import create_app
from superset.extensions import db


def upsert_database(Database, database_name: str, sqlalchemy_uri: str, extra: str) -> None:
    database = db.session.query(Database).filter_by(database_name=database_name).one_or_none()
    if database is None:
        database = Database(database_name=database_name)
        db.session.add(database)

    setter = getattr(database, "set_sqlalchemy_uri", None)
    if callable(setter):
        setter(sqlalchemy_uri)
    else:
        database.sqlalchemy_uri = sqlalchemy_uri

    database.expose_in_sqllab = True
    database.allow_ctas = True
    database.allow_cvas = True
    database.allow_dml = False
    database.extra = extra


def build_trino_uri(catalog: str, schema: str) -> str:
    user = quote_plus(os.environ["TRINO_USER"])
    password = quote_plus(os.environ["TRINO_PASSWORD"])
    host = os.environ.get("TRINO_HOST", "trino")
    port = os.environ.get("TRINO_PORT", "8443")
    return f"trino://{user}:{password}@{host}:{port}/{catalog}/{schema}"


def main() -> None:
    app = create_app()
    catalogs_dir = Path(os.environ.get("TRINO_CATALOGS_DIR", "/opt/trino/catalog"))
    default_schema = os.environ.get("TRINO_DEFAULT_SCHEMA", "raw")
    extra = json.dumps(
        {
            "engine_params": {
                "connect_args": {
                    "http_scheme": os.environ.get("TRINO_HTTP_SCHEME", "https"),
                    "verify": os.environ.get("TRINO_VERIFY", "false").lower() == "true",
                }
            },
            "metadata_params": {},
            "version": "1.0.0",
        }
    )

    with app.app_context():
        from superset.models.core import Database

        for catalog_file in sorted(catalogs_dir.glob("*.properties")):
            catalog = catalog_file.stem
            upsert_database(
                Database,
                database_name=f"trino_{catalog}",
                sqlalchemy_uri=build_trino_uri(catalog, default_schema),
                extra=extra,
            )
        db.session.commit()


if __name__ == "__main__":
    main()
