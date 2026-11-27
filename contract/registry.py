"""Register and check the topic's value schema in the schema registry (ADR 0008).

    python contract/registry.py register          # set BACKWARD and register the committed schema
    python contract/registry.py check FILE        # would FILE be accepted as the next version?
    python contract/registry.py show              # latest version and compatibility mode

Redpanda ships a Confluent-compatible schema registry on port 8081 (docker-compose.yml).
`check` exits 1 when the registry says the schema is incompatible, so CI can stop a breaking
change before any producer uses it.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient  # noqa: E402

from contract.transaction import SCHEMA_PATH, SUBJECT  # noqa: E402

log = logging.getLogger(__name__)
REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
COMPATIBILITY = "BACKWARD"


def client() -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": REGISTRY_URL})


def as_schema(path: Path) -> Schema:
    return Schema(path.read_text(), schema_type="JSON")


def register(path: Path = SCHEMA_PATH) -> int:
    sr = client()
    sr.set_compatibility(SUBJECT, COMPATIBILITY)
    schema_id = sr.register_schema(SUBJECT, as_schema(path))
    log.info("%s: schema id %d, compatibility %s", SUBJECT, schema_id, COMPATIBILITY)
    return schema_id


def check(path: Path) -> bool:
    sr = client()
    ok = sr.test_compatibility(SUBJECT, as_schema(path))
    verdict = "compatible" if ok else "INCOMPATIBLE"
    log.info("%s: %s with the latest %s (%s)", path, verdict, SUBJECT, COMPATIBILITY)
    return ok


def show() -> None:
    sr = client()
    latest = sr.get_latest_version(SUBJECT)
    mode = sr.get_compatibility(SUBJECT)
    info = {
        "subject": SUBJECT,
        "version": latest.version,
        "id": latest.schema_id,
        "compatibility": mode,
    }
    log.info(json.dumps(info, indent=2))


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("register")
    c = sub.add_parser("check")
    c.add_argument("path", type=Path)
    sub.add_parser("show")
    args = parser.parse_args(argv)
    if args.command == "register":
        register()
    elif args.command == "check":
        sys.exit(0 if check(args.path) else 1)
    else:
        show()


if __name__ == "__main__":
    main()
