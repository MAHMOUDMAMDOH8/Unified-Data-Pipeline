import json
import os
import sys
from pathlib import Path

import requests


def main():
    registry_url = os.getenv("SCHEMA_REGISTRY_URL")
    subject = os.getenv("SCHEMA_SUBJECT", "online-store-order")
    schema_path = os.getenv("SCHEMA_PATH", str(Path(__file__).parent / "schema" / "order_log_schema.json"))

    if not registry_url:
        print("ERROR: SCHEMA_REGISTRY_URL env var is required", file=sys.stderr)
        sys.exit(1)

    schema_file = Path(schema_path)
    if not schema_file.exists():
        print(f"ERROR: Schema file not found: {schema_file}", file=sys.stderr)
        sys.exit(1)

    with open(schema_file, "r", encoding="utf-8") as f:
        schema_dict = json.load(f)

    payload = {
        "schemaType": "JSON",
        "schema": json.dumps(schema_dict)
    }

    url = f"{registry_url.rstrip('/')}/subjects/{subject}/versions"
    try:
        resp = requests.post(url, headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}, data=json.dumps(payload))
        resp.raise_for_status()
    except requests.HTTPError as e:
        print(f"ERROR: Failed to register schema: {e}\nResponse: {resp.text}", file=sys.stderr)
        sys.exit(1)

    print(f"Schema registered under subject '{subject}': {resp.json()}")


if __name__ == "__main__":
    main()
