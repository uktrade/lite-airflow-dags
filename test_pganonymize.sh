#!/bin/bash
set -e
cd -- "$(dirname "$BASH_SOURCE")"
activate () {
  . .venv/bin/activate
}
activate
pganonymize --schema=anonymization_schema.yaml \
    --dbname=lite-api \
    --user=postgres \
    --password=password \
    --host=localhost \
    --port=5462 \
    --dry-run \
    --dump-file=dump.sql \
    -v