#!/bin/sh
# Apply schema before the server process starts. A failure is logged and the
# server still starts: history and incidents then report the store as unavailable.
if ! node dist/persistence/migrate-cli.js; then
  echo "kafka-lens: migration failed; starting without a confirmed schema" >&2
fi
exec "$@"
