#!/usr/bin/env bash
set -euo pipefail

# Packs the library, installs it as a reader would, and runs every example in the docs and
# examples/ with Node.js alone.
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
consumer_dir="$(mktemp -d "${TMPDIR:-/tmp}/effect-delta.XXXXXX")"
trap 'rm -rf "$consumer_dir"' EXIT

pnpm --dir "$root" --filter @jpowersdev/effect-delta pack --pack-destination "$consumer_dir"

cat > "$consumer_dir/package.json" <<'JSON'
{
  "private": true,
  "type": "module",
  "dependencies": {
    "@effect/platform-node": "4.1.1",
    "@jpowersdev/effect-delta": "file:./jpowersdev-effect-delta-0.1.0.tgz",
    "effect": "4.1.1"
  }
}
JSON
pnpm --dir "$consumer_dir" install --ignore-scripts --lockfile=false

node "$root/scripts/doc-examples.mjs" "$root" "$consumer_dir/examples"
cp "$root"/examples/*.ts "$consumer_dir/examples/"

cd "$consumer_dir/examples"
for example in *.ts; do
  echo "--- $example"
  timeout 60 node "$example" > /dev/null || { echo "failed: $example"; exit 1; }
done
echo "All examples ran."
