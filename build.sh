#!/bin/bash
set -e
echo "=== Building SPA ==="
npx vite build apps/cf-page-vless --outDir dist/apps/cf-page-vless

echo "=== Building Node server ==="
cd apps/node-vless
npx tsc -p tsconfig.app.json
cd ../..

echo "=== Done ==="
ls -la dist/out-tsc/apps/node-vless/src/main.js
