#!/bin/sh
set -eu
npm test
npm run build
npm run acceptance
node --test synthetic_exchange/smoke.test.js
node --test synthetic_exchange/scheduler_smoke.test.js
