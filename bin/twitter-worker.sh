#!/bin/bash

cd "$(dirname "$0")"/../worker/
exec node index.js
