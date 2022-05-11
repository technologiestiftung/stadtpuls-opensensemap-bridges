#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

curl https://api.opensensemap.org/boxes/5fcf669dfab469001ce52232 | jq '.sensors[]._id'
