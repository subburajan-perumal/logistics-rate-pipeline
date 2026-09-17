#!/usr/bin/env bash
# Scan the working tree AND full git history for credential shapes before
# anything is pushed or the repo is flipped public (docs/PLAN.md Phase 0/11).
set -uo pipefail
pattern='AKIA[0-9A-Z]{16}|ASIA[0-9A-Z]{16}|aws_secret_access_key\s*=\s*[A-Za-z0-9/+=]{40}|dapi[0-9a-f]{32}|ghp_[A-Za-z0-9]{36}|github_pat_[A-Za-z0-9_]{22,}|AIza[0-9A-Za-z_-]{35}|pcsk_[A-Za-z0-9_]{20,}|BEGIN (RSA|EC|OPENSSH) PRIVATE KEY'
status=0
echo "== working tree"
if grep -rInE "$pattern" --exclude-dir=.git --exclude-dir=.venv --exclude-dir=node_modules --exclude=secret-scan.sh . ; then status=1; fi
echo "== git history"
if git log -p --all | grep -nE "$pattern" | grep -v secret-scan.sh; then status=1; fi
echo "== tracked env files"
if git ls-files | grep -E '(^|/)\.env(\.|$)' | grep -v '\.env\.example$'; then status=1; fi
[ $status -eq 0 ] && echo "clean" || echo "FINDINGS ABOVE — rotate and clean history before pushing"
exit $status
