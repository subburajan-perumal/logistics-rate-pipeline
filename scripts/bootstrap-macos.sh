#!/usr/bin/env bash
# macOS bootstrap (docs/PLAN.md Appendix C.2): Homebrew + Colima instead of
# Docker Desktop. Idempotent.
set -euo pipefail
command -v brew >/dev/null || { echo "install Homebrew first: https://brew.sh"; exit 1; }

brew install go golangci-lint kind kubectl helm kubeconform awscli gh terraform jq make openjdk@17 colima docker databricks 2>/dev/null || true
brew link --overwrite openjdk@17 >/dev/null 2>&1 || true

# Colima: a 4 GB VM is enough for the one-node kind cluster.
colima status >/dev/null 2>&1 || colima start --cpu 4 --memory 4 --disk 30

if [ -f spark/pyproject.toml ]; then
  python3 -m venv spark/.venv
  spark/.venv/bin/pip install -q -r spark/requirements.lock -e "spark[dev]"
fi

for c in "go version" "docker --version" "kind version" "kubectl version --client" "helm version --short" \
         "kubeconform -v" "aws --version" "gh --version" "terraform version" "databricks --version" "java -version"; do
  printf '  %-28s ' "$c"; $c 2>&1 | head -1 || true
done
