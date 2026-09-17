#!/usr/bin/env bash
# Idempotent toolchain bootstrap for Ubuntu 24.04 (WSL2 or a plain VM) —
# docs/PLAN.md Appendix C.2. Re-run freely; every step checks first.
set -euo pipefail

GO_VERSION="${GO_VERSION:-1.27.0}"
KIND_VERSION="${KIND_VERSION:-v0.33.0}"
KUBECTL_VERSION="${KUBECTL_VERSION:-v1.36.1}"
KUBECONFORM_VERSION="${KUBECONFORM_VERSION:-v0.7.0}"
ARCH="$(dpkg --print-architecture)"   # amd64 | arm64

log() { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }

log "apt packages"
sudo apt-get update -qq
sudo apt-get install -y -qq ca-certificates curl gnupg lsb-release make jq unzip git \
  openjdk-17-jre-headless python3 python3-venv python3-pip >/dev/null

log "Docker Engine"
if ! command -v docker >/dev/null; then
  sudo install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  echo "deb [arch=$ARCH signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
    | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
  sudo apt-get update -qq && sudo apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-buildx-plugin >/dev/null
  sudo usermod -aG docker "$USER"
  echo "   (log out/in or run: newgrp docker)"
fi

log "Go $GO_VERSION"
if ! /usr/local/go/bin/go version 2>/dev/null | grep -q "go$GO_VERSION"; then
  curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-${ARCH}.tar.gz" -o /tmp/go.tgz
  sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf /tmp/go.tgz
fi
grep -q '/usr/local/go/bin' ~/.profile || echo 'export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin' >> ~/.profile
export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin

log "golangci-lint"
command -v golangci-lint >/dev/null || \
  curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b "$HOME/go/bin"

log "kind $KIND_VERSION"
if ! kind version 2>/dev/null | grep -q "$KIND_VERSION"; then
  curl -fsSLo /tmp/kind "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-${ARCH}"
  sudo install -m 0755 /tmp/kind /usr/local/bin/kind
fi

log "kubectl $KUBECTL_VERSION"
if ! kubectl version --client 2>/dev/null | grep -q "$KUBECTL_VERSION"; then
  curl -fsSLo /tmp/kubectl "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${ARCH}/kubectl"
  sudo install -m 0755 /tmp/kubectl /usr/local/bin/kubectl
fi

log "Helm 4"
command -v helm >/dev/null || curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-4 | bash

log "kubeconform"
if ! command -v kubeconform >/dev/null; then
  curl -fsSL "https://github.com/yannh/kubeconform/releases/download/${KUBECONFORM_VERSION}/kubeconform-linux-${ARCH}.tar.gz" | tar xz -C /tmp
  sudo install -m 0755 /tmp/kubeconform /usr/local/bin/kubeconform
fi

log "AWS CLI v2"
if ! command -v aws >/dev/null; then
  curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-$(uname -m).zip" -o /tmp/awscli.zip
  (cd /tmp && unzip -qo awscli.zip && sudo ./aws/install --update)
fi

log "GitHub CLI"
if ! command -v gh >/dev/null; then
  sudo mkdir -p -m 755 /etc/apt/keyrings
  curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo tee /etc/apt/keyrings/githubcli-archive-keyring.gpg >/dev/null
  echo "deb [arch=$ARCH signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
    | sudo tee /etc/apt/sources.list.d/github-cli.list >/dev/null
  sudo apt-get update -qq && sudo apt-get install -y -qq gh >/dev/null
fi

log "Terraform"
if ! command -v terraform >/dev/null; then
  curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/hashicorp.gpg
  echo "deb [arch=$ARCH signed-by=/etc/apt/keyrings/hashicorp.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" \
    | sudo tee /etc/apt/sources.list.d/hashicorp.list >/dev/null
  sudo apt-get update -qq && sudo apt-get install -y -qq terraform >/dev/null
fi

log "Databricks CLI"
command -v databricks >/dev/null || curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh

log "Python venv for the Spark job"
if [ -f spark/pyproject.toml ]; then
  python3 -m venv spark/.venv
  spark/.venv/bin/pip install -q -r spark/requirements.lock -e "spark[dev]"
fi

log "versions (paste into docs/PLAN.md Appendix A)"
for c in "go version" "docker --version" "kind version" "kubectl version --client" "helm version --short" \
         "kubeconform -v" "aws --version" "gh --version" "terraform version" "databricks --version" "java -version"; do
  printf '  %-28s ' "$c"; $c 2>&1 | head -1 || true
done
