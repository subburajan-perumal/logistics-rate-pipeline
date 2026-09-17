# Every recurring action in one place (docs/PLAN.md D-37). Runs in WSL/Linux/macOS.
SHELL := /bin/bash
VERSION ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
IMG_INGESTD ?= ghcr.io/subburajan-perumal/logistics-rate-pipeline/ingestd
IMG_MOCK    ?= ghcr.io/subburajan-perumal/logistics-rate-pipeline/mocksources
NS ?= rates
RELEASE ?= rates
RUN_ID ?= fixture
ENV_FILE ?= .env.kind
export MOCKSOURCES_URL ?= http://127.0.0.1:18081
export EVENTIDE_API_KEY ?= eventide-demo-key
PY ?= spark/.venv/bin/python

.PHONY: help build test test-race lint fixture bench mock-up mock-down run-local \
        docker kind-up kind-down deploy-kind run-kind test-kind \
        spark-venv spark-test spark-local helm-lint clean

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-14s %s\n",$$1,$$2}'

build: ## build ingestd and mocksources into bin/
	go build -trimpath -ldflags="-s -w -X main.version=$(VERSION)" -o bin/ ./cmd/...

test: ## go unit tests
	go test ./...

test-race: ## go unit tests under the race detector
	go test -race -count=1 ./...

lint: ## go vet + golangci-lint + ruff
	go vet ./... && golangci-lint run ./... && $(PY) -m ruff check spark/src spark/tests

fixture: ## regenerate the Spark fixture and port table from the mock feeds
	go run ./cmd/mocksources dump --recipes ./recipes --out spark/tests/fixtures/raw
	go run ./cmd/mocksources ports --out spark/config/ports.csv

mock-up: build ## start mocksources on :18081 with documented latencies
	@mkdir -p bench/tmp
	@(./bin/mocksources serve --listen 127.0.0.1:18081 > bench/tmp/mock.log 2>&1 &); sleep 1
	@curl -sf http://127.0.0.1:18081/healthz >/dev/null && echo "mocksources up on :18081"

mock-down: ## stop mocksources
	@pkill -f "mocksources serve" || true

run-local: build ## one ingest run to data/raw with 8 workers
	./bin/ingestd run --recipes ./recipes --sink fs --out data/raw --workers 8 --as-of 2026-09-01

bench: build ## worker-pool benchmark -> bench/results/<ts>-pool.{json,md}
	@mkdir -p bench/results bench/tmp; ts=$$(date -u +%Y%m%dT%H%M%SZ); \
	./bin/ingestd bench --recipes ./recipes --sink fs --out bench/tmp/bench --workers-list 1,2,4,8,16 --reps 5 \
	  --as-of 2026-09-01 --json bench/results/$$ts-pool.json --md bench/results/$$ts-pool.md --log-level error

docker: ## build both images locally (tag dev)
	docker build --target ingestd     --build-arg VERSION=$(VERSION) -t $(IMG_INGESTD):dev .
	docker build --target mocksources --build-arg VERSION=$(VERSION) -t $(IMG_MOCK):dev .
	docker images --format "{{.Repository}}:{{.Tag}} {{.Size}}" | grep -E "ingestd|mocksources"

kind-up: ## create the one-node kind cluster + ingress-nginx
	kind create cluster --config deploy/kind/cluster.yaml
	kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml
	kubectl -n ingress-nginx wait --for=condition=ready pod -l app.kubernetes.io/component=controller --timeout=180s

kind-down: ## delete the kind cluster (end of every session)
	kind delete cluster --name rates

deploy-kind: docker ## load images and install the chart on kind
	kind load docker-image $(IMG_INGESTD):dev $(IMG_MOCK):dev --name rates
	kubectl get ns $(NS) >/dev/null 2>&1 || kubectl create ns $(NS)
	kubectl -n $(NS) get secret ingestd-secrets >/dev/null 2>&1 || \
	  kubectl -n $(NS) create secret generic ingestd-secrets --from-env-file=$(ENV_FILE)
	helm upgrade --install $(RELEASE) deploy/helm/rate-pipeline -n $(NS) \
	  -f deploy/helm/rate-pipeline/values-kind.yaml --wait --timeout 180s

run-kind: ## start a run through the Ingress
	@mkdir -p bench/tmp
	curl -sf -X POST http://ingest.localtest.me/runs -H "content-type: application/json" -d "{}" | tee bench/tmp/last-run.json
	@echo; echo "poll: curl http://ingest.localtest.me/runs"

test-kind: ## helm test + graceful-shutdown e2e on kind
	helm test $(RELEASE) -n $(NS) --logs
	bash tests/e2e/shutdown.sh $(NS) $(RELEASE)

spark-venv: ## create spark/.venv with pinned deps
	python3 -m venv spark/.venv && $(PY) -m pip install -q -r spark/requirements.lock -e "spark[dev]"

spark-test: ## pytest for the normalizer
	cd spark && ../$(PY) -m pytest tests -q

spark-local: ## normalize data/raw/runs/run_id=$(RUN_ID) locally
	$(PY) -m rate_normalizer --input data/raw/runs/run_id=$(RUN_ID) --output data/out/run_id=$(RUN_ID) \
	  --as-of 2026-09-01 --master "local[2]"

helm-lint: ## helm lint + kubeconform for both profiles
	helm lint deploy/helm/rate-pipeline -f deploy/helm/rate-pipeline/values-kind.yaml
	helm template $(RELEASE) deploy/helm/rate-pipeline -n $(NS) -f deploy/helm/rate-pipeline/values-kind.yaml | kubeconform -strict -summary
	helm template $(RELEASE) deploy/helm/rate-pipeline -n $(NS) -f deploy/helm/rate-pipeline/values-eks.yaml --set ingestd.s3.bucket=b | kubeconform -strict -summary

clean: ## remove build outputs and local data
	rm -rf bin data bench/tmp
