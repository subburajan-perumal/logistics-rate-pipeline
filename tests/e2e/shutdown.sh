#!/usr/bin/env bash
# Graceful-shutdown e2e (docs/PLAN.md §9.5, D-16): start a run, delete the
# pod mid-run, then assert (a) the pod exited within the grace period and
# (b) the interrupted run has NO manifest and NO promoted parts on the PVC.
set -euo pipefail
ns="${1:-rates}"
release="${2:-rates}"
svc="$release-rate-pipeline-ingestd"
pvc="$release-rate-pipeline-raw"

kubectl -n "$ns" port-forward "svc/$svc" 18080:8080 >/dev/null 2>&1 &
pfpid=$!
trap 'kill $pfpid 2>/dev/null || true' EXIT
sleep 2

echo "== starting a run"
run_id=$(curl -sf -X POST http://127.0.0.1:18080/runs -H 'content-type: application/json' -d '{}' \
  | sed -n 's/.*"run_id":"\([^"]*\)".*/\1/p')
echo "run_id=$run_id"
sleep 1

echo "== deleting the pod mid-run"
pod=$(kubectl -n "$ns" get pod -l app.kubernetes.io/component=ingestd -o jsonpath='{.items[0].metadata.name}')
start=$(date +%s)
kubectl -n "$ns" delete pod "$pod" --wait=true --timeout=90s
end=$(date +%s)
echo "pod gone in $((end - start))s (terminationGracePeriodSeconds is 60)"
if [ $((end - start)) -gt 60 ]; then echo "FAIL: exceeded terminationGracePeriodSeconds"; exit 1; fi

kill $pfpid 2>/dev/null || true
kubectl -n "$ns" rollout status "deploy/$svc" --timeout=120s

echo "== inspecting the PVC for run $run_id"
# distroless has no shell; a throwaway busybox pod mounts the same PVC.
overrides=$(printf '{"spec":{"volumes":[{"name":"raw","persistentVolumeClaim":{"claimName":"%s"}}],"containers":[{"name":"i","image":"busybox:1.37","command":["sh","-c","ls -R /data/raw/runs/run_id=%s 2>/dev/null || echo NO_RUN_DIR"],"volumeMounts":[{"name":"raw","mountPath":"/data/raw"}]}]}}' "$pvc" "$run_id")
kubectl -n "$ns" run pvc-inspect --rm -i --restart=Never --image=busybox:1.37 --overrides="$overrides" > /tmp/pvc-listing.txt
cat /tmp/pvc-listing.txt
if grep -q "_MANIFEST.json" /tmp/pvc-listing.txt; then echo "FAIL: cancelled run has a manifest"; exit 1; fi
if grep -qE 'part-[0-9]+\.jsonl\.gz$' /tmp/pvc-listing.txt; then echo "FAIL: cancelled run promoted parts"; exit 1; fi
echo "PASS: interrupted run $run_id left no manifest and no promoted parts"
