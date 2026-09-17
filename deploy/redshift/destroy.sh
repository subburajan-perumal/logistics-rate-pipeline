#!/usr/bin/env bash
# Tear down everything create.sh made so the standing cost is $0 (Phase 9 acceptance).
set -euo pipefail
region="${AWS_REGION:-us-east-1}"
ns="${REDSHIFT_NAMESPACE:-rates-ns}"
wg="${REDSHIFT_WORKGROUP:-rates-wg}"
role_name="rates-redshift-s3"

aws redshift-serverless delete-workgroup --workgroup-name "$wg" --region "$region" >/dev/null 2>&1 || true
echo "waiting for workgroup deletion"
while aws redshift-serverless get-workgroup --workgroup-name "$wg" --region "$region" >/dev/null 2>&1; do sleep 15; done
aws redshift-serverless delete-namespace --namespace-name "$ns" --region "$region" >/dev/null 2>&1 || true
aws iam delete-role-policy --role-name "$role_name" --policy-name s3-read >/dev/null 2>&1 || true
aws iam delete-role --role-name "$role_name" >/dev/null 2>&1 || true
echo "deleted: workgroup $wg, namespace $ns, role $role_name"
