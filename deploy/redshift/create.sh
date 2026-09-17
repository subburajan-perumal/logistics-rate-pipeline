#!/usr/bin/env bash
# Redshift Serverless at the cost floor (docs/PLAN.md D-33, A7):
#   base = max = 4 RPU → $1.50/hour while active, $0 idle.
#   The workgroup's default IAM role reads the bucket for COPY.
# Usage: deploy/redshift/create.sh <bucket>    (destroy.sh tears it all down)
set -euo pipefail
bucket="${1:?bucket}"
region="${AWS_REGION:-us-east-1}"
ns="${REDSHIFT_NAMESPACE:-rates-ns}"
wg="${REDSHIFT_WORKGROUP:-rates-wg}"
db="${REDSHIFT_DATABASE:-rates}"
role_name="rates-redshift-s3"
account=$(aws sts get-caller-identity --query Account --output text)

echo "== IAM role $role_name (S3 read on $bucket)"
aws iam get-role --role-name "$role_name" >/dev/null 2>&1 || aws iam create-role --role-name "$role_name" \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"redshift.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
  --tags Key=project,Value=logistics-rate-pipeline >/dev/null
aws iam put-role-policy --role-name "$role_name" --policy-name s3-read --policy-document "{
  \"Version\":\"2012-10-17\",\"Statement\":[
    {\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\",\"s3:ListBucket\"],\"Resource\":[\"arn:aws:s3:::$bucket\",\"arn:aws:s3:::$bucket/*\"]}]}"
role_arn="arn:aws:iam::$account:role/$role_name"

echo "== namespace $ns"
aws redshift-serverless get-namespace --namespace-name "$ns" --region "$region" >/dev/null 2>&1 || \
  aws redshift-serverless create-namespace --namespace-name "$ns" --db-name "$db" \
    --iam-roles "$role_arn" --default-iam-role-arn "$role_arn" \
    --tags key=project,value=logistics-rate-pipeline --region "$region" >/dev/null

echo "== workgroup $wg (4 RPU base, 4 RPU max)"
aws redshift-serverless get-workgroup --workgroup-name "$wg" --region "$region" >/dev/null 2>&1 || \
  aws redshift-serverless create-workgroup --workgroup-name "$wg" --namespace-name "$ns" \
    --base-capacity 4 --max-capacity 4 --no-publicly-accessible \
    --tags key=project,value=logistics-rate-pipeline --region "$region" >/dev/null

echo "waiting for the workgroup to be AVAILABLE"
until [ "$(aws redshift-serverless get-workgroup --workgroup-name "$wg" --region "$region" --query workgroup.status --output text)" = "AVAILABLE" ]; do sleep 15; done
echo "ready: REDSHIFT_WORKGROUP=$wg REDSHIFT_DATABASE=$db REDSHIFT_IAM_ROLE=$role_arn"
