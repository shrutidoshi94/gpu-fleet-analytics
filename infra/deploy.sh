#!/usr/bin/env bash
# Create a NEW stack; refuse updates to an existing stack.
set -euo pipefail
: "${VPC_ID:?Set an existing VPC ID}"
: "${SUBNET_ID:?Set a public subnet ID with an internet gateway route}"
STACK_NAME="${STACK_NAME:-gpu-fleet-repro}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-2}"
PROFILE="${PROFILE:-gpu-fleet}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
AWS=(aws --profile "$PROFILE" --region "$AWS_DEFAULT_REGION")
if "${AWS[@]}" cloudformation describe-stacks --stack-name "$STACK_NAME" >/dev/null 2>&1; then
  echo "Stack already exists; refusing an implicit infrastructure update." >&2
  exit 1
fi
"${AWS[@]}" cloudformation create-stack --stack-name "$STACK_NAME" --template-body "file://$ROOT/infra/worker.json" --capabilities CAPABILITY_IAM --parameters "ParameterKey=VpcId,ParameterValue=$VPC_ID" "ParameterKey=SubnetId,ParameterValue=$SUBNET_ID" "ParameterKey=InstanceType,ParameterValue=${INSTANCE_TYPE:-m6a.2xlarge}" "ParameterKey=RootVolumeGiB,ParameterValue=${ROOT_VOLUME_GIB:-60}" "ParameterKey=MaxBootHours,ParameterValue=${MAX_BOOT_HOURS:-48}"
"${AWS[@]}" cloudformation wait stack-create-complete --stack-name "$STACK_NAME"
BUCKET="$("${AWS[@]}" cloudformation describe-stacks --stack-name "$STACK_NAME" --query 'Stacks[0].Outputs[?OutputKey==`BucketName`].OutputValue' --output text)"
INSTANCE="$("${AWS[@]}" cloudformation describe-stacks --stack-name "$STACK_NAME" --query 'Stacks[0].Outputs[?OutputKey==`InstanceId`].OutputValue' --output text)"
git -C "$ROOT" diff --quiet HEAD || { echo "Commit local changes before packaging a deployment." >&2; exit 1; }
SOURCE_SHA="$(git -C "$ROOT" rev-parse HEAD)"
ARCHIVE="$(mktemp)"
trap 'rm -f "$ARCHIVE"' EXIT
git -C "$ROOT" archive --format=tar.gz HEAD > "$ARCHIVE"
"${AWS[@]}" s3 cp "$ARCHIVE" "s3://$BUCKET/source/$SOURCE_SHA.tar.gz" --only-show-errors
printf 'Bucket: %s\nWorker: %s\nSource SHA: %s\n' "$BUCKET" "$INSTANCE" "$SOURCE_SHA"
printf 'Wait for SSM registration, then use the bootstrap command in infra/README.md.\n'
