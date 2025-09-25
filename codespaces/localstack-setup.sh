#!/bin/bash
set -e

echo "[INFO] Running LocalStack init script..."

AWS="aws --endpoint-url=http://localhost:4566 --region us-east-1 \
     --access-key-id test --secret-access-key test"

# Create bucket if it does not exist
if ! $AWS s3api head-bucket --bucket incircl 2>/dev/null; then
  echo "[INFO] Creating bucket incircl"
  $AWS s3api create-bucket --bucket incircl --region us-east-1
else
  echo "[INFO] Bucket incircl already exists"
fi

# Bronze
$AWS s3api put-object --bucket incircl --key "bronze_layer/batch_job/"
$AWS s3api put-object --bucket incircl --key "bronze_layer/stream_job/"

# Silver
$AWS s3api put-object --bucket incircl --key "silver_layer/processing_data/"
$AWS s3api put-object --bucket incircl --key "silver_layer/valid_data/"
$AWS s3api put-object --bucket incircl --key "silver_layer/reject_data/"

# Gold
$AWS s3api put-object --bucket incircl --key "gold_layer/valid_data/"

echo "[INFO] Init script completed ✅"
