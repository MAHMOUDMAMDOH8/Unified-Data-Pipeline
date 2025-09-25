#!/bin/bash
set -e

# Always use LocalStack endpoint + dummy credentials
AWS="aws --endpoint-url=http://localhost:4566 --region us-east-1 \
     --access-key-id test --secret-access-key test"

# Example: create bucket
$AWS s3 mb s3://incircl

# Example: upload a test file
echo "hello from localstack" | $AWS s3 cp - s3://incircl/hello.txt
