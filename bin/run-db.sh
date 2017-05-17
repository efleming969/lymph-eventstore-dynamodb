#!/bin/bash

docker run \
  --name dynamodb \
  -p 8000:8000 \
  -d \
  efleming969/aws-dynamo:0.1.0
