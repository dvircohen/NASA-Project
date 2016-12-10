#!/bin/sh
BIN_DIR=/home/ec2-user/secret1/

REGION_NAME=us-east-1
AWS_ACCESS_KEY_ID=AKIAJRJLQHBZH3PC7QUQ
AWS_SECRET_ACCESS_KEY=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

export REGION_NAME AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
export PYTHONPATH=$PYTHONPATH:/home/ec2-user/secret1/NASA_Project


mkdir -p ${BIN_DIR}
aws s3 cp s3://673333208134-very-secret-do-not-enter/full_code.zip ${BIN_DIR}
cd ${BIN_DIR}
unzip full_code.zip -d ${BIN_DIR}

cd NASA_Project
sudo pip install -r requirements.txt

cd core
python worker.py
