#!/bin/sh
BIN_DIR=/home/ec2-user/secret1/

REGION_NAME=us-east-1
AWS_ACCESS_KEY_ID={}
AWS_SECRET_ACCESS_KEY={}

export REGION_NAME AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
export PYTHONPATH=$PYTHONPATH:/home/ec2-user/secret1/NASA_Project
export ROY_IS_THE_BEST=true


mkdir -p /home/ec2-user/secret1/
aws s3 cp s3://673333208134-very-secret-do-not-enter/full_code.zip /home/ec2-user/secret1/
cd /home/ec2-user/secret1/
unzip full_code.zip -d /home/ec2-user/secret1/

cd NASA_Project
sudo pip install -r requirements.txt

cd core
python manager.py
