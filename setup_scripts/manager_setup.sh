#!/bin/sh
BIN_DIR=~/tmp/code

mkdir -p ${BIN_DIR}
aws s3 cp s3://673333208134-very-secret-do-not-enter/full_code.zip ${BIN_DIR}
cd ${BIN_DIR}
unzip full_code.zip -d ${BIN_DIR}
python core/manager.py
