e#!/bin/bash
echo "" > ${BASE_DIR}nohup.out
### Main file
BASE_DIR=/root/dedupe/
PY_BIN=/usr/bin/python3
nohup ${PY_BIN} ${BASE_DIR}main.py &



