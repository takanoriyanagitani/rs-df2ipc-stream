#!/bin/sh

echo "example 1 JSONL to DF to IPC Stream"
./df2ipc-stream ./ex1.input.json | arrow-cat

echo "example 2 CSV to DF to IPC Stream"
./df2ipc-stream ./ex2.input.csv | arrow-cat
