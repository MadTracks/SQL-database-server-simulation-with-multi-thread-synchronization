#!/bin/bash

./server -p 39999 -o logFile -l 4 -d dataset.csv
sleep 1
./client -i 1 -a 127.0.0.1 -p 39999 -o queries_1
./client -i 2 -a 127.0.0.1 -p 39999 -o queries_1
kill -2 `pgrep server`
