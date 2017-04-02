#!/bin/bash
# A simple script
export GOROOT=/usr/local/go/
export PATH=$PATH:$GOROOT/bin
# Change the <abs path> to the absolute path to the worker folder
export GOPATH=<abs path>/worker
# Change the <abs path> to the absolute path to the worker file
go run <abs path>/worker.go
