.ONESHELL:
SHELL = /bin/bash

UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

OS := linux
ifeq ($(UNAME_S),Linux)
  OS := linux
endif
ifeq ($(UNAME_S),Darwin)
  OS := darwin
endif

ARCH := amd64
ifeq ($(UNAME_M), arm64)
  ARCH := arm64
endif

build:
        GOOS=${OS} GARCH=${ARCH} go build .

my_binary: build

darwin_arm64:
  GOOS=darwin GARCH=arm64 go build .

darwin_amd64:
        GOOS=darwin GARCH=amd64 go build .

linux_amd64:
  GOOS=linux GARCH=amd64 go build .