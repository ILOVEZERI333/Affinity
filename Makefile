# Makefile for distriscrape — build, test, and image push targets.
# TODO: Wire controller-gen, kustomize, and registry push details.

MODULE ?= github.com/affinity/distriscrape
IMG_OPERATOR ?= distriscrape-operator:latest
IMG_WORKER ?= distriscrape-worker:latest

.PHONY: build test docker-build docker-push

build:
	go build -o bin/operator ./cmd/operator
	go build -o bin/worker ./cmd/worker

test:
	go test ./...

docker-build:
	docker build -f Dockerfile.operator -t $(IMG_OPERATOR) .
	docker build -f Dockerfile.worker -t $(IMG_WORKER) .

docker-push:
	docker push $(IMG_OPERATOR)
	docker push $(IMG_WORKER)
