NAME    = google-exchange-to-redshift-pipeline
VERSION = $$(cat VERSION)
TAG     = $$(git log -1 --pretty=%h)
IMG     = ${NAME}:${VERSION}-${TAG}
LATEST  = ${NAME}:latest
DEV     = ${NAME}:dev
DC_FILE = docker/docker-compose.yaml

build:
	docker build -t ${IMG} -f docker/Dockerfile .
	docker tag ${IMG} ${LATEST}

build-amd64:
	docker build -t ${IMG}-amd64 -f docker/Dockerfile --platform=linux/amd64 .
	docker tag ${IMG}-amd64 ${NAME}-amd64:latest

build-dev:
	docker build -t ${NAME}:${VERSION}-${TAG} .
	docker tag ${IMG} ${DEV}

run:
	docker compose --file ${DC_FILE} up

cleanup:
	docker compose --file ${DC_FILE} rm
