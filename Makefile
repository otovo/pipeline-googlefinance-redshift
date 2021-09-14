NAME      = google-exchange-to-redshift-pipeline
VERSION   = $$(cat VERSION)
TAG       = $$(git log -1 --pretty=%h)
DC_FILE   = docker/docker-compose.yaml

build:
	docker build -t ${NAME}:${VERSION}-${TAG} -f docker/Dockerfile .
	docker tag ${NAME}:${VERSION}-${TAG} ${NAME}:latest

build-amd64:
	docker build -t ${NAME}-amd64:${VERSION}-${TAG} -f docker/Dockerfile --platform=linux/amd64 .
	docker tag ${NAME}-amd64:${VERSION}-${TAG} ${NAME}-amd64:latest

build-aws-lambda:
	docker build -t ${NAME}-aws-lambda:${VERSION}-${TAG} -f docker/Dockerfile-aws-lambda --platform=linux/amd64 .
	docker tag ${NAME}-aws-lambda:${VERSION}-${TAG} ${NAME}-aws-lambda:latest

run:
	docker compose --file ${DC_FILE} up

cleanup:
	docker compose --file ${DC_FILE} rm
