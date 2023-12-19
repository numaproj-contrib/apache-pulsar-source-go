SHELL:=/bin/bash
TARGET_ARCH?=amd64

PACKAGE=github.com/numaproj-contrib/gcp-pub-sub-source-go
CURRENT_DIR=$(shell pwd)
DIST_DIR=${CURRENT_DIR}/dist
BINARY_NAME:=gcp-pubsub-source-go
IMAGE_NAMESPACE?=quay.io/numaproj
VERSION?=latest

override LDFLAGS += \
  -X ${PACKAGE}.version=${VERSION} \
  -X ${PACKAGE}.buildDate=${BUILD_DATE} \
  -X ${PACKAGE}.gitCommit=${GIT_COMMIT} \
  -X ${PACKAGE}.gitTreeState=${GIT_TREE_STATE}


DOCKER_PUSH?=false
BASE_VERSION:=latest
DOCKERIO_ORG=quay.io/numaio
PLATFORMS=linux/x86_64
MULTIPLE_PLATFORMS=linux/x86_64,linux/arm64,linux/amd64
TARGET=gcloud-pubsub-source


ifneq (${GIT_TAG},)
VERSION=$(GIT_TAG)
override LDFLAGS += -X ${PACKAGE}.gitTag=${GIT_TAG}
endif

IMAGE_TAG=$(TAG)
ifeq ($(IMAGE_TAG),)
    IMAGE_TAG=latest
endif

DOCKER:=$(shell command -v docker 2> /dev/null)
ifndef DOCKER
DOCKER:=$(shell command -v podman 2> /dev/null)
endif

CURRENT_CONTEXT:=$(shell [[ "`command -v kubectl`" != '' ]] && kubectl config current-context 2> /dev/null || echo "unset")
IMAGE_IMPORT_CMD:=$(shell [[ "`command -v k3d`" != '' ]] && [[ "$(CURRENT_CONTEXT)" =~ k3d-* ]] && echo "k3d image import -c `echo $(CURRENT_CONTEXT) | cut -c 5-`")
ifndef IMAGE_IMPORT_CMD
IMAGE_IMPORT_CMD:=$(shell [[ "`command -v minikube`" != '' ]] && [[ "$(CURRENT_CONTEXT)" =~ minikube* ]] && echo "minikube image load")
endif
ifndef IMAGE_IMPORT_CMD
IMAGE_IMPORT_CMD:=$(shell [[ "`command -v kind`" != '' ]] && [[ "$(CURRENT_CONTEXT)" =~ kind-* ]] && echo "kind load docker-image")
endif

.PHONY: build image lint clean test imagepush install-numaflow

build: clean
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -v -o ./dist/gcloud-pubsub-source-amd64 main.go
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -v -o ./dist/gcloud-pubsub-source-arm64 main.go

image: build
	docker buildx build --no-cache -t "$(DOCKERIO_ORG)/numaflow-go/gcloud-pubsub-source:$(IMAGE_TAG)" --platform $(PLATFORMS) --target $(TARGET) . --load

lint:
	go mod tidy
	golangci-lint run --fix --verbose --concurrency 4 --timeout 5m

test:
	@echo "Running integration tests..."
	@go test -race ./pkg/pubsubsource -run TestPubSubSource_Read

imagepush: build
	docker buildx build --no-cache -t "$(DOCKERIO_ORG)/numaflow-go/gcloud-pubsub-source:$(IMAGE_TAG)" --platform $(MULTIPLE_PLATFORMS) --target $(TARGET) . --push

dist/e2eapi:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -v -ldflags '${LDFLAGS}' -o ${DIST_DIR}/e2eapi ./test/e2e-api

.PHONY: cleanup-e2e
cleanup-e2e:
	kubectl -n numaflow-system delete svc -lnumaflow-e2e=true --ignore-not-found=true
	kubectl -n numaflow-system delete sts -lnumaflow-e2e=true --ignore-not-found=true
	kubectl -n numaflow-system delete deploy -lnumaflow-e2e=true --ignore-not-found=true
	kubectl -n numaflow-system delete cm -lnumaflow-e2e=true --ignore-not-found=true
	kubectl -n numaflow-system delete secret -lnumaflow-e2e=true --ignore-not-found=true
	kubectl -n numaflow-system delete po -lnumaflow-e2e=true --ignore-not-found=true

.PHONY: test-e2e
test-e2e:
	$(MAKE) cleanup-e2e
	$(MAKE) e2eapi-image
	kubectl -n numaflow-system delete po -lapp.kubernetes.io/component=controller-manager,app.kubernetes.io/part-of=numaflow
	kubectl -n numaflow-system delete po e2e-api-pod  --ignore-not-found=true
	cat test/manifests/e2e-api-pod.yaml |  sed 's@quay.io/numaio/numaflow-go/@$(IMAGE_NAMESPACE)/@' | sed 's/:latest/:$(VERSION)/' | kubectl -n numaflow-system apply -f -
	go generate $(shell find ./pkg/e2e/test$* -name '*.go')
	go test -v -timeout 15m -count 1 --tags test -p 1 ./test/pubsub/pubsub_e2e_test.go
	$(MAKE) cleanup-e2e

.PHONY: e2eapi-image
e2eapi-image: clean dist/e2eapi
	DOCKER_BUILDKIT=1 $(DOCKER) build . --build-arg "ARCH=amd64" --target e2eapi --tag $(IMAGE_NAMESPACE)/e2eapi:$(VERSION) --build-arg VERSION="$(VERSION)"
	@if [[ "$(DOCKER_PUSH)" = "true" ]]; then $(DOCKER) push $(IMAGE_NAMESPACE)/e2eapi:$(VERSION); fi
ifdef IMAGE_IMPORT_CMD
	$(IMAGE_IMPORT_CMD) $(IMAGE_NAMESPACE)/e2eapi:$(VERSION)
endif
clean:
	-rm -rf ${CURRENT_DIR}/dist

install-numaflow:
	kubectl create ns numaflow-system
	kubectl apply -n numaflow-system -f https://raw.githubusercontent.com/numaproj/numaflow/stable/config/install.yaml