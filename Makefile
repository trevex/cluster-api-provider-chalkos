# Image URLs to use for docker-build-bootstrap and docker-build-controlplane.
IMG_BOOTSTRAP ?= cluster-api-provider-chalkos-bootstrap:latest
IMG_CONTROLPLANE ?= cluster-api-provider-chalkos-controlplane:latest

# The Kubernetes version envtest's control plane binaries are fetched for.
ENVTEST_K8S_VERSION ?= 1.37

.PHONY: generate
generate: ## Generate DeepCopy, DeepCopyInto and DeepCopyObject methods for both providers' APIs.
	controller-gen object:headerFile="hack/boilerplate.go.txt" paths="./bootstrap/..."
	controller-gen object:headerFile="hack/boilerplate.go.txt" paths="./controlplane/..."

.PHONY: manifests
manifests: ## Generate CustomResourceDefinition, RBAC and WebhookConfiguration manifests.
	controller-gen rbac:roleName=manager-role crd webhook paths="./bootstrap/..." \
		output:crd:artifacts:config=bootstrap/config/crd/bases \
		output:rbac:artifacts:config=bootstrap/config/rbac \
		output:webhook:artifacts:config=bootstrap/config/webhook
	controller-gen rbac:roleName=manager-role crd webhook paths="./controlplane/..." \
		output:crd:artifacts:config=controlplane/config/crd/bases \
		output:rbac:artifacts:config=controlplane/config/rbac \
		output:webhook:artifacts:config=controlplane/config/webhook

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: lint
lint: ## Run golangci-lint against code.
	golangci-lint run

.PHONY: test
test: ## Run the unit and envtest suites.
	KUBEBUILDER_ASSETS="$$(setup-envtest use $(ENVTEST_K8S_VERSION) -p path)" go test ./...

.PHONY: build
build: ## Build the bootstrap and control-plane manager binaries.
	go build ./bootstrap ./controlplane

.PHONY: docker-build-bootstrap
docker-build-bootstrap: ## Build the bootstrap manager's image.
	docker build --build-arg package=bootstrap -t ${IMG_BOOTSTRAP} .

.PHONY: docker-build-controlplane
docker-build-controlplane: ## Build the control-plane manager's image.
	docker build --build-arg package=controlplane -t ${IMG_CONTROLPLANE} .

.PHONY: release-manifests
release-manifests: manifests ## Build the providers' installation manifests into out/.
	mkdir -p out
	kustomize build bootstrap/config/default > out/bootstrap-components.yaml
	kustomize build controlplane/config/default > out/controlplane-components.yaml
