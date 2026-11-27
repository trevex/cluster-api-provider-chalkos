# Image URLs to use for docker-build-bootstrap and docker-build-controlplane.
IMG_BOOTSTRAP ?= cluster-api-provider-chalkos-bootstrap:latest
IMG_CONTROLPLANE ?= cluster-api-provider-chalkos-controlplane:latest

# Registry, image tag and provider version for the local clusterctl provider repository that
# local-repo and local-repo-manifests lay out under out/repo.
REGISTRY ?= localhost:5001
IMG_TAG ?= dev
VERSION ?= v0.1.0

IMG_BOOTSTRAP_LOCAL := $(REGISTRY)/cluster-api-provider-chalkos-bootstrap:$(IMG_TAG)
IMG_CONTROLPLANE_LOCAL := $(REGISTRY)/cluster-api-provider-chalkos-controlplane:$(IMG_TAG)

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
	kustomize build controlplane/config/default > out/control-plane-components.yaml

# local-repo-manifests lays out a clusterctl local filesystem repository under out/repo: one
# directory per provider, named after its clusterctl manifest label (bootstrap-chalkos,
# control-plane-chalkos), holding the version's components file and a copy of metadata.yaml.
# It points each manager at IMG_BOOTSTRAP_LOCAL/IMG_CONTROLPLANE_LOCAL by editing a throwaway
# copy of the kustomize tree, so the tracked config/manager/kustomization.yaml files are never
# touched. out/clusterctl.yaml then lists both providers with an absolute file path to their
# components file, ready to pass to clusterctl as --config.
.PHONY: local-repo-manifests
local-repo-manifests: manifests ## Lay out out/repo and out/clusterctl.yaml without building or pushing images.
	rm -rf out/repo out/clusterctl.yaml out/.repo-stage
	mkdir -p out/repo/bootstrap-chalkos/$(VERSION) out/repo/control-plane-chalkos/$(VERSION) out/.repo-stage
	cp -r bootstrap/config out/.repo-stage/bootstrap
	cp -r controlplane/config out/.repo-stage/controlplane
	cd out/.repo-stage/bootstrap/manager && kustomize edit set image controller=$(IMG_BOOTSTRAP_LOCAL)
	cd out/.repo-stage/controlplane/manager && kustomize edit set image controller=$(IMG_CONTROLPLANE_LOCAL)
	kustomize build out/.repo-stage/bootstrap/default > out/repo/bootstrap-chalkos/$(VERSION)/bootstrap-components.yaml
	kustomize build out/.repo-stage/controlplane/default > out/repo/control-plane-chalkos/$(VERSION)/control-plane-components.yaml
	cp metadata.yaml out/repo/bootstrap-chalkos/$(VERSION)/metadata.yaml
	cp metadata.yaml out/repo/control-plane-chalkos/$(VERSION)/metadata.yaml
	rm -rf out/.repo-stage
	printf 'providers:\n- name: chalkos\n  type: BootstrapProvider\n  url: "%s/out/repo/bootstrap-chalkos/$(VERSION)/bootstrap-components.yaml"\n- name: chalkos\n  type: ControlPlaneProvider\n  url: "%s/out/repo/control-plane-chalkos/$(VERSION)/control-plane-components.yaml"\n' $(CURDIR) $(CURDIR) > out/clusterctl.yaml

.PHONY: local-repo
local-repo: local-repo-manifests ## Build and push both managers' images to REGISTRY, on top of local-repo-manifests.
	docker build --build-arg package=bootstrap -t $(IMG_BOOTSTRAP_LOCAL) .
	docker build --build-arg package=controlplane -t $(IMG_CONTROLPLANE_LOCAL) .
	docker push $(IMG_BOOTSTRAP_LOCAL)
	docker push $(IMG_CONTROLPLANE_LOCAL)
