# The images that docker-build-bootstrap and docker-build-controlplane build and that
# release-manifests names in the installation manifests. The release workflow sets both to the
# release's images by digest, repository:tag@sha256:<digest>.
IMG_BOOTSTRAP ?= cluster-api-provider-chalkos-bootstrap:latest
IMG_CONTROLPLANE ?= cluster-api-provider-chalkos-controlplane:latest

# The directory release-manifests writes the installation manifests and metadata.yaml to.
RELEASE_DIR ?= out

# Registry, image tag and provider version for the local clusterctl provider repository that
# local-repo and local-repo-manifests lay out under out/repo. VERSION must be of a release series
# that metadata.yaml lists; release-please sets it to each release's version.
REGISTRY ?= localhost:5001
IMG_TAG ?= dev
# x-release-please-start-version
VERSION ?= v0.3.0
# x-release-please-end

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

# release-manifests points each manager at IMG_BOOTSTRAP and IMG_CONTROLPLANE by editing a
# throwaway copy of the kustomize tree, so the tracked config/manager/kustomization.yaml files are
# never touched. It writes the components files a clusterctl provider repository serves, and a
# copy of metadata.yaml, to RELEASE_DIR.
.PHONY: release-manifests
release-manifests: manifests ## Build the providers' installation manifests and metadata.yaml into RELEASE_DIR.
	rm -rf $(RELEASE_DIR)/.stage
	mkdir -p $(RELEASE_DIR)/.stage
	cp -r bootstrap/config $(RELEASE_DIR)/.stage/bootstrap
	cp -r controlplane/config $(RELEASE_DIR)/.stage/controlplane
	cd $(RELEASE_DIR)/.stage/bootstrap/manager && kustomize edit set image controller=$(IMG_BOOTSTRAP)
	cd $(RELEASE_DIR)/.stage/controlplane/manager && kustomize edit set image controller=$(IMG_CONTROLPLANE)
	kustomize build $(RELEASE_DIR)/.stage/bootstrap/default > $(RELEASE_DIR)/bootstrap-components.yaml
	kustomize build $(RELEASE_DIR)/.stage/controlplane/default > $(RELEASE_DIR)/control-plane-components.yaml
	cp metadata.yaml $(RELEASE_DIR)/metadata.yaml
	rm -rf $(RELEASE_DIR)/.stage

# local-repo-manifests lays out a clusterctl local filesystem repository under out/repo: one
# directory per provider, named after its clusterctl manifest label (bootstrap-chalkos,
# control-plane-chalkos), holding the version's components file and a copy of metadata.yaml, which
# release-manifests builds with the managers at IMG_BOOTSTRAP_LOCAL and IMG_CONTROLPLANE_LOCAL.
# out/clusterctl.yaml then lists both providers with an absolute file path to their components
# file, ready to pass to clusterctl as --config.
.PHONY: local-repo-manifests
local-repo-manifests: ## Lay out out/repo and out/clusterctl.yaml without building or pushing images.
	rm -rf out/repo out/clusterctl.yaml out/.local
	$(MAKE) release-manifests RELEASE_DIR=out/.local IMG_BOOTSTRAP=$(IMG_BOOTSTRAP_LOCAL) IMG_CONTROLPLANE=$(IMG_CONTROLPLANE_LOCAL)
	mkdir -p out/repo/bootstrap-chalkos/$(VERSION) out/repo/control-plane-chalkos/$(VERSION)
	mv out/.local/bootstrap-components.yaml out/repo/bootstrap-chalkos/$(VERSION)/
	mv out/.local/control-plane-components.yaml out/repo/control-plane-chalkos/$(VERSION)/
	cp out/.local/metadata.yaml out/repo/bootstrap-chalkos/$(VERSION)/
	cp out/.local/metadata.yaml out/repo/control-plane-chalkos/$(VERSION)/
	rm -rf out/.local
	printf 'providers:\n- name: chalkos\n  type: BootstrapProvider\n  url: "%s/out/repo/bootstrap-chalkos/$(VERSION)/bootstrap-components.yaml"\n- name: chalkos\n  type: ControlPlaneProvider\n  url: "%s/out/repo/control-plane-chalkos/$(VERSION)/control-plane-components.yaml"\n' $(CURDIR) $(CURDIR) > out/clusterctl.yaml

.PHONY: local-repo
local-repo: local-repo-manifests ## Build and push both managers' images to REGISTRY, on top of local-repo-manifests.
	docker build --build-arg package=bootstrap -t $(IMG_BOOTSTRAP_LOCAL) .
	docker build --build-arg package=controlplane -t $(IMG_CONTROLPLANE_LOCAL) .
	docker push $(IMG_BOOTSTRAP_LOCAL)
	docker push $(IMG_CONTROLPLANE_LOCAL)

# e2e runs the lab's scenario: it creates the workload cluster lab on the KubeVirt lab, checks it,
# and deletes it. The lab must run already: just -f lab/justfile create kubevirt providers-local.
.PHONY: e2e
e2e: ## Create and delete the lab's workload cluster on a running KubeVirt lab.
	go test -tags e2e -timeout 90m -count=1 -v ./test/e2e/

# e2e-ha scales the lab's control plane 1 → 3, checks that a fixed field's change is refused,
# scales the workers to zero, rolls the control plane with maxSurge 1 and 0, and scales it back to
# 1 on the lab's workload cluster, which it creates and deletes. It needs about 12 GiB for its VMs
# at the peak; the nightly run runs it, pull requests do not.
.PHONY: e2e-ha
e2e-ha: ## Scale and roll the lab's control plane on a running KubeVirt lab.
	go test -tags e2e_ha -run '^TestHA$$' -timeout 180m -count=1 -v ./test/e2e/
