# cluster-api-provider-chalkos

Cluster API bootstrap and control-plane providers for [chalkos](https://github.com/trevex/chalkos),
the API-driven, immutable NixOS-based Linux distribution. The bootstrap provider turns a Machine's
chalkos node into a Kubernetes node; the control-plane provider turns a set of Machines into a
control plane, keeps its kubeconfig Secret current, and scales it the way Cluster API scales any
control plane. See [`lab/`](lab/) for a KubeVirt-based lab that runs both providers against
chalkos virtual machines end to end.

## Building a local provider repository

clusterctl installs providers from a provider repository: a versioned set of `metadata.yaml` and
components files, addressed either on GitHub or on the local filesystem. `make local-repo` builds
this repository on the local filesystem, so a management cluster can run both providers without a
release:

```sh
make local-repo REGISTRY=localhost:5001 IMG_TAG=dev VERSION=v0.1.0
```

This builds the bootstrap and control-plane manager images, pushes them to `REGISTRY`, and lays
out `out/repo/bootstrap-chalkos/$(VERSION)` and `out/repo/control-plane-chalkos/$(VERSION)`, each
with a components file and a copy of `metadata.yaml`. It also writes `out/clusterctl.yaml`, a
clusterctl configuration file with `providers:` entries pointing at those components files by
absolute path. `REGISTRY`, `IMG_TAG` and `VERSION` default to `localhost:5001`, `dev` and
`v0.1.0`; the registry must be reachable both from the machine running `make`, to push the
images, and from the management cluster, to pull them.

`make local-repo-manifests` runs the same layout step without building or pushing images. It
produces the same `out/repo` and `out/clusterctl.yaml`, with the managers pointing at images that
may not exist yet in the registry, which is enough to check the generated manifests without
Docker.

`clusterctl init` reads `out/clusterctl.yaml` through its `--config` flag, which adds the chalkos
providers to clusterctl's built-in provider list rather than replacing it. Pointed at a management
cluster that can reach `REGISTRY`, and paired with an infrastructure provider such as
[CAPK](https://github.com/kubernetes-sigs/cluster-api-provider-kubevirt), this installs every
provider the [lab](lab/) needs:

```sh
clusterctl init \
  --config out/clusterctl.yaml \
  --bootstrap chalkos \
  --control-plane chalkos \
  --infrastructure kubevirt
```

## End-to-end test

`make e2e` runs the lab's scenario against a running lab: it creates the workload cluster `lab`
of one control plane and one worker, waits for the control plane to become available and both
nodes to become Ready, checks the providers' statuses and the kubeconfig's certificate, and then
deletes the cluster and waits until its Machines and VMs are gone. It skips unless the lab runs:

```sh
just -f lab/justfile registry create kubevirt images providers
make e2e
just -f lab/justfile destroy
```

A run that stops early still deletes the cluster, after it logged the cluster's state on failure.
`E2E_KEEP=1 make e2e` keeps the cluster of such a run to look at.
