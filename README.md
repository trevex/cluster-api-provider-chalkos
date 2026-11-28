# cluster-api-provider-chalkos

[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/trevex/cluster-api-provider-chalkos/badge)](https://scorecard.dev/viewer/?uri=github.com/trevex/cluster-api-provider-chalkos)

Cluster API bootstrap and control-plane providers for [chalkos](https://github.com/trevex/chalkos),
the API-driven, immutable NixOS-based Linux distribution. The bootstrap provider installs a
Machine's chalkos node over chalkd's API. The control-plane provider creates a cluster's
control-plane Machines one at a time, bootstraps the cluster on the first, and keeps the cluster's
kubeconfig Secret current. See [`lab/`](lab/) for a KubeVirt-based lab that runs both providers
against chalkos virtual machines end to end, from released images without building anything.

## Status and limitations

The providers are alpha. Their API is `v1alpha1` and may change between releases without a
conversion path.

What works:

- Creating a cluster: the control-plane Machines one at a time, then the workers of
  MachineDeployments.
- Scaling the control plane up, one Machine at a time, each after the one before it joined etcd.
- Removing a Machine: a pre-terminate hook takes its node out of etcd and revokes it before
  Cluster API deletes the Machine's infrastructure, as [Removing Machines](#removing-machines)
  describes. This covers scaling a MachineDeployment down and deleting a Machine by hand.
- Scaling the control plane down, one Machine at a time, oldest first, once every Machine joined
  and etcd lists one healthy member per Machine.
- Replacing the only Machine of a one-replica control plane by deleting it: the replacement
  joins etcd before the old Machine leaves it.
- Deleting a cluster: the control plane waits until the cluster's worker Machines and
  MachinePools are deleted, then deletes its own Machines. Deleting the Cluster deletes them in
  this order, and no node leaves etcd or is revoked. A ChalkosControlPlane deleted on its own
  deletes no workers: it waits until someone else deletes them.

What does not work yet:

- Rollouts and upgrades of the control plane. A changed `spec.version` marks the Machines as not
  up to date, and a changed `spec.image` does not affect Machines that exist; neither replaces a
  Machine.
- Remediation of control-plane Machines by a MachineHealthCheck. Cluster API leaves an unhealthy
  Machine to its owner, and marks it with `OwnerRemediated` set to `False`; the
  ChalkosControlPlane does not act on that, so the Machine stays. Delete the Machine to replace it.
  A MachineHealthCheck remediates the workers of a MachineDeployment as usual: their MachineSet
  deletes the unhealthy Machine, whose hook revokes its node, and creates a new one.
- Rotation of the cluster's CAs and keys. The kubeconfig Secret's client certificate is the only
  credential the providers renew.

The end-to-end test covers one control plane and one worker on KubeVirt through
[CAPK](https://github.com/kubernetes-sigs/cluster-api-provider-kubevirt), with the workers scaled
to two and back to one; `make e2e-ha` scales the control plane to three and back to one, as
[End-to-end test](#end-to-end-test) describes. Other infrastructure providers, ClusterClass
topologies and worker rollouts are untested. An infrastructure provider must report each
Machine's `providerID` and an `InternalIP` address that the managers can reach: the providers
talk to chalkd on that address.

## How the providers fit together

The two providers ship together and work only together. The control-plane provider creates a
ChalkosConfig for each control-plane Machine. The bootstrap provider watches ChalkosControlPlanes
and reads the cluster-wide documents from them, so it refuses a Machine whose Cluster names
another kind of control plane in `spec.controlPlaneRef`.

The API has four kinds:

| Kind | Provider | What it holds |
| --- | --- | --- |
| `ChalkosControlPlane` | control plane | `replicas` (odd, default 1), `version`, `image`, `imagePullSecretRef`, `machineTemplate` with the infrastructure template, `files` with the cluster-wide chalkos documents, and `kubeconfig.ttlSeconds`. |
| `ChalkosControlPlaneTemplate` | control plane | The same without `replicas`, `version` and the infrastructure reference, for a ClusterClass. |
| `ChalkosConfig` | bootstrap | One Machine's `image`, `imagePullSecretRef` and the `files` of its pool, such as a Node's labels and taints. |
| `ChalkosConfigTemplate` | bootstrap | The ChalkosConfig that a MachineDeployment or MachineSet clones for each Machine. |

Every `image` names a chalkos image by digest, `repository@sha256:…`. A `files` entry is a chalkos
config document, written inline in the object. The ChalkosControlPlane's `files` must hold the
`kubernetes.chalkos.dev` Cluster document. The providers set the fields they own from the Cluster
API objects and refuse documents that set them: the Node's type and providerID, the Cluster's
endpoint, and its IP families and pod and service ranges when the Cluster's `clusterNetwork` gives
ranges. [`lab/cluster.yaml.tmpl`](lab/cluster.yaml.tmpl) is a complete example of a Cluster with
one control plane and a MachineDeployment of one worker.

## Removing Machines

Every chalkos Machine carries the pre-terminate hook
`pre-terminate.delete.hook.machine.cluster.x-k8s.io/chalkos`. Its value names the provider that
runs it: `control-plane-chalkos` on control-plane Machines and `bootstrap-chalkos` on workers.
Cluster API drains a deleted Machine and then keeps its infrastructure until no pre-terminate hook
is left. The provider runs the hook only once Cluster API waits for it, so the kubelet can still
report while its pods are evicted:

- A worker's node is revoked through the first control plane that answers.
- A control plane leaves etcd. When its node cannot be reached, another control plane removes its
  etcd member instead. Then its node is revoked through another control plane. The control-plane
  provider runs the hooks of its Machines one at a time, oldest deletion first.

Then the provider removes the annotation, and Cluster API deletes the infrastructure and the Node.
A revoked node can no longer renew its certificates or change anything in the cluster;
[Kubernetes certificates](https://github.com/trevex/chalkos/blob/main/docs/how-it-works/kubernetes/certificates.md#revoking-a-node-cuts-off-its-writes-and-renewals)
in the chalkos docs lists what the revocation blocks. A control plane's disk also held the
cluster's CA keys, which no revocation takes back: when a removed control plane's disk may be
read by someone else, rotate the cluster's CAs with chalkctl.

Some Machines skip the steps:

- A Machine whose machine the bootstrap provider never reached to install loses its hook at
  once: it never received certificates.
- The last Machine of a ChalkosControlPlane that is being deleted, or that is not initialized,
  neither leaves etcd nor is revoked.
- A control-plane Machine that is etcd's last voter, such as the only Machine of an initialized
  one-replica control plane, keeps its hook until another Machine has joined etcd as a healthy
  voting member. The control plane creates the replacement as soon as the Machine is deleted, so
  deleting that Machine replaces it. The waiting Machine holds back no other hook: a replacement
  that never joins can be deleted, its etcd learner is removed through the waiting Machine, and
  the control plane creates the next replacement.
- Deleting the Cluster removes every hook at once: nothing leaves etcd and nothing is revoked.

A step that cannot finish, such as a leave that etcd refuses for its quorum or a revocation that
no control plane answers, keeps the hook, records a Warning event and runs again 30 seconds later.
Every step can be repeated safely. A Machine that waits as etcd's last voter records no Warning
event; it checks again every 30 seconds.

A node whose own etcd member does not answer, for example after it lost its pinned address,
refuses to leave etcd, and its hook waits. Remove the member through another control plane with
`chalkctl etcd remove-member <node>`, or with `chalkctl etcd leave <node> --through-others`. The
node is then no etcd member, and the hook goes on with the revocation.

### What the conditions say

A worker's ChalkosConfig reports its hook in the condition `NodeRemoved`:

| Status | Reason | Meaning |
| --- | --- | --- |
| False | `Revoking` | A control plane refused the revocation, or it failed. The message names each control plane tried and its answer; one that cannot be reached reads as "unreachable", and the manager's log has the full error. |
| False | `WaitingForControlPlane` | No control plane could be reached, none has an address, or the cluster's secrets are gone. |
| False | `RevokedListFull` | The list of revoked nodes has no room, even after the provider pruned it. See [The list of revoked nodes](#the-list-of-revoked-nodes). |
| True | `Done` | The node was revoked, and the message names the control plane and the list's fill level. Or the node needs no revocation, because its machine was never reached or the Cluster is being deleted. |

The ChalkosControlPlane reports control-plane removals in the condition `ScalingDown`:

| Status | Reason | Meaning |
| --- | --- | --- |
| True | `ScalingDown` | A Machine is being removed. The message names the Machine and the step it waits on: its drain ("is drained before it leaves etcd"), a leave that its node refuses, an etcd member that no other control plane removes, a revocation that no control plane answers, or a Machine that is etcd's last voter: it "waits for its replacement control plane to join etcd", or "waits for a control plane to list etcd's members" when none answers. The messages of a leave refused as a node's own etcd member does not answer, and of a wait that may last for good, name what to run or the annotation to remove. A node that cannot be reached reads as "unreachable"; the manager's log has the full error. |
| True | `RevokedListFull` | The Machine's node is not revoked because the list of revoked nodes has no room, even after the provider pruned it. |
| False | `WaitingToScaleDown` | The control plane has more Machines than `replicas`, but may not delete one yet: a Machine has not joined the cluster, or etcd does not list one healthy voting member per Machine. The message says which. |
| False | `NotScalingDown` | No Machine is being removed. |

While the ChalkosControlPlane itself is being deleted, `ScalingDown` stays `NotScalingDown` and
the `Deleting` condition, with the reason `WaitingForMachineDeletion`, carries the message of the
hook that waits. Both providers also record events on the ChalkosConfig or ChalkosControlPlane,
such as `NodeRevoked`, `ControlPlaneRemoved` and `PreTerminateHookWaits`.

### The list of revoked nodes

chalkos keeps the cluster's revoked nodes in one list, the ConfigMap
`kube-system/chalkos-revoked-nodes`, of a fixed capacity. A control plane reports the list's
length and capacity in its Kubernetes status. Each entry stores when it expires: once the node's
last certificate has expired, plus a margin. chalkd does not prune the list on its own; the
control-plane provider calls `PruneRevoked` through a control plane:

- after each revocation of a control plane,
- once a day otherwise, which covers the workers' revocations; `status.lastRevokedPruneTime` on
  the ChalkosControlPlane records the last attempt,
- before a hook that finds the list full tries the revocation again, at most every five minutes.
  The bootstrap provider does the same for a worker's hook, at most every five minutes for the
  workers of a Cluster; it keeps the time of the last prune in memory, so a restarted manager
  prunes once more.

`PruneRevoked` drops the expired entries and those revoked before both the cluster's OS CA and its
Kubernetes CA were made. A prune that fails is logged and recorded as a Warning event
`PruneRevokedFailed`; it never holds a hook. When the list stays full, the hook waits with the
reason `RevokedListFull`. Rotate the cluster's CAs with `chalkctl rotate os-ca` and
`chalkctl rotate kubernetes-ca` to make room: the next prune drops every node revoked before them.

### Removing the hook by hand

Removing the annotation is the break-glass for a hook that cannot finish, such as one whose
control planes are all gone:

```sh
kubectl annotate machine <machine> pre-terminate.delete.hook.machine.cluster.x-k8s.io/chalkos-
```

Cluster API then deletes the Machine's infrastructure without either step. A control plane
removed this way stays an etcd member until `chalkctl etcd remove-member <node>` removes it, and
its node is not revoked: its certificates stay valid until they expire, unless
`chalkctl node revoke <node>` revokes it. Removing the hook of a Machine that waits as etcd's
last voter deletes the cluster's last voting etcd member, and the cluster's state with it. The
provider adds no hook to a Machine that is being deleted, so the annotation stays removed.

## Install

Each release publishes both managers' images to the GitHub Container Registry, as
`ghcr.io/trevex/cluster-api-provider-chalkos-bootstrap` and
`ghcr.io/trevex/cluster-api-provider-chalkos-controlplane`. The GitHub release holds the files
clusterctl installs the providers from: `bootstrap-components.yaml` and
`control-plane-components.yaml`, which name the images by digest, and `metadata.yaml`. clusterctl
does not list the chalkos providers itself, so add them to its configuration,
`~/.config/cluster-api/clusterctl.yaml`:

```yaml
providers:
- name: chalkos
  type: BootstrapProvider
  url: https://github.com/trevex/cluster-api-provider-chalkos/releases/latest/bootstrap-components.yaml
- name: chalkos
  type: ControlPlaneProvider
  url: https://github.com/trevex/cluster-api-provider-chalkos/releases/latest/control-plane-components.yaml
```

The URLs take the form clusterctl expects of a GitHub repository, without the `download/` of a
browser's link. The lab passes the same entries to clusterctl as
[`lab/clusterctl.yaml`](lab/clusterctl.yaml). Then install both providers of the same release with
an infrastructure provider:

<!-- x-release-please-start-version -->
```sh
clusterctl init --bootstrap chalkos:v0.2.0 --control-plane chalkos:v0.1.0 --infrastructure <provider>
```
<!-- x-release-please-end -->

The release workflow signs each image and each file of a release with cosign, keylessly as the
workflow itself, and attests their provenance with GitHub. To check a release's components files
before you install it, download them with their signatures and verify both:

<!-- x-release-please-start-version -->
```sh
gh release download v0.2.0 --repo trevex/cluster-api-provider-chalkos \
  --pattern '*-components.yaml*' --pattern SHA256SUMS
for file in bootstrap-components.yaml control-plane-components.yaml; do
  cosign verify-blob "$file" --bundle "$file.sigstore.json" \
    --certificate-identity https://github.com/trevex/cluster-api-provider-chalkos/.github/workflows/release.yml@refs/heads/main \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com
  gh attestation verify "$file" --repo trevex/cluster-api-provider-chalkos
done
sha256sum --check --ignore-missing SHA256SUMS
```
<!-- x-release-please-end -->

Each components file names its manager's image as `repository:tag@sha256:<digest>`, and the
release notes list both. Verify an image by that digest:

```sh
cosign verify ghcr.io/trevex/cluster-api-provider-chalkos-bootstrap@sha256:<digest> \
  --certificate-identity https://github.com/trevex/cluster-api-provider-chalkos/.github/workflows/release.yml@refs/heads/main \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
gh attestation verify oci://ghcr.io/trevex/cluster-api-provider-chalkos-bootstrap@sha256:<digest> \
  --repo trevex/cluster-api-provider-chalkos
```

Each image also carries an SPDX SBOM, as a release asset `<image>.spdx.json` and as cosign and
GitHub attestations on its digest; the release notes show how to fetch it.

## Quick start

The lab in [`lab/`](lab/) runs a kind management cluster with Cluster API, CAPK, KubeVirt, CDI
and both providers, and creates the workload cluster `lab` of chalkos VMs on it. It builds
nothing: the VMs boot chalkos's released images from `ghcr.io`, and the providers are the latest
release. It needs Nix with flakes, Docker and internet access. The two VMs take 5 GiB of memory on
top of the kind cluster. With `/dev/kvm` the VMs run hardware-accelerated; without it, KubeVirt
emulates them. All state lives in `.lab/` of the repository.

From the devshell (`nix develop`), create the lab and the workload cluster, then look at it:

```sh
just -f lab/justfile up
just -f lab/justfile describe
just -f lab/justfile kt get nodes
```

`up` runs four recipes in order. `create` starts the kind cluster. `kubevirt` installs KubeVirt
and CDI. `providers` installs the chalkos providers of the release that `provider_version` in
[`lab/justfile`](lab/justfile) names, with Cluster API, cert-manager and CAPK, from the GitHub
release that [`lab/clusterctl.yaml`](lab/clusterctl.yaml) points clusterctl at. `cluster` creates
the workload cluster from [`lab/cluster.yaml.tmpl`](lab/cluster.yaml.tmpl). `describe` shows its
state, and `kt` runs kubectl against it. `just -f lab/justfile destroy` deletes the kind cluster
with the workload cluster.

The VMs boot `ghcr.io/trevex/chalkos/kubernetes-<minor>-kvm` of the chalkos release that `go.mod`
requires, for the Kubernetes minor that `kubernetes_minor` in the justfile names. CDI imports each
VM's disk from its containerDisk, `ghcr.io/trevex/chalkos/kubernetes-<minor>-kvm-disk`, and the
providers install the nodes in place from the image itself. `cluster` resolves both tags to
digests, and sets the cluster's Kubernetes version to the one the image carries.

To run the providers of your checkout instead, run `providers-local` in place of `providers`. It
builds both managers' images, loads them into the kind node and installs them from a local
clusterctl repository; on a lab whose providers run already, it rolls them out to the new images:

```sh
just -f lab/justfile create kubevirt providers-local cluster
```

The lab installs the Cluster API core release that `go.mod` builds the providers against. The
devshell's `clusterctl` runs `go tool clusterctl`, a tool that `go.mod` names, so it is of the same
release. It works only from inside the repository. `go.mod` depends on Cluster API v1.15.0-rc.0
for now and moves to v1.15.0 once that release is out.

## Security model

The providers never pass a node's configuration through the infrastructure provider. Every
Machine's bootstrap data Secret holds a fixed marker, which the infrastructure provider passes to
the machine as user data. The configuration, the node's certificates and the keys it needs reach
the machine over chalkd's API, in the install call the bootstrap provider makes.

A machine in maintenance mode presents a self-signed certificate that nothing can verify in
advance. The bootstrap provider trusts it on first use: it reads the certificate's fingerprint over
an unverified connection, then connects again pinned to that fingerprint, so the install goes to
that certificate alone. Each new fingerprint is recorded, with the time it was seen, in the
ChalkosConfig's `status.maintenanceFingerprints`, which keeps the last 10. Compare them with
fingerprints obtained from the machines out of band to check after the fact that no install went to
another host. Once installed, a node presents a certificate that the cluster's node CA issued, and
the providers verify it.

An image can carry the cluster's OS CA, set when the image is built. A machine that boots such an
image accepts only clients whose certificates that OS CA issued, also in maintenance mode, so no
other party can install it. Before an install, the bootstrap provider checks the image's OS CA
against the cluster's secrets and reports `ImageRefused` when they differ. An image without an OS
CA, such as the lab's, accepts any client in maintenance mode and suits a private network only.

The cluster's secrets live in plaintext in the Secret `<cluster>-chalkos-secrets` next to the
Cluster, under the key `secrets.json`. It holds every CA and private key of the cluster, among them
the OS CA, the node CA, the Kubernetes, front-proxy and etcd CAs, the service-account key and the
encryption key. Anyone who can read it controls the cluster, so restrict read access to Secrets in
the Cluster's namespace and encrypt Secrets at rest in the management cluster.

The control-plane provider generates this Secret once the Cluster has a control-plane endpoint, if
none exists. It sets the ChalkosControlPlane as the Secret's owner, so the Secret is deleted with
the ChalkosControlPlane, and labels it for `clusterctl move`. A Secret that exists already is kept
as it is. To install images that carry an OS CA, create the Secret before the Cluster from the
`secrets.json` that `chalkctl gen secrets --plaintext` writes, of type `cluster.x-k8s.io/secret`
and with the labels
`cluster.x-k8s.io/cluster-name: <cluster>` and `clusterctl.cluster.x-k8s.io/move: ""`, and build
the images with the `secrets.pub.json` written beside it.

The kubeconfig Secret `<cluster>-kubeconfig` holds an admin kubeconfig whose client certificate
the cluster's Kubernetes CA issues. The certificate is valid for `spec.kubeconfig.ttlSeconds`, one
day by default, and is issued again after two thirds of that time.

Both managers read and create Secrets, and the control-plane manager also updates them. Their
ClusterRoles grant this in every namespace. `--namespace` limits the objects a manager reconciles
to one namespace, but not what its ClusterRole grants.

## Building a local provider repository

clusterctl installs providers from a provider repository: a versioned set of `metadata.yaml` and
components files, addressed either on GitHub or on the local filesystem. `make local-repo` builds
this repository on the local filesystem, so a management cluster can run both providers without a
release:

<!-- x-release-please-start-version -->
```sh
make local-repo REGISTRY=localhost:5001 IMG_TAG=dev VERSION=v0.2.0
```
<!-- x-release-please-end -->

This builds the bootstrap and control-plane manager images, pushes them to `REGISTRY`, and lays
out `out/repo/bootstrap-chalkos/$(VERSION)` and `out/repo/control-plane-chalkos/$(VERSION)`, each
with a components file and a copy of `metadata.yaml`. It also writes `out/clusterctl.yaml`, a
clusterctl configuration file with `providers:` entries pointing at those components files by
absolute path. `REGISTRY` and `IMG_TAG` default to `localhost:5001` and `dev`, and `VERSION` to
the latest release's version; the registry must be reachable both from the machine running
`make`, to push the images, and from the management cluster, to pull them.

`make local-repo-manifests` runs the same layout step without building or pushing images. It
produces the same `out/repo` and `out/clusterctl.yaml`, with the managers pointing at images that
may not exist yet in the registry, which is enough to check the generated manifests without
Docker.

`clusterctl init` reads `out/clusterctl.yaml` through its `--config` flag, which adds the chalkos
providers to clusterctl's built-in provider list rather than replacing it. Pointed at a management
cluster that can reach `REGISTRY`, and paired with an infrastructure provider such as CAPK, this
installs every provider the [lab](lab/) needs:

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
nodes to become Ready, and checks the providers' statuses and the kubeconfig's certificate. It
then scales the MachineDeployment to two workers and back to one, and checks that every Machine
carries its hook and that the removed worker's Node is gone and its node revoked: the control
plane lists it among the revoked nodes in its Kubernetes status, which the test reads from chalkd
through a NodePort Service on the kind node. This step takes a few minutes more and a third VM,
of 2 GiB. Last, the test deletes the cluster and waits until its Machines and VMs are gone. It
skips unless the lab runs.
The test is for the providers of the checkout, so start the lab with `providers-local`:

```sh
just -f lab/justfile create kubevirt providers-local
make e2e
just -f lab/justfile destroy
```

A run that stops early still deletes the cluster, after it logged the cluster's state on failure.
`E2E_KEEP=1 make e2e` keeps the cluster of such a run to look at.

`make e2e-ha` tests control-plane scaling on the same lab. It creates the cluster `lab`, scales the
ChalkosControlPlane to three replicas and waits until all four nodes are Ready and etcd lists
three healthy voting members. It then scales the control plane back to one and checks that the
provider removes the oldest control plane first and then the next one. For each removed control
plane, the test checks that its Node is gone and that the remaining control plane lists it among
the revoked nodes. Last, it checks that etcd has one member and that the remaining Node carries
`node-role.kubernetes.io/control-plane`. At its peak the test runs three control planes of 3 GiB
and one worker of 2 GiB, about 11 GiB for the VMs, so pull requests do not run it; the nightly
run does.

CI runs `make e2e` for a pull request with the label `e2e`, and `make e2e` and `make e2e-ha`
every night on `main`, each on a runner of its own. A failed run uploads the test's output, the
cluster's state and the kind node's logs as the artifact `e2e-logs` or `e2e-ha-logs`.

## Contributing and security

[CONTRIBUTING.md](CONTRIBUTING.md) describes the development setup, the tests and the rules for
commits and pull requests. Report a vulnerability privately, as [SECURITY.md](SECURITY.md)
describes.
