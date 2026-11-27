# Contributing to cluster-api-provider-chalkos

The providers take changes as pull requests against `main`. This file describes the setup, the
tests and the rules for commits and pull requests. The [README](README.md) explains what the
providers do, and chalkos's
[Contribute](https://trevex.github.io/chalkos/contribute/) pages describe chalkos itself.

## Set up

You need an x86-64 Linux machine with Nix and flakes enabled. The lab also needs Docker, and runs
its VMs faster with `/dev/kvm`. The development shell has every other tool, such as Go,
controller-gen, golangci-lint, kind, clusterctl and just:

```sh
git clone https://github.com/trevex/cluster-api-provider-chalkos
cd cluster-api-provider-chalkos
nix develop
```

## Build and test

Run the targets of the `Makefile` in the development shell:

| Target | What it does |
| --- | --- |
| `make generate` | Generates the DeepCopy methods of both providers' APIs. |
| `make manifests` | Generates the CRDs, RBAC and webhook configurations under `bootstrap/config` and `controlplane/config`. |
| `make fmt`, `make vet`, `make lint` | Run go fmt, go vet and golangci-lint. |
| `make test` | Runs the unit tests and the envtest suites. |
| `make build` | Builds both managers. |
| `make e2e` | Runs the end-to-end test against a running lab. |

The generated files are committed. A change to an API type or a kubebuilder marker commits what
`make generate manifests` produces from it.

## The lab and the end-to-end test

The lab in [`lab/`](lab/) runs a kind management cluster with Cluster API, CAPK, KubeVirt and
both providers, and boots chalkos VMs on it. `make e2e` creates the workload cluster `lab` there,
checks it and deletes it:

```sh
just -f lab/justfile registry create kubevirt images providers
make e2e
just -f lab/justfile destroy
```

Run it for a change to the controllers, the lab or the Cluster API contract.
[End-to-end test](README.md#end-to-end-test) in the README has the details.

## Commit

Run `make fmt lint test` before you commit. Commits follow
[Conventional Commits](https://www.conventionalcommits.org/): a type, an optional scope and a
summary in the imperative, such as
`fix(controlplane): renew the kubeconfig while a deletion waits for the workers`. The body, when
there is one, says what changed and why. Code comments and commit messages name no plan, task or
milestone.

Sign off every commit with `git commit -s`. The sign-off adds the trailer
`Signed-off-by: Your Name <you@example.com>`, which must match the commit's author. With it you
certify the [Developer Certificate of Origin](https://developercertificate.org/): you wrote the
change, or have the right to submit it under the project's licence, and you agree that it and
your sign-off are kept in the project's public record. The sign-off is the only trailer a commit
carries, so leave out `Co-Authored-By` and the like. To sign off the commits of a branch after
the fact, run `git rebase --signoff origin/main` and force-push.

Sign every commit too, with a GPG or SSH key that your GitHub account lists as a signing key:
`main` takes a pull request only when GitHub shows each of its commits as verified. For an SSH
key:

```sh
git config --global gpg.format ssh
git config --global user.signingkey ~/.ssh/id_ed25519.pub
git config --global commit.gpgsign true
```

GitHub's
[commit signature verification](https://docs.github.com/en/authentication/managing-commit-signature-verification)
describes the other kinds of key.

You may write a change with the help of an AI tool. The person who signs it off answers for it:
they have read and understood it, and they certify its origin like that of any other change.

A change carries its tests, the documentation it affects and the generated files its sources
produce.

## Open a pull request

Every change reaches `main` through a pull request, which a squash merge puts on `main` as one
commit. The pull request's title becomes that commit's subject, and release-please writes the
changelog from it, so the title is a Conventional Commit too. For a pull request with more than
one change for the changelog, list them in its description between `BEGIN_COMMIT_OVERRIDE` and
`END_COMMIT_OVERRIDE`, one Conventional Commit each:

```text
BEGIN_COMMIT_OVERRIDE
feat(bootstrap): report the image's OS CA in the ChalkosConfig's status

fix(controlplane): keep the kubeconfig Secret's owner on an update
END_COMMIT_OVERRIDE
```

Open the pull request as a draft while the change is in progress: a draft runs only the sign-off,
title and lint checks. Mark it ready for review to run the full CI. Add the label `e2e` when the
change touches the controllers, the lab or the Cluster API contract, which runs the end-to-end
test on the pull request.

## Licence

The providers are licensed under the [Apache License 2.0](LICENSE). A contribution is licensed
under the same terms.
