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
| `make release-manifests` | Builds `bootstrap-components.yaml`, `control-plane-components.yaml` and a copy of `metadata.yaml` into `out/`, with the managers at `IMG_BOOTSTRAP` and `IMG_CONTROLPLANE`. |
| `make e2e` | Runs the end-to-end test against a running lab. |
| `make e2e-ha` | Scales the control plane 1 → 3, rolls it with `maxSurge` 1 and 0, and scales it back to 1 against a running lab. It needs about 12 GiB for its VMs; the nightly run runs it. |

The generated files are committed. A change to an API type or a kubebuilder marker commits what
`make generate manifests` produces from it.

## The lab and the end-to-end test

The lab in [`lab/`](lab/) runs a kind management cluster with Cluster API, CAPK, KubeVirt and
both providers, and boots chalkos VMs on it from chalkos's released images. `just -f lab/justfile
up` installs the released providers; the tests run the providers of your checkout, which
`providers-local` builds and installs. `make e2e` creates the workload cluster `lab` there, checks
it, scales its workers to two and back to one, checks that the removed worker was revoked, and
deletes the cluster:

```sh
just -f lab/justfile create kubevirt providers-local
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
title, lint and workflow checks. Mark it ready for review to run the full CI. Add the label `e2e`
when the change touches the controllers, the lab or the Cluster API contract: CI then runs the
end-to-end test on the pull request, on a runner with `/dev/kvm`. A push to `main` does not run
it; the nightly workflow runs it and `make e2e-ha` on `main` and opens an issue labelled
`ci-failure` when one fails.

## Dependency updates

Renovate keeps the dependencies current, configured in
[`.github/renovate.json5`](.github/renovate.json5). It opens a pull request per group each Monday
before 06:00 Europe/Berlin, labelled `dependencies`, and lists every pending and open update on
its Dependency Dashboard issue. A release of a Go module, an action or a base image waits seven
days before Renovate proposes it.

| Group | What it updates | Labels | Merge |
| --- | --- | --- | --- |
| `chalkos` | The chalkos release of `go.mod` and of the flake input, whose images the lab boots, as soon as chalkos publishes it | `dependencies`, `e2e` | by hand |
| `go-kubernetes` | The modules `k8s.io/*` and `sigs.k8s.io/*`, Cluster API and controller-runtime among them | `dependencies`, `e2e` | by hand |
| `go-x` | The modules `golang.org/x/*` | `dependencies` | by hand |
| `go-deps` | Every other Go module's patch and minor releases | `dependencies` | by hand |
| `go-toolchain` | The `go` directive of `go.mod` and the Dockerfile's `golang` release | `dependencies`, `e2e` | by hand |
| `dockerfile` | The digests of the Dockerfile's base images, which stay pinned by digest | `dependencies` | by hand |
| `github-actions` | The actions' patch and minor releases and digests, pinned by digest | `dependencies` | automerge |
| `github-actions-release` | The same for the actions of the jobs that hold `id-token: write` | `dependencies` | by hand |

Renovate bumps chalkos in `flake.nix` but leaves `flake.lock` alone. The workflow
`renovate-fixup.yml` runs `nix flake update chalkos` on Renovate's branch and pushes the new lock
file as a commit of the release App's bot. Renovate finds the flake input only when it names a
release tag, such as `github:trevex/chalkos/v0.8.0`. If you pin a chalkos commit between
releases, set it in both `go.mod` and `flake.nix` and run `nix flake update chalkos`.

A major release of a Go module waits on the Dependency Dashboard until you approve it, and a
major release of an action gets a pull request of its own. A fix for a known vulnerability comes
at any time, outside its group, labelled `security`.

## Release

[release-please](https://github.com/googleapis/release-please) makes the releases, from
`.github/workflows/release.yml`, as in chalkos's
[Release chalkos](https://trevex.github.io/chalkos/contribute/releasing/). On every push to `main`
it opens or rebuilds the release pull request, `chore(main): release <version>`, as a draft. The
pull request adds the release's section to `CHANGELOG.md` and sets the version on every line that
`release-please-config.json` lists in `extra-files`: lines that hold the marker
`x-release-please-version` or lie between `x-release-please-start-version` and
`x-release-please-end`, such as the install commands in the README, `VERSION` in the `Makefile`
and `provider_version` in `lab/justfile`. Before 1.0, a `feat` commit or a breaking change raises
the minor version and a `fix` the patch version.

clusterctl installs a version only when `metadata.yaml` maps its major.minor to a Cluster API
contract, and release-please cannot add that entry. The `release-series` check fails the release
pull request of a new minor version until a pull request to `main` adds it to `releaseSeries`.
release-please then rebuilds the release pull request.

To release, mark the release pull request ready for review, wait for `result`, and squash-merge
it. Never use "Update branch" on it: the `release-pr` check accepts only release-please's one
commit on the current tip of `main`, so wait until release-please rebuilds it. The push of the
merge runs the release workflow again: release-please tags `v<version>` and creates the GitHub
release, and the same run publishes it.

| Job | What it does |
| --- | --- |
| `plan` | Reads the version from `.release-please-manifest.json` and fails when it is not the tag release-please made or `metadata.yaml` lacks its series. |
| `image <package>` | Builds a manager's image with buildx, pushes it as `ghcr.io/trevex/cluster-api-provider-chalkos-<package>:<tag>`, signs the digest with `cosign sign`, writes its SPDX SBOM with syft and attests its provenance and its SBOM in the registry. |
| `assets` | Runs `make release-manifests` with both images by digest, adds `metadata.yaml` and the SBOMs, writes `SHA256SUMS`, signs every file with `cosign sign-blob`, attests their provenance and uploads them to the release. |
| `notes` | Appends the images' digests and the commands that install and verify the release to the release's notes. |

Signing is keyless: the certificate's identity is `release.yml` on `refs/heads/main`. When a job
fails, "Re-run failed jobs" builds the same commit again and replaces the assets and the section
of the notes.

A dry run builds the images and the assets of a branch and publishes nothing; the run's summary
lists the digests and checksums, and its artifact `release-assets` holds the files for seven
days:

```sh
gh workflow run release.yml --repo trevex/cluster-api-provider-chalkos --ref <branch> -f dry-run=true
```

Before the first release a maintainer installs the release GitHub App, which needs Contents, Pull
requests and Issues read and write, on the repository and sets the secrets `RELEASE_APP_ID`, its
client ID, and `RELEASE_APP_PRIVATE_KEY`, a private key in PEM form. The GitHub Container Registry
creates each image's package private on its first push, so after the first release the maintainer
makes both packages public in their settings. `.release-please-manifest.json` holds `0.0.0` until
then, and the first release is the `initial-version` of `release-please-config.json`, v0.1.0.

## Licence

The providers are licensed under the [Apache License 2.0](LICENSE). A contribution is licensed
under the same terms.
