<!--
The title is a Conventional Commit, such as "fix(controlplane): renew the kubeconfig while a
deletion waits": a squash merge makes it the subject of the commit on main, and the changelog
comes from it. Sign off every commit with git commit -s.

Open the pull request as a draft while the change is in progress: a draft runs only the DCO, title
and lint checks, and its result check fails. Mark it ready for review to run the full CI.

Add the label e2e when the change touches the controllers, the lab or the Cluster API contract. A
label added later runs CI again with the end-to-end test.
-->

## What and why

## Checklist

- [ ] The change carries its tests and the docs it affects, and I ran `make generate manifests`.
- [ ] The change has the label e2e, or touches neither the controllers, the lab nor the Cluster API contract.
