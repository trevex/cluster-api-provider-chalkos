# Security policy

This file says which releases of cluster-api-provider-chalkos receive security fixes, how to
report a vulnerability, and what the maintainers do with a report.

## Supported versions

Before 1.0, the project supports the latest release alone. A fix ships in a new release and on
`main`; earlier releases receive none, so upgrade to the release that fixes a vulnerability.

| Version | Supported |
| --- | --- |
| Latest release | Yes |
| Earlier releases | No |

## Report a vulnerability

Report a vulnerability privately through GitHub's private vulnerability reporting: open
[a new report](https://github.com/trevex/cluster-api-provider-chalkos/security/advisories/new),
or select Report a vulnerability on the repository's Security tab. Only the maintainers and the
reporter see the report. Never report a vulnerability in a public issue, pull request or
discussion.

A useful report names:

- the release, image or commit that has the vulnerability
- the component: the bootstrap provider, the control-plane provider or the components YAML
- the steps that reproduce it, with the Cluster API objects that show it where possible
- what an attacker gains, and from where: the network, a machine, the management cluster or a
  workload cluster

## What happens after a report

The maintainers aim to:

1. acknowledge the report within 7 days
2. assess it within 14 days: whether it is a vulnerability in the providers, how severe it is,
   and which releases it affects
3. release a fix and disclose the vulnerability in coordination with the reporter, normally within
   90 days of the report

The report's private advisory carries the discussion, and the reporter hears of each step there.
A vulnerability that is already public or exploited gets a fix sooner. The maintainers publish a
GitHub Security Advisory with the release that fixes the vulnerability, credit the reporter unless
they decline, and request a CVE for it when the vulnerability warrants one.

## Scope

The policy covers:

- the code of this repository, the Go module `github.com/trevex/cluster-api-provider-chalkos`
- what a release publishes: the bootstrap and control-plane manager images on `ghcr.io` and the
  components YAML that installs them, with its RBAC and webhooks

The bootstrap provider trusts a machine's certificate in maintenance mode on first use, as
[Security model](README.md#security-model) in the README describes. That model is not a
vulnerability by itself; a way to make the provider install to a host other than the one whose
fingerprint it recorded is.

A vulnerability in chalkos itself, such as in chalkd or a chalkos image, belongs to
[chalkos](https://github.com/trevex/chalkos/security/policy). A vulnerability in Cluster API,
controller-runtime, Kubernetes or another Go module belongs to its upstream project: report it
there. The providers ship the fix in a release once the module has it. How the providers use or
configure an upstream component is in scope.
