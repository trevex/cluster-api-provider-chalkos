#!/usr/bin/env bash
# Tests dco.sh, pr-title.sh and release-series.sh: dco.sh on the commits of a repository made for
# the test, pr-title.sh on titles that pass and titles that fail, and release-series.sh on
# release-please manifests and metadata.yaml files made for the test.
set -euo pipefail

scripts=$(cd "$(dirname "$0")" && pwd)
failures=0

# expect pass|fail DESCRIPTION COMMAND...: runs the command and counts a failure when its outcome
# is not the one expected.
expect() {
  local want=$1 what=$2 got
  shift 2
  if "$@" >/dev/null 2>&1; then got=pass; else got=fail; fi
  if [ "$got" = "$want" ]; then
    echo "ok: $what"
  else
    echo "FAIL: $what: wanted $want, got $got" >&2
    failures=$((failures + 1))
  fi
}

repo=$(mktemp -d)
trap 'rm -rf "$repo"' EXIT
cd "$repo"
git init -q -b main
git config user.name "Ada Lovelace"
git config user.email "ada@example.com"
git config commit.gpgsign false
git commit -q --allow-empty -m "chore: start"
base=$(git rev-parse HEAD)

# commit AUTHOR MESSAGE: an empty commit by AUTHOR, "Name <email>", with MESSAGE.
commit() {
  git commit -q --allow-empty --author="$1" -m "$2"
  git rev-parse HEAD
}
# dco BASE HEAD: dco.sh on the commits of BASE..HEAD.
dco() {
  bash "$scripts/dco.sh" "$1" "$2"
}

ada="Ada Lovelace <ada@example.com>"
renovate="renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>"
bot="chalkreleaser[bot] <359050260+chalkreleaser[bot]@users.noreply.github.com>"

signed=$(commit "$ada" "$(printf 'feat: a\n\nSigned-off-by: %s' "$ada")")
expect pass "a commit signed off by its author" dco "$base" "$signed"
expect pass "an empty range" dco "$signed" "$signed"

git reset -q --hard "$base"
unsigned=$(commit "$ada" "fix: b")
expect fail "a commit without a sign-off" dco "$base" "$unsigned"

git reset -q --hard "$base"
other=$(commit "$ada" "$(printf 'fix: c\n\nSigned-off-by: Charles Babbage <charles@example.com>')")
expect fail "a commit signed off by someone else" dco "$base" "$other"

git reset -q --hard "$base"
email=$(commit "$ada" "$(printf 'fix: d\n\nSigned-off-by: Ada Lovelace <ada@example.org>')")
expect fail "a sign-off with another email" dco "$base" "$email"

git reset -q --hard "$base"
name=$(commit "$ada" "$(printf 'fix: e\n\nSigned-off-by: A. Lovelace <ada@example.com>')")
expect fail "a sign-off with another name" dco "$base" "$name"

git reset -q --hard "$base"
body=$(commit "$ada" "$(printf 'fix: f\n\nSigned-off-by: %s\n\nA paragraph after it.' "$ada")")
expect fail "a sign-off in the body, not among the trailers" dco "$base" "$body"

git reset -q --hard "$base"
several=$(commit "$ada" "$(printf 'fix: g\n\nSigned-off-by: Charles Babbage <charles@example.com>\nSigned-off-by: %s' "$ada")")
expect pass "the author's sign-off among others" dco "$base" "$several"

git reset -q --hard "$base"
commit "$renovate" "build(deps): update a module" >/dev/null
fixup=$(commit "$bot" "build(deps): update the vendor hash")
expect pass "Renovate's and the App bot's commits without a sign-off" dco "$base" "$fixup"

git reset -q --hard "$base"
first=$(commit "$ada" "$(printf 'fix: h\n\nSigned-off-by: %s' "$ada")")
mixed=$(commit "$ada" "fix: i")
commit "$ada" "$(printf 'fix: j\n\nSigned-off-by: %s' "$ada")" >/dev/null
expect fail "one unsigned commit among signed ones" dco "$base" HEAD
expect pass "the signed commits before it" dco "$base" "$first"
expect fail "the unsigned commit alone" dco "$first" "$mixed"

git reset -q --hard "$base"
main=$(commit "$ada" "$(printf 'fix: k\n\nSigned-off-by: %s' "$ada")")
git switch -q -c topic "$base"
commit "$ada" "$(printf 'fix: l\n\nSigned-off-by: %s' "$ada")" >/dev/null
git merge -q --no-ff -m "Merge branch 'main' into topic" "$main"
expect pass "a merge commit without a sign-off" dco "$base" HEAD

git rebase -q --signoff --onto "$base" "$base" "$unsigned"
expect pass "a commit after git rebase --signoff" dco "$base" HEAD

for title in \
  "feat: add a kind" \
  "fix(chalkctl): check every node" \
  "chore(main): release 0.2.2" \
  "build(deps): update the flake inputs" \
  "refactor(chalkctl,chalkd)!: rename the flag" \
  "feat!: drop the old API" \
  "ci(release): rebuild the release pull request" \
  "style(chalkd): wrap a doc comment" \
  "deps: update module golang.org/x/net to v0.62.0" \
  "revert: feat: add a kind"; do
  expect pass "the title '$title'" bash "$scripts/pr-title.sh" "$title"
done
for title in \
  "Add a kind" \
  "feature: add a kind" \
  "wip: add a kind" \
  "feat:add a kind" \
  "feat: " \
  "feat(): add a kind" \
  "feat(a b): add a kind" \
  "Feat: add a kind" \
  " feat: add a kind" \
  "feat!(chalkctl): add a kind"; do
  expect fail "the title '$title'" bash "$scripts/pr-title.sh" "$title"
done

# series MANIFEST_VERSION METADATA_SERIES...: a directory with a manifest of MANIFEST_VERSION, the
# initial version 0.1.0 and a metadata.yaml that lists each METADATA_SERIES, written as
# "major minor" or, with a third word, with minor before major.
series() {
  local dir version=$1
  shift
  dir=$(mktemp -d "$repo/series.XXXXXX")
  printf '{".": "%s"}\n' "$version" >"$dir/.release-please-manifest.json"
  printf '{"initial-version": "0.1.0"}\n' >"$dir/release-please-config.json"
  {
    echo 'apiVersion: clusterctl.cluster.x-k8s.io/v1alpha3'
    echo 'releaseSeries:'
    for entry in "$@"; do
      read -r major minor swapped <<<"$entry"
      if [ -n "$swapped" ]; then
        printf -- '- minor: %s\n  contract: v1beta2\n  major: %s\n' "$minor" "$major"
      else
        printf -- '- major: %s\n  minor: %s\n  contract: v1beta2\n' "$major" "$minor"
      fi
    done
    echo 'kind: Metadata'
  } >"$dir/metadata.yaml"
  echo "$dir"
}

expect pass "the initial version's series before the first release" bash "$scripts/release-series.sh" "$(series 0.0.0 "0 1")"
expect fail "no series for the initial version" bash "$scripts/release-series.sh" "$(series 0.0.0 "0 2")"
expect pass "the manifest's series among others" bash "$scripts/release-series.sh" "$(series 0.3.1 "0 1" "0 2" "0 3")"
expect pass "a series that names its minor first" bash "$scripts/release-series.sh" "$(series 0.3.0 "0 1" "0 3 swapped")"
expect fail "no series for the manifest's minor" bash "$scripts/release-series.sh" "$(series 0.4.0 "0 1" "0 3")"
expect fail "a series of another major" bash "$scripts/release-series.sh" "$(series 1.1.0 "0 1")"
expect fail "a minor that only starts like the manifest's" bash "$scripts/release-series.sh" "$(series 0.1.0 "0 10")"
expect pass "the repository's metadata.yaml" bash "$scripts/release-series.sh" "$scripts/../.."

if [ "$failures" != 0 ]; then
  echo "$failures cases failed" >&2
  exit 1
fi
