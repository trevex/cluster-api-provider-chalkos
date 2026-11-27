#!/usr/bin/env bash
# Checks that metadata.yaml lists a release series for the version that release-please releases
# next or released last, which clusterctl needs to install that version:
#
#   release-series.sh [DIR]
#
# The version is the one .release-please-manifest.json of DIR, by default the current directory,
# records. Before the first release the manifest records 0.0.0, and the version is the
# initial-version of release-please-config.json. release-please cannot add a release series, so a
# pull request to main adds it before the release pull request that needs it can be merged.
set -euo pipefail

dir=${1:-.}
version=$(jq -r '."."' "$dir/.release-please-manifest.json")
if [ "$version" = 0.0.0 ]; then
  version=$(jq -r '."initial-version"' "$dir/release-please-config.json")
fi
if ! [[ $version =~ ^([0-9]+)\.([0-9]+)\.[0-9]+ ]]; then
  echo "the version '$version' is not of the form x.y.z" >&2
  exit 1
fi
major=${BASH_REMATCH[1]} minor=${BASH_REMATCH[2]}

# The major.minor of each entry of releaseSeries, one per line. An entry starts with "- " and
# names its major and minor in any order.
series=$(awk '
  /^releaseSeries:/ { in_series = 1; next }
  /^[^ -]/ { in_series = 0 }
  !in_series { next }
  /^- / { if (entry) print major "." minor; entry = 1; major = ""; minor = ""; sub(/^- /, "  ") }
  /^ +major:/ { major = $2 }
  /^ +minor:/ { minor = $2 }
  END { if (entry) print major "." minor }
' "$dir/metadata.yaml")

if grep -qxF "$major.$minor" <<<"$series"; then
  echo "metadata.yaml lists the release series $major.$minor of $version"
  exit 0
fi
if [ -n "${GITHUB_ACTIONS:-}" ]; then
  echo "::error file=metadata.yaml,title=No release series::metadata.yaml lists no release series $major.$minor for $version"
fi
{
  echo "metadata.yaml lists no release series $major.$minor, which clusterctl needs to install $version."
  echo "It lists: $(tr '\n' ' ' <<<"$series")"
  echo "Add an entry to releaseSeries in a pull request to main, with the Cluster API contract the"
  echo "release implements:"
  echo
  echo "  - major: $major"
  echo "    minor: $minor"
  echo "    contract: v1beta2"
} >&2
exit 1
