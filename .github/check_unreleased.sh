#!/bin/sh
# Refuses to publish for a tag that already has a GitHub release. Every job that pushes
# a registry artifact runs this first: re-running one of them after the release would push
# a new digest (the chart is not byte-reproducible) under a release page that names the
# first run's. HTTP 404 proceeds, 200 refuses, anything else fails: an unanswered check is
# not permission. Needs GH_TOKEN, GITHUB_REPOSITORY and GITHUB_REF_NAME.
set -eu
code=$(gh api -i "repos/$GITHUB_REPOSITORY/releases/tags/$GITHUB_REF_NAME" 2>/dev/null \
  | head -1 | awk '{ print $2 }') || true
case "$code" in
  404) ;;
  200) echo "$GITHUB_REF_NAME is already released: nothing is published again"; exit 1 ;;
  *) echo "could not tell whether $GITHUB_REF_NAME is released (HTTP ${code:-none})"; exit 1 ;;
esac
