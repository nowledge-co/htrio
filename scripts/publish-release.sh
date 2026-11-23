#!/usr/bin/env bash
set -euo pipefail

tag=${1:-}
if [[ -z "$tag" ]]; then
    echo "usage: $0 vX.Y.Z" >&2
    exit 2
fi

if [[ ! "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "invalid release tag: $tag" >&2
    exit 2
fi

head_tag=$(git describe --tags --exact-match HEAD 2>/dev/null || true)
if [[ "$head_tag" != "$tag" ]]; then
    echo "HEAD must be the exact $tag tag (found: ${head_tag:-untagged})" >&2
    exit 1
fi

metadata=$(cargo metadata --locked --no-deps --format-version 1)
client_version=$(jq -r '.packages[] | select(.name == "htrio") | .version' <<<"$metadata")
if [[ "v$client_version" != "$tag" ]]; then
    echo "tag $tag does not match htrio version $client_version" >&2
    exit 1
fi

packages=(
    htrio-quinn-udp
    htrio-quinn-proto
    htrio-quinn
    htrio-h3
    htrio-h3-datagram
    htrio-h3-quinn
    htrio
)

crate_exists() {
    local package=$1
    local version=$2
    local status
    status=$(curl --silent --show-error --location \
        --user-agent "htrio-release/${GITHUB_RUN_ID:-local}" \
        --output /dev/null --write-out '%{http_code}' \
        "https://crates.io/api/v1/crates/$package/$version")
    case "$status" in
        200) return 0 ;;
        404) return 1 ;;
        *)
            echo "crates.io returned HTTP $status for $package@$version" >&2
            exit 1
            ;;
    esac
}

for package in "${packages[@]}"; do
    version=$(jq -r --arg package "$package" \
        '.packages[] | select(.name == $package) | .version' <<<"$metadata")
    if [[ -z "$version" ]]; then
        echo "workspace package not found: $package" >&2
        exit 1
    fi

    if crate_exists "$package" "$version"; then
        echo "already published: $package@$version"
        continue
    fi

    echo "publishing $package@$version"
    cargo publish --locked --package "$package"

    visible=false
    for _ in {1..18}; do
        if crate_exists "$package" "$version"; then
            visible=true
            break
        fi
        sleep 10
    done
    if [[ "$visible" != true ]]; then
        echo "$package@$version was uploaded but is not yet visible on crates.io" >&2
        exit 1
    fi
done

echo "all workspace packages for $tag are visible on crates.io"
