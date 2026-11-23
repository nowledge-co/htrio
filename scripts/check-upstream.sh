#!/usr/bin/env bash
set -euo pipefail

root=$(git rev-parse --show-toplevel)
lock_file="$root/UPSTREAM.lock"
drift=0

echo "# htrio upstream review"
echo
echo "| Source | Tracked ref | Reviewed head | Current head | Status |"
echo "| --- | --- | --- | --- | --- |"

while IFS='|' read -r name repository tracked_ref reviewed_head provenance document; do
    [[ -z "$name" || "$name" == \#* ]] && continue

    if ! grep -Fq "$provenance" "$root/$document"; then
        echo "missing provenance $provenance in $document" >&2
        exit 2
    fi

    if ! result=$(git ls-remote "$repository" "$tracked_ref"); then
        echo "unable to query $name $tracked_ref" >&2
        exit 2
    fi
    current_head=${result%%$'\t'*}
    if [[ -z "$current_head" || "$current_head" == "$result" ]]; then
        echo "unable to resolve $name $tracked_ref" >&2
        exit 2
    fi

    status="reviewed"
    if [[ "$current_head" != "$reviewed_head" ]]; then
        status="review required"
        drift=1
    fi
    echo "| $name | \`$tracked_ref\` | \`$reviewed_head\` | \`$current_head\` | $status |"
done < "$lock_file"

echo
echo "Update the reviewed head only after inspecting upstream security, protocol, and compatibility changes."
exit "$drift"
