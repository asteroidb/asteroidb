#!/usr/bin/env bash
#
# check-doc-drift.sh - Detect drift between API routes in code and docs.
#
# Compares route paths defined in src/http/routes.rs against the endpoint
# table in docs/getting-started.md. Exits non-zero if any routes are
# missing from either side.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

ROUTES_FILE="$ROOT_DIR/src/http/routes.rs"
DOCS_FILE="$ROOT_DIR/docs/getting-started.md"

if [[ ! -f "$ROUTES_FILE" ]]; then
    echo "ERROR: Routes file not found: $ROUTES_FILE"
    exit 1
fi

if [[ ! -f "$DOCS_FILE" ]]; then
    echo "ERROR: Docs file not found: $DOCS_FILE"
    exit 1
fi

echo "Checking for API document drift..."
echo "  Routes source: src/http/routes.rs"
echo "  Docs source:   docs/getting-started.md"
echo ""

# Extract unique route paths from routes.rs (production code only).
# Stop reading at #[cfg(test)] to exclude test-only paths, then match
# quoted strings like "/api/..." from .route() calls.
code_routes=$(sed '/#\[cfg(test)\]/,$d' "$ROUTES_FILE" \
    | grep -oP '"/api/[^"]+"' | tr -d '"' | sort -u)

# Extract unique API paths from the endpoint table in docs/getting-started.md.
# First filter lines that look like markdown table rows with HTTP methods,
# then pull out the backtick-quoted /api/ paths.
doc_routes=$(grep -P '\| `(GET|POST|PUT|DELETE)` \|' "$DOCS_FILE" \
    | grep -oP '`/api/[^`]+`' | tr -d '`' | sort -u)

errors=0

# Routes in code but missing from docs
missing_in_docs=()
while IFS= read -r route; do
    if ! echo "$doc_routes" | grep -qxF "$route"; then
        missing_in_docs+=("$route")
    fi
done <<< "$code_routes"

if [[ ${#missing_in_docs[@]} -gt 0 ]]; then
    echo "FAIL: Routes defined in code but missing from docs:"
    for route in "${missing_in_docs[@]}"; do
        echo "  - $route"
    done
    errors=$((errors + ${#missing_in_docs[@]}))
    echo ""
fi

# Routes in docs but missing from code
missing_in_code=()
while IFS= read -r route; do
    if ! echo "$code_routes" | grep -qxF "$route"; then
        missing_in_code+=("$route")
    fi
done <<< "$doc_routes"

if [[ ${#missing_in_code[@]} -gt 0 ]]; then
    echo "FAIL: Routes documented but not defined in code:"
    for route in "${missing_in_code[@]}"; do
        echo "  - $route"
    done
    errors=$((errors + ${#missing_in_code[@]}))
    echo ""
fi

if [[ "$errors" -gt 0 ]]; then
    echo "RESULT: $errors drift(s) detected. Please update docs/getting-started.md or src/http/routes.rs."
    exit 1
fi

total=$(echo "$code_routes" | wc -l | tr -d ' ')
echo "OK: All $total route path(s) match between code and docs."
exit 0
