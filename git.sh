#!/usr/bin/env bash
# Push local changes to GitHub (uses existing origin remote).
# Does not commit secrets: .env and credential JSON patterns stay ignored via .gitignore.
#
# Usage (from repo root):
#   ./git.sh                          # status, commit with prompt, push main
#   ./git.sh "Your commit message"    # commit with message, push main
#   ./git.sh -b feature/x "msg"       # push branch feature/x (must exist locally or be current)
#   ./git.sh --dry-run                # show what would be committed
#
# Environment:
#   GIT_BRANCH  override branch to push (optional; default: current branch)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

BRANCH="${GIT_BRANCH:-}"
COMMIT_MSG=""
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    -b|--branch)
      BRANCH="${2:?}"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      grep '^#' "$0" | head -n 22 | sed 's/^# \?//'
      exit 0
      ;;
    *)
      if [[ -z "$COMMIT_MSG" ]]; then
        COMMIT_MSG="$1"
      else
        COMMIT_MSG="$COMMIT_MSG $1"
      fi
      shift
      ;;
  esac
done

if [[ -z "$BRANCH" ]]; then
  BRANCH="$(git rev-parse --abbrev-ref HEAD)"
fi

if ! git rev-parse --is-inside-work-tree &>/dev/null; then
  echo "ERROR: Not a git repository." >&2
  exit 1
fi

if ! git remote get-url origin &>/dev/null; then
  echo "ERROR: No 'origin' remote. Add one, e.g.:" >&2
  echo "  git remote add origin https://github.com/ORG/Reporting-browser-use.git" >&2
  exit 1
fi

ORIGIN_URL="$(git remote get-url origin)"
echo "=============================================="
echo " Git push — origin: $ORIGIN_URL"
echo " Branch: $BRANCH"
echo "=============================================="

git fetch origin 2>/dev/null || true

if [[ "$DRY_RUN" == 1 ]]; then
  echo ""
  echo ">>> Dry run — unstaged/staged changes:"
  git status --short
  echo ""
  git diff --stat
  exit 0
fi

echo ""
echo ">>> Staging (respects .gitignore)..."
git add -A
git status --short

if git diff --cached --quiet; then
  echo "Nothing to commit (working tree clean after staging)."
else
  if [[ -z "$COMMIT_MSG" ]]; then
    read -r -p "Commit message: " COMMIT_MSG
    COMMIT_MSG="${COMMIT_MSG:-chore: update}"
  fi
  git commit -m "$COMMIT_MSG"
fi

echo ""
if [[ "$(git rev-parse --abbrev-ref HEAD)" != "$BRANCH" ]]; then
  echo "WARN: You are on '$(git rev-parse --abbrev-ref HEAD)' but pushing '$BRANCH'." >&2
  echo "      Ensure commits exist on that branch (e.g. git checkout $BRANCH)." >&2
fi
echo ">>> Pushing to origin $BRANCH..."
git push -u origin "$BRANCH"

echo "Done."
