#!/usr/bin/env bash
# Push this project to GitHub: https://github.com/nithintodc/Reporting-browser-use.git
# Run from project root: ./push_to_github.sh
# Ensure .env is not committed (it's in .gitignore).

set -e
REPO_URL="https://github.com/nithintodc/Reporting-browser-use.git"
BRANCH="${1:-main}"

if ! git rev-parse --is-inside-work-tree &>/dev/null; then
  echo "Initializing git repository..."
  git init
fi

if ! git remote get-url origin &>/dev/null; then
  echo "Adding remote origin..."
  git remote add origin "$REPO_URL"
else
  echo "Setting remote origin to $REPO_URL..."
  git remote set-url origin "$REPO_URL"
fi

echo "Staging files (respecting .gitignore)..."
git add -A
git status

echo ""
read -p "Commit message [default: Initial commit: DoorDash report automation]? " MSG
MSG="${MSG:-Initial commit: DoorDash report automation}"
git commit -m "$MSG" || true

echo "Pushing to origin $BRANCH..."
git branch -M "$BRANCH"
git push -u origin "$BRANCH"

echo "Done. Repo: $REPO_URL"
