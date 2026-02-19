#!/usr/bin/env bash
set -euo pipefail

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root"

missing=0

required=(
  review/skills-shell-tips.md
  artifacts/agent/.gitkeep
)

for file in "${required[@]}"; do
  if [[ ! -f "$file" ]]; then
    echo "Missing required skills-shell artifact: $file"
    missing=1
  fi
done

if ! rg -Fq 'review/skills-shell-tips.md' AGENTS.md; then
  echo "AGENTS.md missing reference: review/skills-shell-tips.md"
  missing=1
fi

if ! rg -Fq 'artifacts/agent/' AGENTS.md; then
  echo "AGENTS.md missing reference: artifacts/agent/"
  missing=1
fi

skill_files=$(rg --files -g '**/SKILL.md' || true)
if [[ -z "${skill_files:-}" ]]; then
  echo "No in-repo SKILL.md files found; skills lint skipped."
else
  while IFS= read -r skill; do
    [[ -z "$skill" ]] && continue
    if ! rg -qi 'use when|when to use' "$skill"; then
      echo "SKILL missing routing trigger section: $skill"
      missing=1
    fi
    if ! rg -qi "don't use|do not use|avoid when" "$skill"; then
      echo "SKILL missing anti-trigger section: $skill"
      missing=1
    fi
    if ! rg -qi 'example|template' "$skill"; then
      echo "SKILL missing examples/templates: $skill"
      missing=1
    fi
  done <<< "$skill_files"
fi

if [[ $missing -ne 0 ]]; then
  exit 1
fi

echo "Skills + shell tips check OK"
