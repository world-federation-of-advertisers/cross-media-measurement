cd /home/marcopremier/cmm-entitykeys-20260701
git config rerere.enabled false
try() { # $1 branch  $2 old-parent-tip  $3 trial-parent  $4 out
  if ! git rev-parse "$3" >/dev/null 2>&1; then echo "SKIP     $1 (parent $3 missing)"; return 1; fi
  git checkout -q -B "$4" "origin/marcopremier/$1"
  if GIT_EDITOR=true git rebase --onto "$3" "$2" >/tmp/rb.log 2>&1; then
    echo "CLEAN     $1 -> $4"; return 0
  else
    echo "CONFLICT  $1 [$(grep -oiE 'could not apply [0-9a-f]+' /tmp/rb.log | head -1)]:"
    git diff --name-only --diff-filter=U | sed 's/^/       /'
    git rebase --abort >/dev/null 2>&1; return 1
  fi
}
# shared spine
try base-vid-labeling-tee-app-runner 8594a056d _trial_4009 _trial_4093 || exit 0
try vid-labeler-terraform 8d3b413b2 _trial_4093 _trial_4087 || exit 0
echo "--- Stack A ---"
if try vid-labeler-cloud-function-deployment 8aac756b2 _trial_4087 _trial_4065; then
  if try dlq-mark-edpa-failed f0ff94134 _trial_4065 _trial_4089; then
    try data-availability-header e237b76cf _trial_4089 _trial_4172
  fi
fi
echo "--- Stack B ---"
if try read-event-date 8aac756b2 _trial_4087 _trial_4162; then
  try vid-labeling-monitor-gaps 451243203 _trial_4162 _trial_4160
fi
