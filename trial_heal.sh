set -e
cd /home/marcopremier/cmm-entitykeys-20260701
git config rerere.enabled false
# branch : old-parent-tip : trial-parent-branch : output-trial-branch
rows='
ranker-phase1:38e1adb2d:_trial_4083:_trial_4009
base-vid-labeling-tee-app-runner:8594a056d:_trial_4009:_trial_4093
vid-labeler-terraform:8d3b413b2:_trial_4093:_trial_4087
vid-labeler-cloud-function-deployment:8aac756b2:_trial_4087:_trial_4065
dlq-mark-edpa-failed:f0ff94134:_trial_4065:_trial_4089
data-availability-header:e237b76cf:_trial_4089:_trial_4172
read-event-date:8aac756b2:_trial_4087:_trial_4162
vid-labeling-monitor-gaps:451243203:_trial_4162:_trial_4160
'
for row in $rows; do
  br=$(echo "$row" | cut -d: -f1)
  oldp=$(echo "$row" | cut -d: -f2)
  tp=$(echo "$row" | cut -d: -f3)
  out=$(echo "$row" | cut -d: -f4)
  if ! git rev-parse "$tp" >/dev/null 2>&1; then echo "SKIP $br (parent $tp missing)"; continue; fi
  git checkout -q -B "$out" "origin/marcopremier/$br"
  if GIT_EDITOR=true git rebase --onto "$tp" "$oldp" >/tmp/rb.log 2>&1; then
    echo "CLEAN     $br -> $out"
  else
    echo "CONFLICT  $br :"
    git diff --name-only --diff-filter=U | sed 's/^/       /'
    git rebase --abort
  fi
done
