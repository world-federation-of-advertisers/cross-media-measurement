import re, sys
p = 'src/main/proto/wfa/measurement/edpaggregator/v1alpha/vid_rank_builder_params.proto'
s = open(p).read()
# keep-both: base's `reserved 10;` PLUS #4009's model_storage_params block
m = re.search(r'<<<<<<< HEAD\n(.*?)\n=======\n(.*?)\n>>>>>>> [0-9a-f]+ \([^\n]*\)\n', s, re.S)
if not m:
    print('NO_CONFLICT_MARKERS'); sys.exit(2)
head = m.group(1)      # reserved 10;
theirs = m.group(2)    # model_storage_params block
replacement = head + '\n\n' + theirs + '\n'
s = s[:m.start()] + replacement + s[m.end():]
assert '<<<<<<<' not in s
open(p, 'w').write(s)
print('RESOLVED keep-both (reserved 10 + model_storage_params)')
