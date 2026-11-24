# Dev-only reporting transport experiment

This branch is not intended to merge. It isolates the large-report transport and
deadline experiment from PR #5013's fixture and test coverage.

The 90-day, four-EDP report with demographic groupings and filtered device/media
components produced a 22,946,459-byte internal CreateReport request. The deployed
internal Reporting server rejected it above the 4 MiB receive limit, before any
measurements were created.

The dependency patch preserves the normal 4 MiB receive limit and 30-second
channel deadline unless JVM properties are set. For this experiment only,
Reporting components use 64 MiB and a 600-second channel deadline. The gateway
receives up to 64 MiB; the test allows a ten-minute HTTP response deadline.
These are experiment ceilings, not proposed API requirements.

Only the internal Reporting server, public Reporting server, Reporting gateway,
and BasicReportsReports job image are rebuilt. The impression metadata audit and
its loopback regression keep the normal 4 MiB receive limit.
