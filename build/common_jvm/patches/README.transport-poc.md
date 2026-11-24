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
BasicReportsReports job, and Python post-processor images are rebuilt. The
Python receiver allows 64 MiB. The optional scale test has a four-hour ceiling. The impression metadata audit and
its loopback regression keep the normal 4 MiB receive limit.

The same isolated experiment also applies #4831's bounded SQL statements to the
legacy PostgreSQL report and metric writers. All statements still execute inside
the original transaction. The unbounded writer failed with PostgreSQL 08P01
(invalid message format) after the transport and memory limits were raised.
These legacy-report changes are not part of PR #5013 or a recommendation for the
future multi-measurement writer.
