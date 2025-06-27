# Code coverage in Kotlin

## Jacoco issues and instrumentation filter
There is a known [issue](https://github.com/bazelbuild/rules_kotlin/issues/509) with Jacoco when instrumenting Kotlin inline functions. Due to these challenges, we employ the `instrumentation_filter` flag to exclude certain build rules from Jacoco’s analysis. This exclusion is necessary to avoid build failures, which can arise when Jacoco attempts to process inline functions.

## Developer guidelines for handling Jacoco issues
If the build breaks due to Jacoco issues, the developer must expand the instrumentation filter by adding an exclusion for the specific build rule that is now causing the failure.

This error can be identified in the logs by the inclusion of the following message:

`Caused by: java.lang.IllegalArgumentException: Illegal LDC constant $jacocoData : Ljava/lang/Object; org/wfanet/path/to/SomeClass.$jacocoInit(...`

To expand the filter, locate the package containing the build rule for the source file in question and add an exclusion (prefixed with a comma and a minus sign) in the `instrumentation_filter` flag of [build-test.yml](../.github/workflows/build-test.yml). For instance:
`,-src/main/kotlin/path/to/incompatible/package:`

### Example:

bazel coverage --combined_report=lcov --instrumentation_filter=//src/main/kotlin/.*,-@.*,-src/main/kotlin/org/wfanet/measurement/api/v2alpha:,-src/main/kotlin/org/wfanet/measurement/common/grpc:,-src/main/kotlin/org/wfanet/panelmatch/client/logger:,-src/main/kotlin/org/wfanet/panelmatch/common/beam/testing:,-src/main/kotlin/org/wfanet/measurement/access/client/v1alpha/testing:,-src/main/kotlin/org/wfanet/measurement/access/service/internal:,-src/main/kotlin/org/wfanet/panelmatch/common/beam:,-src/main/kotlin/org/wfanet/panelmatch/common:**,-src/main/kotlin/path/to/incompatible/package:**

## Report format
Currently, the code coverage report is available only as a .dat file in the output of the GitHub Actions (GHA) workflow. This is a temporary measure until integration with Codecov is complete.

## Final notes
This setup using the instrumentation filter is a temporary workaround until the Jacoco issues with inline functions are resolved.
