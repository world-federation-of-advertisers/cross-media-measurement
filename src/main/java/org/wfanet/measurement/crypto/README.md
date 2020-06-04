# Protocol Encryption Utility Java Library

The ProtocolEncryptionUtility java class is auto-generated from the
protocol_encryption_utility.swig definition. The implementation of the methods
is written in c++ and the source codes are under src/main/cc/measurement/crypto.
We create a swig wrapper on the library and call into the library via JNI in our
java code.

## Possible errors when using the JNI java library.

### swig uninstalled

To keep the library updated, each time when the java library is built, it would
run a swig command (provided in the BUILD.bazel rule) to re-generate all the
swig wrapper files using the latest c++ codes. As a result, the swig software is
required to build the java library. Install swig before building the package.

For example, in a system using apt-get, run the following command to get swig:

```shell
sudo apt-get install swig
```

### GRTE error (e.g., version `GLIBC_2.29' not found)

If you are developing the code in some customized linux systems, e.g., glinux.
There may be issue. For example, on a glinux machine, the openjdk will be
shadowed by the Google JDK. Since our project is an open source project, it
shouldn't be linking against GRTE (google runtime environment, aka Google3
libraries). To run the binary or test using a specific javabase, use the
following command:

```
bazel test --javabase=@bazel_tools//tools/jdk:absolute_javabase --define=ABSOLUTE_JAVABASE=/usr/lib/jvm/java-11-openjdk-amd64 //src/test/java/org/wfanet/measurement/crypto:tests
```

TODO: add a workspace rule that downloads OpenJDK 11, a java_runtime rule that
uses it, and then update the .bazelrc in the repo to use that for --javabase
