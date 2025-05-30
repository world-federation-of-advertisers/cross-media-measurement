# Honest Majority Share Shuffle Utility Java Library

The HonestMajorityShareShuffleUtility java class is auto-generated from the
honest_majority_share_shuffle_utility.swig definition. The implementation of the
methods is written in c++ and the source codes are under
src/main/cc/wfa/measurement/internal/duchy/protocol/share_shuffle/crypto. We
create a swig wrapper on the library and call into the library via JNI in our
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
