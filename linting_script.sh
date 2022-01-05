export PATH=/usr/local/bin:$PATH
find . -type f -name "*.cc" | xargs cpplint "$@"
find . -type f -name "*.h" | xargs cpplint "$@"
find . -type f -name "*.cc" | xargs clang-format --Werror --style=Google --i "$@"
find . -type f -name "*.h" | xargs clang-format --Werror --style=Google --i "$@"
#find . -type f -name "*.proto" | xargs clang-format --Werror --style=Google --i "$@"
#find . -type f -name "*.bazel" | xargs ~/go/bin/buildifier --mode=check --lint=warn "$@"
#find . -type f -name "*.bazel" | xargs ~/go/bin/buildifier "$@"
find . -type f -name "*.kt" | xargs java -jar ./ktfmt-0.29.jar --google-style "$@"
find . -type f -name "*.kt" | xargs ./ktlint -F --relative "$@" 1>&2
find . -type f -name "*.kt" | xargs java -jar ./google-java-format-1.13.0.jar --dry-run "$@"