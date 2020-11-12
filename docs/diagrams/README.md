# PlantUML Diagrams

The diagram images are generated using [PlantUML](https://plantuml.com/) and
optimized with [OptiPNG](http://optipng.sourceforge.net/).

## Generating Diagrams

Run the following:

```sh
bazel build //tools:generate_plantuml_diagrams && bazel-bin/tools/generate_plantuml_diagrams
```

It's assumed that `java` and `optipng` are in the user's `PATH`.
