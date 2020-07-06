def _java_wrap_cc_impl(ctx):
    name = ctx.attr.name
    module = ctx.attr.module

    src = ctx.file.src
    inputs = [src] + ctx.files.swig_includes

    outfile = ctx.outputs.outfile
    outputs = [outfile, ctx.outputs.java_output, ctx.outputs.jni_output]

    ctx.actions.run(
        outputs = outputs,
        inputs = inputs,
        executable = "swig",
        arguments = [
            "-c++",
            "-java",
            "-package",
            ctx.attr.package,
            "-module",
            module,
            "-outdir",
            outfile.dirname,
            "-o",
            outfile.path,
            src.path,
        ],
        mnemonic = "SwigCompile",
    )

_java_wrap_cc = rule(
    implementation = _java_wrap_cc_impl,
    attrs = {
        "src": attr.label(allow_single_file = True, mandatory = True),
        "swig_includes": attr.label_list(allow_files = True),
        "module": attr.string(mandatory = True),
        "package": attr.string(doc = "Java package.", mandatory = True),
        "outfile": attr.output(mandatory = True),
        "java_output": attr.output(mandatory = True),
        "jni_output": attr.output(mandatory = True),
    },
)

def java_wrap_cc(name, src, module, package, swig_includes = []):
    """
    Wraps C++ in Java using Swig.

    It's expected that the `swig` binary exists in the host's path.

    Args:
        name: target name.
        src: single .swig source file.
        module: name of Swig module.
        package: package of generated Java files.
        swig_includes: optional list of files included by Swig source.

    Outputs:
        {name}.cc
        {module}.java
        {module}JNI.java
    """
    _java_wrap_cc(
        name = name,
        src = src,
        module = module,
        package = package,
        swig_includes = swig_includes,
        outfile = name + ".cc",
        java_output = module + ".java",
        jni_output = module + "JNI.java",
    )
