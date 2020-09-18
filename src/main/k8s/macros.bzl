"""Build rules for generating Kubernetes yaml from CUE files."""

def cue_dump(name, srcs):
    native.genrule(name = name, srcs = srcs, outs = [name + ".yaml"], tools = [], cmd = "cue cmd dump $(SRCS) > $@")
