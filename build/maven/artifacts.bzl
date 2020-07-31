def _list_to_dict(artifact_list):
    """Returns a dict of artifact name to version."""
    tuples = [tuple(item.rsplit(":", 1)) for item in artifact_list]
    return {name: version for (name, version) in tuples}

def _dict_to_list(artifact_dict):
    """Returns a list artifacts from a dict of name to version."""
    return [
        ":".join([name, version])
        for (name, version) in artifact_dict.items()
    ]

artifacts = struct(
    list_to_dict = _list_to_dict,
    dict_to_list = _dict_to_list,
)
