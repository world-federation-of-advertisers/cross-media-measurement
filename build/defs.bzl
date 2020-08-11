"""Utility functions definitions."""

def _label(target):
    if target.startswith("@") or target.startswith("//"):
        return Label(target)

    return Label("{repo}//{package}".format(
        repo = native.repository_name(),
        package = native.package_name(),
    )).relative(target)

def test_target(target):
    """Returns the label for the corresponding target in the test tree."""
    label = _label(target)
    test_package = label.package.replace("src/main/", "src/test/", 1)
    return Label("@{workspace}//{package}:{target_name}".format(
        workspace = label.workspace_name,
        package = test_package,
        target_name = label.name,
    ))
