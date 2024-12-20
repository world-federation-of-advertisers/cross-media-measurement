# Updating to a New Release

How to update Halo components to a new release version.

## Assumptions

You maintain your Kubernetes object configurations, such as the Kustomization
directory. Keeping these under version control is recommended.

## Process

1.  Read the release notes

    Notes for each release can be found on
    [Releases](https://github.com/world-federation-of-advertisers/cross-media-measurement/releases).
    Read through the notes for each release that is after your current revision
    through the release that you wish to update to. In particular, read through
    the "Potentially Requiring Action" section of each release.

    The release notes will also contain information on how to configure/enable
    new features.

2.  Update Kubernetes object configuration files

    *   Make any changes indicated in the release notes.
    *   Update `image` field for each
        [`Container`](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#Container)
        in each `PodSpec`.

        Pre-built images are published for each release on
        [GitHub Packages](https://github.com/orgs/world-federation-of-advertisers/packages?repo_name=cross-media-measurement).
        Use the release version as the image tag. For example,
        `ghcr.io/world-federation-of-advertisers/kingdom/v2alpha-public-api:0.5.7`
        is the published v2alpha Kingdom public API server image for the
        [0.5.7 release](https://github.com/world-federation-of-advertisers/cross-media-measurement/releases/tag/v0.5.7).

3.  Diff the Kubernetes object configuration changes (recommended)

    Assuming you have `kubectl` configured to point to your cluster, you can use
    `kubectl diff`. Verify that diff is what you expect.

4.  Apply the new Kubernetes object configuration

    Run `kubectl apply`. You may want to pass the `--prune` option to ensure
    that object deletions or renames are applied properly. The safest way to do
    this is using an ApplySet. See
    https://kubernetes.io/blog/2023/05/09/introducing-kubectl-applyset-pruning/.
