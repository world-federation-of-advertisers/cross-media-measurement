# Updating to a New Release

How to update Halo components to a new release version.

This guide assumes that you maintain all of your Kubernetes object configuration
files, for example in a Kustomization directory as described in the deployment
guides.

## From a revision after the first release

This is the process for updating from a known release version or a revision
between two known release versions.

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
    *   Update image names for each
        [`Container`](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#Container)
        in each `PodSpec`.

        Pre-built images are published for each release on
        [GitHub Packages](https://github.com/orgs/world-federation-of-advertisers/packages?repo_name=cross-media-measurement).
        Use the release version as the image tag. For example,
        `ghcr.io/world-federation-of-advertisers/kingdom/v2alpha-public-api:0.3.0`
        is the published v2alpha Kingdom public API server image for the
        [0.3.0 release](https://github.com/world-federation-of-advertisers/cross-media-measurement/releases/tag/v0.3.0).

3.  Diff the Kubernetes object configuration changes

    Assuming you have `kubectl` configured to point to your cluster, you can use
    `kubectl diff`. Verify that diff is what you expect.

    Note: You may want to pass `--prune --prune-allowlist=apps/v1/Deployment
    --selector='app.kubernetes.io/part-of=halo-cmms'` to `kubectl` commands to
    ensure that old/renamed Deployment resources are pruned.

4.  Apply the new Kubernetes object configuration

    Run `kubectl apply`.

## From a revision before the first release

For revisions prior to the first release
([0.1.0](https://github.com/world-federation-of-advertisers/cross-media-measurement/releases/tag/v0.1.0)),
the starting state is basically unknown as are no release notes to determine
what actions need to be taken. Therefore, the recommended process is to generate
new configurations and manually edit them with any customizations.

Follow the deployment guide in the [docs](../) folder for the desired component.
If your existing infrastructure is not managed using Terraform, you can use the
[`import`](https://developer.hashicorp.com/terraform/cli/import) subcommand of
the Terraform CLI to import your existing resources.
