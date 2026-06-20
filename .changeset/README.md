# Changesets

Adding a change to a release: run `pnpm changeset`, pick the bump (patch/minor/major), and describe
it. Commit the generated `.changeset/*.md` file in your PR.

On merge to `main`, the `version` workflow opens a **Version Packages** PR that bumps both SDK
packages (they are `fixed` together) and updates CHANGELOGs. Merging that PR + pushing a `vX.Y.Z`
tag triggers the gated release (`release.yml`).
