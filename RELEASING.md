Rotonda RC and Final Release

For a final release the workflow is the same as for an RC. Just leave out the
`-rcXX` everywhere it is mentioned.

A. CREATE RELEASE BRANCH

  * create release branch with name release-x.y.0-rcz, e.g. release-0.1.0-rc0.
    Note: we start counting at ZERO for the rc, and there's no '.' or '-' in
    between rc and the number.
  * Update [package] version field in Cargo.toml (e.g. version = "0.1.0-rc0").
  * Finalize Changelog.md:
  * Add release date in the form "Released yyyy-mm-dd.". Note the dot at the
    end.
  * Remove unused sections.
  * Review & update dependencies in `Cargo.toml`.
  * Run `cargo update`
  * If the previous step actually updated any crates, run `cargo check
    --all-features --tests --release --examples`. If there are any warnings
    and/or errors, fix them and go back to step A.4.
  * [library only] Run cargo +nightly test -Z minimal-versions --release
    --all-features. If there are any warnings and/or errors, fix them and go
    back to step A.4.
  * Add and commit changes to this release branch.
  * Run `cargo package`. This will try building the crate locally, and will
    output warnings and/or errors. If there are any, fix those, and add &
    commit these fixes. Run cargo package again to see if they are actually
    fixed.
  * Push changes to GitHub (e.g. `git push --set-upstream origin release-0.1.i
    0-rc0`).


B. CREATE PULL REQUEST

  * Create pull request on GitHub main ← release-x.y.0-rcz and wait for CI
    to finish.
  * Merge pull request. Add the changes for the release from Changelog.md as
    the commit message body (the GitHub web UI lets you do this).


C. TAG ON BRANCH MAIN AND RELEASE PACKAGE

  * Check out branch `main` and pull
  * Double-check `Cargo.toml` and `Changelog.md`.
  * Run `cargo publish`
  * Create and push release tag with tag of form vx.y.0-rcz, e.g. `git tag -a
    v0.1.0-rc0`.
  * Add the changes for the release from Changelog.md as the tag message body
    (Same as the merge commit message from the previous PR).
  * Push the tag to GitHub (e.g. `git push --tags`).
  * [Application Only] Create a release on GitHub.


D. CREATE POST RELEASE COMMIT

  * Still in branch `main`. Add new section to `Changelog.md` called
    Unreleased new version.
  * `Cargo.toml`: Increase version.
  * [Application Only] Cargo.lock: Run cargo update -p <APPLICATION_NAME>,
    e.g. `cargo update -p rotonda`


E. CREATE PACKAGES ON PACKAGES.NLNETLABS.NL

  (not described here)
