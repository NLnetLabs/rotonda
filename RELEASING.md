Rotonda RC and Final Release

For a final release the workflow is the same as for an RC. Just leave out the
`-rcXX` everywhere it is mentioned.

A. CREATE RELEASE BRANCH

  1. Create a release branch named release-x.y.0-rcz, e.g. release-0.1.0-rc0.
     Note: we start counting at ZERO for the rc, and there's no '.' or '-' in
     between rc and the number.
  2. Update the [package] version field in Cargo.toml
     (e.g. version = "0.1.0-rc0").
  3. Finalize Changelog.md:
      * Add release date in the form "Released yyyy-mm-dd.". Note the dot at
        the end.
      * Remove unused sections.
  4. Review & update dependencies in `Cargo.toml`.
  5. Run `cargo update`
  6. If the previous step actually updated any crates, run `cargo check
     --all-features --tests --release --examples`. If there are any warnings
     and/or errors, fix them and go back to step A.4.
  7. [library only] Run cargo +nightly test -Z minimal-versions --release
     --all-features. If there are any warnings and/or errors, fix them and go
     back to step A.4.
  8. Add and commit changes to this release branch.
  9. Run `cargo package`. This will try building the crate locally, and will
     output warnings and/or errors. If there are any, fix those, and add &
     commit these fixes. Run cargo package again to see if they are actually
     fixed.
 10. Push changes to GitHub
     (e.g. `git push --set-upstream origin release-0.1.i 0-rc0`).


B. CREATE PULL REQUEST

  1. Create pull request on GitHub main ← release-x.y.0-rcz and wait for CI to
     finish.
  2. Merge pull request. Add the changes for the release from Changelog.md as
     the commit message body (the GitHub web UI lets you do this).


C. TAG ON BRANCH MAIN AND RELEASE PACKAGE

  1. Check out branch `main` and pull.
  2. Double-check `Cargo.toml` and `Changelog.md`.
  3. Run `cargo publish`.
  4. Create and push release tag with tag of form vx.y.0-rcz, e.g. `git tag -a
     v0.1.0-rc0`.
  5. Add the changes for the release from Changelog.md as the tag message body
     (same as the merge commit message from the previous PR).
  6. Push the tag to GitHub (e.g. `git push --tags`).
  7. [Application Only] Create a release on GitHub.


D. CREATE POST RELEASE COMMIT

  1. Still in branch `main`. Add new section to `Changelog.md` called
     Unreleased new version.
  2. `Cargo.toml`: Increase version (e.g. 0.1.1-dev).
  3. [Application Only] Cargo.lock: Run cargo update -p <APPLICATION_NAME>,
     e.g. `cargo update -p rotonda`


E. CREATE PACKAGES ON PACKAGES.NLNETLABS.NL

  (not described here)
