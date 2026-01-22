# Contributing to Rotonda

Hi! Thank you for wanting to contribute to Rotonda.

This document offers guidelines on how best to contribute. Following the
guidelines here give your contributions the best chance at getting merged.

## Code of Conduct

This project and everyone participating in it is governed by the [NLnet Labs
Code of Conduct]. By participating, you are expected to uphold this code.

## A note on Generative AI

We do not accept any content -- code, documentation or otherwise -- generated
by large language models or other generative AI.

## Discussion

### Asking questions

The best place to ask questions is on [Discourse]. Please look through
existing posts before creating a new thread.

### Reporting security vulnerabilities

See [SECURITY.md].

### Reporting other bugs

If you find something that is clearly a bug, you can submit it directly to the
issue tracker. Please look through existing issues before creating a new issue.
You should also check whether your issue has already been addressed on the
`main` branch.

Try to include as much as possible of the following information:

 - Version/branch/commit of Rotonda you are using
 - Your system (OS, architecture)
 - Any stack traces or logging messages
 - A reproducer that shows the issue

### Suggesting enhancements

If you have ideas for improving Rotonda, you can start a discussion about it
on [Discourse]. Please look through other posts to see whether your idea has
been discussed before.

Be sure to include clear use-cases and motivation for your idea.

## Contribution

### License and copyright

When contributing with code or other content, you agree to put your changes
and new code under the same license Rotonda and its associated libraries is
already using. When changing existing source code, do not alter the copyright
of the original file(s). The copyright will still be owned by the original
creator(s) or those who have been assigned copyright by the original
author(s).

By submitting a patch to Rotonda, you are assumed to have the right to
the code and to be allowed by your employer or whomever to hand over that
patch/code to us. We will credit you for your changes as far as possible, to
give credit but also to keep a trace back to who made what changes.

### Contributing documentation

Writing documentation is very hard, but _very_ appreciated. Improving
the documentation could mean anything from adding new chapters to
fixing a typo. The documentation of Rotonda has a repository of its
[own](https://github.com/NLnetLabs/rotonda-doc).

 - Go to the `/manual/docs/` folder of this repository.

The documentation in the `/manual/docs/` folder is written in
[reStructuredText] and hosted on [ReadTheDocs] via [Sphinx]. You can easily
build the documentation locally with

```
make html
```

You can then serve those files however you want, for example using

```
python -m http.server
```

You can submit your changes in a pull request. A preview version of the updated
documentation will be built automatically for your PR. You can view it by
clicking on the ReadTheDocs action on the PR page or by visiting

```
https://rotonda--XXX.org.readthedocs.build/
```

where `XXX` is the number of your PR.

### Contributing code

Before you start contributing code, ensure that the issue you are tackling has
been properly discussed either on [Discourse] or the [issue tracker]. Do not
submit pull requests for changes that have not been discussed.

You can contribute code by opening a pull request against the branch called
`dev`. Opening a PR against any other branch will have to be rebased to `dev`.
Before you do you should check that you code passes all tests and adheres to
the [style guide](#style-guide). You do not have to update `Changelog.md`.
That will be updated when a release is tagged.

You run the code, you'll need to have a working and recent enough Rust
toolchain installed. The [just] tool can also come in handy to run some common
commands on this repository.

Keep the following in mind while submitting your code:

 - Work on a branch of your own created branched of the branch called `dev` in
   our repository.
 - Start the name of your branch with `feature-` or `bugfix-` or another
   small functional description. Add a small description of the content of
   the changes/additions, e.g. 'tcp-ao', so the full branch name would be
   `feature-tcp-ao`.
 - Make sure to link the appropriate issue for your change. If there is none,
   make the issue and discuss the change before submitting the PR.
 - Use descriptive commit messages.
 - Your code can only be merged once the CI is green.
 - Keep your PRs self-contained and do not include unrelated changes. It's
   better to submit many small PRs than one big PR with multiple changes.

See [TESTING.md] for information on how to test your changes.

### Style guide

We follow standard style guides for Rust code. That is, we require that the
code is formatted by `rustfmt` and that it passes the checks of the latest
version of the `clippy` linter. The code should also be free of warnings
generated by `rustc`.

Rotonda tries to minimize on `unsafe` code. This is not to say that we don't
rely on `unsafe` code, but we are trying to isolate it in separate crates as
much as possible, i.e. the `routedb` crate. If you really must use `unsafe`
code, we require that Valgrind and Miri do not raise any issues on testing.
Each `unsafe` block must be accompanied by a `SAFETY` comment and each `unsafe`
function should have a "Safety" section in its documentation. We generally do
not rely on `unsafe` just to achieve better performance.

## Maintainance

This section is meant for maintainers of Rotonda, not for all contributors.

### Dependencies

Be careful pulling in new dependencies. We should not pull in large
dependencies for small gains. The license should also be compatible with
Rotonda's license. You can use `cargo deny` to check that.

### Releases

The steps for releasing a new version of Rotonda are described in RELEASING.md
in the root of this repository.

[NLnet Labs Code of Conduct]: https://www.nlnetlabs.nl/conduct
[Discourse]: https://community.nlnetlabs.nl/c/rotonda/5
[issue tracker]: https://github.com/rotonda/NLnetLabs/issues
[reStructuredText]: http://www.sphinx-doc.org/en/stable/rest.html
[ReadTheDocs]: https://readthedocs.org/
[Sphinx]: http://www.sphinx-doc.org
[uv]: https://docs.astral.sh/uv/
[just]: https://just.systems/man/en/
[SemVer]: https://semver.org/
[SECURITY.md]: ./SECURITY.md
[TESTING.md]: ./TESTING.md
