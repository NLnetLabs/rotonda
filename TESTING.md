# Testing

The tests of Rotonda live in a few places:

 - the `/tests`
 - unit tests in the source code
 - doctests in the source code

Running these tests is done with a single command:
```
cargo test --all-features --release
```

Use with `cargo nextest` is probably possible, but, well, untested.

## Snapshot testing

Some of the tests use snapshot testing using [insta]. You can run
them with the regular Rust test runner, but to change them, you might want to
install `cargo-insta` with `cargo install cargo-insta`. See the [insta] website for more information.

## Valgrind

If you want to submit `unsafe` code to Rotonda, we require that the entire
test suite is run under [Valgrind]. To do that locally, you'll need to install
Valgrind and `cargo-valgrind`.

If you have [just] installed, you can run the test suite under valgrind with

```
just valgrind
```

But you can also use the full expression without [just] if you prefer:

```
VALGRINDFLAGS="--suppressions=valgrind_suppressions.supp" cargo valgrind test --all-targets
```

## MIRI

A subset of tests is also run under MIRI, which is even more strict than
Valgrind, but also cannot run all tests.

You can run the relevant tests under MIRI with:

```
MIRIFLAGS=-Zmiri-disable-isolation cargo +nightly miri test
```

[insta]: https://insta.rs/
[Valgrind]: https://valgrind.org/
[just]: https://just.systems/man/en/
