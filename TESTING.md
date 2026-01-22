# Testing

The tests of Rotonda live in a few places:

 - unit tests in the source code
 - doctests in the source code
 - end-to-end tests in a separate, private repo called REEDS


Running these tests is done with a single command:
```
cargo test --all-features --release
```


## End-to-end testing (REEDS)

The end-to-end tests for Rotonda are maintained in a separate repository that
is private. Keeping it private enables us to store real-world network data,
which is used as input in the test suites.

While this makes the end-to-end tests invisible, we do encourage contributors
to think about what types of input data would be required to test their
contributions in an end-to-end fashion. For example, when a PR adds certain new
fields to the response of an API endpoint, we should create a test where the
input (e.g. pcap or binary BMP) contains PDUs that result in API responses
featuring the newly added output.

Ideally, a PR is accompanied with a pcap or likewise (perhaps transferred to
use in private) for us to create an end-to-end test from. This might not always
be trivial: in that case, please reach out to discuss how we can proceed to get
proper testing in place.
