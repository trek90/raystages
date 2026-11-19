# Verification

- Python 3.12 on Linux: built and installed the 0.1.0 wheel. From outside
  the source tree, 19 tests passed. These cover scheduler behavior with
  local threads and the Ray backend's API contract with mocks.
- The example ran with the thread backend, producing three correct scores
  and isolating one bad record at its parsing stage.
- Real-Ray integration is unverified. Ray 2.60.0 initially could not write
  its authentication token under the read-only home directory. With a
  temporary writable home, Ray generated the token but its driver could not
  reach the cluster control service before the test timed out. A separate
  socket probe confirmed this environment also denies AF_UNIX creation.
  The opt-in test is included for an environment where Ray can start.
