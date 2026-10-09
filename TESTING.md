# Testing

After an outage, the first question is "how was this tested?", and the answer should be easy to find. Every extra test
stack is one more thing to maintain and debug, one more place where business logic may not actually run, and one more
place for tests to be mishandled or quietly lose coverage. So every OpenHouse test lives in one of four places.

## Where tests live

| Place | What belongs there | Gate |
|---|---|---|
| **Gradle** (default) | Anything that can be defined in this repository: unit tests, Spring Boot tests, `OpenHouseSparkITest`. Gradle owns its own infrastructure, for example an ephemeral MySQL from Testcontainers. | Runs locally, and gates pull requests in this repository's CI |
| **Integration tooling** | Integrations that can't live in this repository without significant work, such as services outside it, plus end-to-end and load tests against a deployed stack. | CI gate |
| **Acceptance tests** | Post-deploy acceptance tests. The integration tooling already reuses this logic; the target home is CD tooling, once that infrastructure exists. | Ideally a CD gate |
| **CD tooling** | Post-deploy soak and load test scripts. | CD gate |

Gradle is the only one of the four that runs in this repository. GitHub Actions workflows only run Gradle and
publishing, with no test logic of their own. Code that isn't on the JVM is tested in its own language through its own
build, the way the Python dataloader (`integrations/python/dataloader`) is through `make`.

## What we're moving away from

- docker-compose stacks driven from CI, and Python scripts that test the JVM services.
- Test logic in GitHub Actions workflow YAML.
- Manual docker-compose runs as test evidence. They're fine for local development, but they don't answer "how was this
  tested?".
- Manual testing on a shared test cluster.

## Rules

- A pull request's **Testing Done** names which of the four places its tests run in, and which tests.
- If a test can't run in one of the four places, move it into one, or start a discussion about why it can't.
- New tests follow this policy now. Existing exceptions are ported before they're deleted.

## Existing exceptions

- `.github/workflows/build-run-tests.yml` starts the `oh-only` docker-compose recipe and runs
  `scripts/python/integration_test.py`, which creates, reads, and deletes one table and checks only status codes.
  `TablesControllerTest` already covers that in Gradle; the only extra signal is that the Dockerfiles and the compose
  recipe still start.
- `.github/workflows/dataloader-tests.yml` starts the `oh-hadoop-spark` docker-compose recipe for the Python
  dataloader's integration tests.
