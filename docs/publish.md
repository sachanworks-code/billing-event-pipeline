# Repository and development workflow

The project is published at [sachanworks-code/billing-event-pipeline](https://github.com/sachanworks-code/billing-event-pipeline) and pinned on [Vikash Sachan’s profile](https://github.com/sachanworks-code).

## Clone and run

```bash
git clone https://github.com/sachanworks-code/billing-event-pipeline.git
cd billing-event-pipeline
docker compose up --build -d
```

See the [README](../README.md) for API examples and the [operational notes](operations.md) for troubleshooting.

## Make changes

```bash
git switch -c your-change
./mvnw verify
```

Requires JDK 21 and a running Docker engine. Commit your changes and open a pull request. The Build and test workflow runs the contract, HTTP API, and Kafka/MySQL integration tests. Keep the hidden `.github` and `.mvn` directories and the executable permission on `mvnw` intact.

See [recorded local verification](verification.md) for the original test and synthetic load results. Those results describe the recorded environment, rather than promising performance on another machine.
