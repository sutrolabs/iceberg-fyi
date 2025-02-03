# Iceberg FYI

Iceberg FYI is a suite of tests to validate various combinations of the Iceberg table format stack.

## Required tools

The test suite is built with Python and uses `uv` for package management.

In addition to Python, the suite uses a number of other tools:
- `docker` and `docker-compose` for isolation (on MacOs, recommend using OrbStack over Docker Desktop).
- AWS CLI (on MacOS, `brew install awscli`) for interacting with AWS services.

## Secrets

Many cloud services require secrets to be passed in via environment variables.

To get started, copy `.env.template` to `.env` and add the variables needed for your tests.

Altneratively, if you want to use Doppler, install it, then run `doppler setup`. The test runner will then automatically get secrets from your configured project.

## Running the integration tests

``` sh
uv run runner.py test ...
```

The test runner takes a particular combination of the iceberg stack and runs the test suite against it.

- `--storage` - the object store to use.
- `--catalog` - the catalog to use.
- `--query-engine` - the query engine to use.

In addition to the stack, there's a few other flags:
- `--record` - if set, the results of the test run will be recorded in the database.
- `--wait` - if set, the test runner will wait for the test to complete before exiting.

#### Examples of stacks:

Test MinIO+Nessie+Trino (pure local stack, requires docker-compose):

``` sh
uv run runner.py test --storage minio --catalog nessie --query-engine trino
```

Test S3+Nessie+Trino (requires AWS_ACCESS_* secrets):

``` sh
uv run runner.py test --storage s3 --catalog nessie --query-engine trino
```

### Running the stack

Set up stack but don't run the tests - useful for manual testing / debugging. Example for Nessie:

``` sh
uv run runner.py start --storage s3 --catalog nessie --wait
```

## Building new components

The test suite is designed to be extensible. To add a new component, you need to:



- Feel free to include the official SDKs for a component in the project dependencies.