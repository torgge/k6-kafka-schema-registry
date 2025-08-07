# How to use

## Run everything

To start all services and run the k6 load test, use the following command:

```shell
docker-compose up --build
```

The test results will be saved in the `kafka/results` directory.
Press `Ctrl+C` to stop all services after the test is done.

```shell
 docker-compose -f docker-compose.yaml up --build --remove-orphans k6-kafka
```

```shell
docker-compose -f docker-compose.yaml up --remove-orphans kafka-k6 kafka-ui-k6 schema-registry-k6
```