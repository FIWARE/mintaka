# <a name="top"></a>Mintaka

[![FIWARE Core Context Management](https://nexus.lab.fiware.org/repository/raw/public/badges/chapters/core.svg)](https://www.fiware.org/developers/catalogue/)
[![License badge](https://img.shields.io/github/license/FIWARE/context.Orion-LD.svg)](https://opensource.org/licenses/AGPL-3.0)
[![Docker badge](https://img.shields.io/docker/pulls/fiware/mintaka.svg)](https://hub.docker.com/r/fiware/mintaka/)
[![NGSI-LD badge](https://img.shields.io/badge/NGSI-LD-red.svg)](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.04.01_60/gs_cim009v010401p.pdf)
[![Coverage Status](https://coveralls.io/repos/github/FIWARE/mintaka/badge.svg)](https://coveralls.io/github/FIWARE/mintaka)
[![Test](https://github.com/FIWARE/mintaka/actions/workflows/test.yml/badge.svg)](https://github.com/FIWARE/mintaka/actions/workflows/test.yml)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/4751/badge)](https://bestpractices.coreinfrastructure.org/projects/4751)

Mintaka is an implementation of the [NGSI-LD](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.04.01_60/gs_cim009v010401p.pdf) temporal retrieval
api. It relies on the [Orion-LD Context Broker](https://github.com/FIWARE/context.Orion-LD) to provide the underlying database.
The NGSI-LD specification is a living, changing document, and the latest Orion-LD beta release is nearly feature complete to the
1.3.1 ETSI specification. 

For more information on Mintaka, please refer to its [github repo](https://github.com/FIWARE/mintaka)

## Tags

All images are created using the [CI pipeline](https://github.com/FIWARE/mintaka/tree/main/.github/workflows). 
We create the following types of tags:

| Tag pattern | Example | Description |
| ------ | ------ | ----- |
| <SEM_VER> | 0.0.1 | Release version, equal to the github release |
| latest | latest | Latest released image. Be careful, this is bleeding edge and not guranteed to be stable | 
| <SEM_VER>-PRE-<PR_NUMBER> | 0.0.2-PRE-12 | Prerelease version. <SEM_VER> is the next release version, <PR_NUMBER> the number of the associated PR. |

Each tag exists in 3 versions: 
- no postfix: the default image, based on  ``gcr.io/distroless/java:11``
- ```-distroless``` postfix: image based on ``gcr.io/distroless/java:11``, f.e. 0.0.1-distroless
- ```-rhel``` postfix: image based on the [Redhat certified base-image](https://catalog.redhat.com/software/containers/explore) ```openjdk-11-rhel7```, f.e. 0.0.1-rhel

## How to use

Mintaka relies on a [TimescaleDB](https://www.timescale.com/), that is populated by [Orion-LD](https://github.com/FIWARE/context.Orion-LD).
A sample setup can be found below, please check [github](https://github.com/FIWARE/mintaka/tree/main/src/test/resources/docker-compose) for more options:
```yaml
version: "3.5"
services:
  # Orion is the context broker
  orion-ld:
    image: fiware/orion-ld
    hostname: orion
    # sometimes the initial startup fails due to a weird timescale behaviour
    restart: always
    environment:
      - ORIONLD_TROE=TRUE
      - ORIONLD_TROE_USER=orion
      - ORIONLD_TROE_PWD=orion
      - ORIONLD_TROE_HOST=timescale
      - ORIONLD_MONGO_HOST=mongo-db
    depends_on:
      - mongo-db
      - timescale
    networks:
      - default
    ports:
      - "1026:1026"
    command: -logLevel DEBUG
    healthcheck:
      test: curl --fail -s http://orion:1026/version || exit 1
      interval: 30s
      retries: 15


  # Databases
  mongo-db:
    image: mongo:4.0
    hostname: mongo-db
    expose:
      - "27017"
    ports:
      - "27017:27017" # localhost:27017
    networks:
      - default
    command: --nojournal
    volumes:
      - mongo-db:/data
    healthcheck:
      test: |
        host=`hostname --ip-address || echo '127.0.0.1'`;
        mongo --quiet $host/test --eval 'quit(db.runCommand({ ping: 1 }).ok ? 0 : 2)' && echo 0 || echo 1
      interval: 30s

  timescale:
    image: timescale/timescaledb-postgis:latest-pg12
    hostname: timescale
    healthcheck:
      test: [ "CMD-SHELL", "pg_isready -U orion" ]
      interval: 15s
      timeout: 15s
      retries: 15
      start_period: 60s
    environment:
      - POSTGRES_USER=orion
      - POSTGRES_PASSWORD=orion
      - POSTGRES_HOST_AUTH_METHOD=trust
    expose:
      - "5432"
    ports:
      - "5432:5432"
    networks:
      - default

volumes:
  mongo-db: ~
```

## Configuration

Mintaka uses the [Micronaut-Framework](https://micronaut.io/). The following table lists the most important environment variables, please check 
the [framework documentation](https://docs.micronaut.io/2.1.3/guide/index.html) for all available options.

| Env-Var | Description | Default |
| ----------------------------------- | ----------------------------------------------- | ------------------------ |
| MICRONAUT_SERVER_PORT | Server port to be used for mintaka    | 8080  |
| MICRONAUT_METRICS_ENABLED | Enable the metrics gathering | true |
| ENDPOINTS_ALL_PORT | Port to provide the management endpoints | 8080 |
| ENDPOINTS_METRICS_ENABLED | Enable the metrics endpoint | true |
| ENDPOINTS_HEALTH_ENABLED | Enable the health endpoint | true | 
| DATASOURCES_DEFAULT_HOST | Host of timescale | localhost |
| DATASOURCES_DEFAULT_PORT | Port of timescale | 5432 |
| DATASOURCES_DEFAULT_DATABASE | Name of the default database, needs to coincide with orion-ld | orion |
| DATASOURCES_DEFAULT_USERNAME | Username to be used for db connections | orion | 
| DATASOURCES_DEFAULT_PASSWORD | Password to be used for db connections | orion | 
| LOGGERS_LEVELS_ROOT | Root log level of mintaka | ERROR |
 