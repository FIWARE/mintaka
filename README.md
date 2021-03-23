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

This project is part of [FIWARE](https://www.fiware.org/). For more information check the FIWARE Catalogue entry for
[Core Context](https://github.com/Fiware/catalogue/tree/master/core). 

| :whale: [Docker Hub](https://hub.docker.com/r/fiware/mintaka/) | :books: [Java Doc](https://fiware.github.io/mintaka/) | 
| ----------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |

## Table of contents
* [Build & Install](#build-and-install)
    * [Build](#build)
        * [Jar](#jar)
        * [Docker](#docker)
    * [Install](#install)
        * [Preconditions](#preconditions)
        * [How-to-run](#how-to-run)
        * [Configuration](#configuration)
        * [Operations](#operations)
* [Testing](#testing)
    * [Coverage](#coverage)
    * [Static analyzes](#static-analyzes)
* [Documentation](#documentation)
* [API-Remarks](#api-remarks)
    * [Pagination](#pagination)


## Build and Install

### Build

The project is build using [maven](https://maven.apache.org/). 

#### Jar

In order to create a working jar file, run: ```mvn clean install```

#### Docker

For building a new [docker container](https://www.docker.com/), the usage of the 
[jib-plugin](https://github.com/GoogleContainerTools/jib/tree/master/jib-maven-plugin) is recommended.
Build the image locally via:
```mvn clean install jib:dockerBuild```

If for some reasons required, a plain [dockerfile](docker/Dockerfile) can be used to build:
```docker build -t my-custom-image -f docker/Dockerfile .```
and run:
```docker run my-custom-image```

### Install

#### Preconditions

Mintaka should be viewed as a component of [Orion-LD](https://github.com/FIWARE/context.Orion-LD) and therefore has no 
mechanism for populating the database itself. It relies on a  [TimescaleDB](https://www.timescale.com/) installation(including the 
[Postgis](https://postgis.net/) extension), that is populated by Orion-LD. 
For installing Orion-LD, see the [Installation-guide](https://github.com/FIWARE/context.Orion-LD/blob/develop/doc/manuals-ld/installation-guide.md),
for Timescale we recommend the [Timescale-Postgis image](https://hub.docker.com/r/timescale/timescaledb-postgis/) for [Postgres 12](https://hub.docker.com/layers/timescale/timescaledb-postgis/latest-pg12/images/sha256-40be823de6035faa44d3e811f04f3f064868ee779ebb49b287e1c809ec786994?context=explore).
Working [docker-compose](https://docs.docker.com/compose/) setups can be found at the [test-folder](src/test/resources/docker-compose).

#### How-to-run

Start mintaka via ```docker run  fiware/mintaka```.

#### Configuration

We recommend to run mintaka with the provided [docker container](https://hub.docker.com/r/fiware/mintaka/).   
Since mintaka is built using the [Micronaut-Framework](https://micronaut.io/) all configurations can be provided either via configuration 
file ([application.yaml](src/main/resources/application.yml)) or as environment variables. For detailed information about the configuration mechanism,
see the [framework documentation](https://docs.micronaut.io/2.1.3/guide/index.html#configurationProperties).

The following table concentrates on the most important configuration parameters for Mintaka:

|  Property | Env-Var | Description | Default |
| ----------------- | ----------------------------------- | ----------------------------------------------- | ------------------------ |
| `micronaut.server.port`        | `MICRONAUT_SERVER_PORT` | Server port to be used for mintaka    | 8080  |
| `micronaut.metrics.enabled` | `MICRONAUT_METRICS_ENABLED` | Enable the metrics gathering | true |
| `endpoints.all.port` | `ENDPOINTS_ALL_PORT` | Port to provide the management endpoints | 8080 |
| `endpoints.metrics.enabled` | `ENDPOINTS_METRICS_ENABLED` | Enable the metrics endpoint | true |
| `endpoints.health.enabled` | `ENDPOINTS_HEALTH_ENABLED` | Enable the health endpoint | true | 
| `datasources.default.host` | `DATASOURCES_DEFAULT_HOST` | Host of timescale | localhost |
| `datasources.default.port` | `DATASOURCES_DEFAULT_PORT` | Port of timescale | 5432 |
| `datasources.default.database` | `DATASOURCES_DEFAULT_DATABASE` | Name of the default database, needs to coincide with orion-ld | orion |
| `datasources.default.username` | `DATASOURCES_DEFAULT_USERNAME` | Username to be used for db connections | orion | 
| `datasources.default.password` | `DATASOURCES_DEFAULT_PASSWORD` | Password to be used for db connections | orion | 
| `loggers.levels.ROOT` | `LOGGERS_LEVELS_ROOT` | Root log level of mintaka | ERROR |

#### Operations 

Mintaka provides the [micronaut management api](https://docs.micronaut.io/latest/guide/index.html#management). If not configured differently,
the health endpoint will be available at ```https://<MINTAKA_HOST>:9090/health``` and metrics at ```https://<MINTAKA_HOST>:9090/metrics```.
For all available options, please check the [framework documentation](https://docs.micronaut.io/latest/guide/index.html#management).

## Testing
 
Since Mintaka relies on the database to be populated by [Orion-LD](https://github.com/FIWARE/context.Orion-LD), the testsuite is built around a 
[docker-compose setup](src/test/resources/docker-compose/docker-compose-it.yml) including Orion-LD. The tests use 
[testcontainers](https://www.testcontainers.org/) and [JUnit-5](https://junit.org/junit5/). 

Test can be executed via: ```mvn clean test```

### Coverage

Code-coverage reports are automatically created by [Jacoco](https://www.eclemma.org/jacoco/) when the test are executed by maven. Public 
reports are available at [Coveralls.io](https://coveralls.io/github/FIWARE/mintaka).

### Static analyzes

Static code analyzes("linting") are provided via [Spotbugs](https://spotbugs.github.io/). 
Reports can be created via: ```mvn -B verify spotbugs:spotbugs -DskipTests```

## Documentation

The code is documented in the [Javadoc comments format](https://docs.oracle.com/javase/1.5.0/docs/tooldocs/solaris/javadoc.html) and 
automatically published from the main branch to: https://fiware.github.io/mintaka/ 

## API-Remarks

The API complies with the [NGSI-LD spec version 1.3.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.03.01_60/gs_cim009v010301p.pdf).

### OpenAPI

Mintaka uses a modified version of the [NGSI-LD OpenAPI-Spec](https://forge.etsi.org/rep/NGSI-LD/NGSI-LD/blob/master/spec/updated/generated/full_api.json) 
that can be found in the [api folder](api/full_api.json).
Models and Interfaces are generated by the [Micronaut-OpenAPI-Codegenerator](https://github.com/kokuwaio/micronaut-openapi-codegen). Only the 
tag "Temporal Retrieval" is implemented since all other tags are in scope of [Orion-LD](https://github.com/FIWARE/context.Orion-LD). It's necessary 
to use a modified spec here, since the underlying Java-Codegenerator does not properly support the [OpenAPI "oneOf"](https://swagger.io/docs/specification/data-models/oneof-anyof-allof-not/#oneof) 
structure, which is heavily used in the api.

### Pagination

The api supports two dimensions of pagination. 

The first one is similar to the normal query api and compliant with the 
[NGSI-LD](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.04.01_60/gs_cim009v010401p.pdf) spec (see 5.5.9):
* retrieval of entities is automatically limited to a default pageSize (100) 
* the id of the next-page anchor will be returned via the header "Next-Page"
* the id of the previous-page anchor will be returned via the header "Previous-Page"
* the page-size will be returned via the header "Page-Size"
* "Next-Page" will not be returned for the last page
* "Previous-Page" will not be returned for the first page
* the parameters "pageAnchor" and "pageSize" can be used for requesting pages

The second one limits the retrieval of temporal instances and will be described in section 6.3.10 of future NGSI-LD api releases. It automatically 
limits the number of returned instances and responds with Http-Status 206 "PARTIAL-CONTENT". The returned range is described in the "Content-Range" header.

## Contribution

Please check the [contribution documentation](/doc/CONTRIBUTION.md) for more information
