# <a name="top"></a>Mintaka-J

[![License badge](https://img.shields.io/github/license/FIWARE/context.Orion-LD.svg)](https://opensource.org/licenses/AGPL-3.0)
[![Docker badge](https://img.shields.io/docker/pulls/wistefan/mintaka-j.svg)](https://hub.docker.com/r/wistefan/mintaka-j/)
[![NGSI-LD badge](https://img.shields.io/badge/NGSI-LD-red.svg)](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.04.01_60/gs_cim009v010401p.pdf)
<br>

Mintaka is an implementation of the [NGSI-LD](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.04.01_60/gs_cim009v010401p.pdf) temporal retrieval
api. It relies on the [Orion-LD Context Broker](https://github.com/FIWARE/helm-charts/tree/main/charts/orion) to provide the underlying database.
The NGSI-LD specification is a living, changing document, and the latest Orion-LD beta release is nearly feature complete to the
1.3.1 ETSI specification. 

This project is part of [FIWARE](https://www.fiware.org/). For more information check the FIWARE Catalogue entry for
[Core Context](https://github.com/Fiware/catalogue/tree/master/core).

| :whale: [Docker Hub](https://hub.docker.com/r/wistefan/mintaka-j/) |


## API-Remarks

### Pagination

The api supports two dimensions of pagination. 
The first one is similar to the normal query api and compliant with the 
[NGSI-LD](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.04.01_60/gs_cim009v010401p.pdf) spec(see 5.5.9):
* retrieval of entities will be automatically limited to a default pageSize(100) 
* the id of the next-page anchor will be returned via the header "Next-Page"
* the id of the previous-page anchor will be returned via the header "Previous-Page"
* the page-size will be returned via the header "Page-Size"
* "Next-Page" will not be returned for the last page
* "Previous-Page" will not be returned for the first page
* the parameters "pageAnchor" and "pageSize" can be used for requesting pages

The second one limits the retrieval of temporal instances and will be described in section 6.3.10 of future NGSI-LD api releases. It automatically 
limits the number of returned instances and responds with Http-Status 206 "PARTIAL-CONTENT". The returned range is described in the "Content-Range" header.