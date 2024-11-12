# Client protocol

The Trino client protocol is a HTTP-based protocol that allows
[clients](/client) to submit SQL queries and receive results.

The protocol is a sequence of REST API calls to the
[coordinator](trino-concept-coordinator) of the Trino
[cluster](trino-concept-cluster). Following is a high-level overview:

1. Client submits SQL query text to the coordinator of the Trino cluster.
2. The coordinator starts processing the query.
3. The coordinator returns a result set and a URI `nextUri` on the coordinator.
4. The client receives the result set and initiates another request for more
   data from the URI `nextUri`.
5. The coordinator continues processing the query and returns further data with
   a new URI.
6. The client and coordinator continue with steps 4. and 5. until all
   result set data is returned to the client or the clients stops requesting
   more data.

The client protocol supports two modes. Configure the [spooling
protocol](protocol-spooling) for optimal performance for your clients.

Detail spooling vs direct protocol here


JSON compressed and other stuff

pruning done by coordinator, only for stuff that wasnt already cleaned up by client

uses encryption on object storage

client reads segments and deletes them

client gets all detials to ready data (url, credentials, and keys for decruption)


(protocol-spooling)=
## Spooling protocol

The spooling protocol has the following characteristics, compared to the [direct
protocol](protocol-direct).

* Provides higher performance for data transfer, specifically for queries that
  return more data.
* Results in faster query processing completion on the cluster, independent of
  the client retrieving all data, since data is read from the object storage.
* Requires object storage and configuration on the Trino cluster.
* Reduces CPU load on the coordinator.
* Automatically falls back to the direct protocol for queries that don't benefit
  from using the spooling protocol.
* Requires newer client drivers or client applications that support the spooling
  protocol.

### Configuration


that is available for access by all Trino cluster
  nodes and all client applications.

fs.location and how it interacts with config of fs from object storage


[spooling protocol on the
cluster](prop-protocol-spooling)


assumption that storage is unbounded, if storage fills up it will fail

need good connectiivty from cluster to object storage, same reagion, same availability zone, 

spooling separate for each cluster

same as objects storage connector, or also FTE exchange 

The following client drivers and client applications support the spooling protocol.

* [Trino JDBC driver](jdbc-spooling-protocol), do we need to add version info here?
* [Trino command line interface](cli-spooling-protocol), do we need to add version info here?

Refer to the documentation for other your specific client drivers and client
applications for up to date information.

(protocol-direct)=
## Direct protocol

The direct protocol, also know as the `v1` protocol, has the following
characteristics, compared to the spooling protocol:

* Provides lower performance, specifically for queries that return more data.
* Results in slower query processing completion on the cluster, since data is
  provided by the coordinator and read by the client sequentially.
* Requires **no** object storage or configuration in the Trino cluster.
* Increases CPU load on the coordinator.
* Works with older client drivers and client applications without support for
  the spooling protocol.

## Development and reference information

Further technical details about the client protocol, including information
useful for developing a client driver, are available in the [Trino client REST
API developer reference](/develop/client-protocol).


maybe rename  `protocol.v1.prepared-statement-compression.length-threshold`
