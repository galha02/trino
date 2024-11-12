# Clients

A [client](trino-concept-client) is used to send SQL queries to Trino, and
therefore any [connected data sources](trino-concept-data-source), and receive
results.

## Client drivers

Client drivers, also called client libraries, provide a mechanism for other
applications, including your own custom applications, to connect to Trino. The
Trino project maintains the following client drivers:

* [Trino JDBC driver](/client/jdbc)
* [trino-go-client](https://github.com/trinodb/trino-go-client)
* [trino-js-client](https://github.com/trinodb/trino-js-client)
* [trino-python-client](https://github.com/trinodb/trino-python-client)

Other communities and vendors provide [a number of other client
drivers](https://trino.io/ecosystem/client).

## Client applications

Client applications typically use a client driver and can provide a user
interface and other user-facing features to run queries with Trino. You can
inspect the results, perform analytics with further queries, and create
visualizations.

The Trino project maintains the [Trino command line interface](/client/cli) as a
client application.

Other communities and vendors provide [numerous other client
applications](https://trino.io/ecosystem/client)

## Client protocol

All client drivers and client applications communicate with the Trino
coordinator using the [client protocol](/client/client-protocol).

Configure support for the [spooling protocol](protocol-spooling) on the cluster
to improve performance for client interactions with higher data transfer
demands.


```{toctree}
:maxdepth: 1
:hidden:

client/client-protocol
client/cli
client/jdbc
```
