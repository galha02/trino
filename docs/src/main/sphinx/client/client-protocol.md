# Client protocol


two modes

spool and direct (v1)


Table with steps


client protocol page in clients section

new one is default

small add in dev guide

query finished faster

json can be compressed (20% ratio) 5x


fs.location and how it interacts with config of fs from object storage


client gets all detials to ready data (url, credentials, and keys for decruption)

uses encryption on object storage

client reads segments and deletes them

pruning done by coordinator, only for stuff that wasnt already cleaned up by client

spooling separate for each cluster

need good connectiivty from cluster to object storage, same reagion, same availability zone, 

same as objects storage connector, or also FTE exchange 

assumption that storage is unbounded, if storage fills up it will fail



\
remove protocol.v1.alternate-header-name`

maybe rename  `protocol.v1.prepared-statement-compression.length-threshold`


have list of clients that support spooling