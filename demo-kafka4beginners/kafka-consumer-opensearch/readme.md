GET /
# create index
PUT /my-first-index
# create dokument
PUT /my-first-index/_doc/1
{"some data"}
# get data
GET  /my-first-index/_doc/1
# delete dokumet
DELETE  /my-first-index/_doc/1
# delete index
DELETE  /my-first-index