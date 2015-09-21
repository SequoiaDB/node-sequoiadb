Node Sequoiadb
==============
Node.js Driver for SequoiaDB

## Installation

```sh
$ npm install sequoiadb --save
```

## API

### Connection
Create a connection to sequoiadb server:

```js
var Connection = require('sequoiadb');
var conn = new Connection(11810, "ip", {
  user: "",
  pass: ""
});
```

Disconnect with sequoiadb server:

```js
conn.disconnect([callback]);
```

Wait for connection ready:

```js
conn.ready(function () {
  // TODO
});
```

All operation must be after db ready.

### User
Create a user:

```js
conn.createUser('user', 'pass', function (err) {
  // TODO
});
```

Remove a user:

```js
conn.removeUser('user', 'pass', function (err) {
  // TODO
});
```

### CollectionSpace
Create CollectionSpace in sequoiadb:

```js
conn.createCollectionSpace("space_name", function (err, space) {
  // TODO
});
```
Get CollectionSpace in sequoiadb by name:

```js
conn.getCollectionSpace("space_name", function (err, space) {
  // TODO
});
```

Check given space name whether exist:

```
conn.isCollectionSpaceExist("space_name", function (err, exist) {
  // TODO
});
```

Drop CollectionSpace:

```js
conn.dropCollectionSpace("space_name", function (err) {
  // TODO
});
```

Get all CollectionSpaces:

```js
conn.getCollectionSpaces(function (err, cursor) {
  // TODO
});
```

### Cursor
Get current item:

```js
cursor.current(function (err, item) {
  // TODO
});
```

Get next item:

```js
cursor.next(function (err, item) {
  // TODO
});
```

Close cursor:

```js
cursor.close(function (err) {
  // TODO
});
```

### Collection

Create a Collection in CollectionSpace:

```js
space.createCollection('collection_name', function (err, collection) {
  // TODO
});
```

Get a Collection from CollectionSpace by given name:

```js
space.getCollection('collection_name', function (err, collection) {
  // TODO
});
```

Check a Collection whether exist:

```js
space.isCollectionExist('collection_name', function (err, exist) {
  // TODO
});
```

Drop a Collection from a CollectionSpace:

```js
space.dropCollection('collection_name', function (err) {
  // TODO
});
```

### Document
Insert a document into Collection:

```js
collection.insert({"name":"sequoiadb"}, function (err) {
  // TODO
});
```

Upsert a document into Collection:

```js
collection.upsert({name: "sequoiadb"}, {'$set': {age: 26}}, {}, function (err) {
  // TODO
});
```

Bulk insert documents into Collection:

```js
var insertors = [
  {name: "hi"},
  {name: "jack"}
];
collection.bulkInsert(insertors, 0, function (err) {
  // TODO
});
```

Query all document of Collection:

```js
collection.query(function (err, cursor) {
  // TODO
});
```

### Index

Create Index for collection:

```js
var key = {
  "Last Name": 1,
  "First Name": 1
};
collection.createIndex("index_name", key, false, false, function (err) {
  // TODO
});
```

Get index with given name:

```js
collection.getIndex("index_name", function (err, cursor) {
  // TODO
});
```

Get all indexes:

```js
collection.getIndex(function (err, cursor) {
  // TODO
});
```

Or:

```js
collection.getIndexes(function (err, cursor) {
  // TODO
});
```

Drop index:

```js
collection.dropIndex('index_name', function (err) {
  // TODO
});
```

## License
[The Apache License 2.0](https://github.com/SequoiaDB/node-sequoiadb/blob/master/LICENSE)
