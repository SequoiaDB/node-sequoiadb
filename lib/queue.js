/*!
 *      Copyright (C) 2015 SequoiaDB Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

'use strict';

var Queue = function (conn) {
  this.conn = conn;
  this.items = [];
  this.state = "ready"; // pending
};

Queue.prototype.enqueue = function (item) {
  this.items.push(item);
  this.start();
};

Queue.prototype.start = function () {
  // send the item when ready
  if (this.state === "ready") {
    if (this.items.length > 0) {
      var head = this.items[0];
      this.send(head);
    }
  }
};

Queue.prototype.dequeue = function () {
  this.state = "ready";
  var item = this.items.shift();
  return item;
};

Queue.prototype.send = function (item) {
  this.state = "pending";
  this.conn.parser.state = item.state;
  this.conn.conn.write(item.buff);
};

Queue.prototype.fail = function (err) {
  var item;
  while ((item = this.items.shift())) {
    item.callback(err);
  }
};

module.exports = Queue;
