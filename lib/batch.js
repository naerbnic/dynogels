'use strict';

const _ = require('lodash');
const async = require('async');

function buildInitialGetItemsRequest(tableName, keys, options) {
  const request = {};

  request[tableName] = _.merge({}, { Keys: keys }, options);

  return { RequestItems: request };
}

function serializeKeys(keys, table, serializer) {
  return keys.map(key => serializer.buildKey(key, null, table.schema));
}

function mergeResponses(tableName, responses) {
  const base = {
    Responses: {},
    ConsumedCapacity: []
  };

  base.Responses[tableName] = [];

  return responses.reduce((memo, resp) => {
    if (resp.Responses && resp.Responses[tableName]) {
      memo.Responses[tableName] = memo.Responses[tableName].concat(resp.Responses[tableName]);
    }

    return memo;
  }, base);
}

function paginatedRequest(request, table, callback) {
  const responses = [];

  const moreKeysToProcessFunc = () => request !== null && !_.isEmpty(request);

  const doFunc = callback => {
    table.runBatchGetItems(request, (err, resp) => {
      if (err && err.retryable) {
        return callback();
      } else if (err) {
        return callback(err);
      }

      request = resp.UnprocessedKeys;
      if (moreKeysToProcessFunc()) {
        request = { RequestItems: request };
      }
      responses.push(resp);

      return callback();
    });
  };

  const resulsFunc = err => {
    if (err) {
      return callback(err);
    }

    callback(null, mergeResponses(table.tableName(), responses));
  };

  async.doWhilst(doFunc, moreKeysToProcessFunc, resulsFunc);
}

function buckets(keys) {
  const buckets = [];

  while (keys.length) {
    buckets.push(keys.splice(0, 100));
  }

  return buckets;
}

function initialBatchGetItems(keys, table, serializer, options, callback) {
  const serializedKeys = serializeKeys(keys, table, serializer);

  const request = buildInitialGetItemsRequest(table.tableName(), serializedKeys, options);

  paginatedRequest(request, table, (err, data) => {
    if (err) {
      return callback(err);
    }

    const dynamoItems = data.Responses[table.tableName()];

    const items = _.map(dynamoItems, i => table.initItem(serializer.deserializeItem(i)));

    return callback(null, items);
  });
}

function getItems(table, serializer) {
  return (keys, options, callback) => {
    if (typeof options === 'function' && !callback) {
      callback = options;
      options = {};
    }

    async.map(buckets(_.clone(keys)), (key, callback) => {
      initialBatchGetItems(key, table, serializer, options, callback);
    }, (err, results) => {
      if (err) {
        return callback(err);
      }

      return callback(null, _.flatten(results));
    });
  };
}

function batch(table, serializer) {
  return {
    getItems: getItems(table, serializer)
  };
}

module.exports = batch;
