'use strict';

const _ = require('lodash');
const utils = require('./utils');
const serializer = require('./serializer');

const actionWords = ['SET', 'ADD', 'REMOVE', 'DELETE'];

const regexMap = _.reduce(actionWords, (result, key) => {
  result[key] = new RegExp(`${key}\\s*(.+?)\\s*(SET|ADD|REMOVE|DELETE|$)`);
  return result;
}, {});

// explanation http://stackoverflow.com/questions/3428618/regex-to-find-commas-that-arent-inside-and
const splitOperandsRegex = new RegExp(/\s*(?![^(]*\)),\s*/);

function match(actionWord, str) {
  const match = regexMap[actionWord].exec(str);

  if (match && match.length >= 2) {
    return match[1].split(splitOperandsRegex);
  } else {
    return null;
  }
}

function parse(str) {
  return _.reduce(actionWords, (result, actionWord) => {
    result[actionWord] = match(actionWord, str);
    return result;
  }, {});
}

function serializeUpdateExpression(schema, item) {
  const datatypes = schema._modelDatatypes;

  const data = utils.omitPrimaryKeys(schema, item);

  const memo = {
    expressions: {},
    attributeNames: {},
    values: {},
  };

  memo.expressions = _.reduce(actionWords, (result, key) => {
    result[key] = [];

    return result;
  }, {});

  const result = _.reduce(data, (result, value, key) => {
    const valueKey = `:${key}`;
    const nameKey = `#${key}`;

    if (_.isNull(value) || (_.isString(value) && _.isEmpty(value))) {
      result.expressions.REMOVE.push(nameKey);
      result.attributeNames[nameKey] = key;
    } else if (_.isPlainObject(value) && value.$add) {
      result.expressions.ADD.push(`${nameKey} ${valueKey}`);
      result.values[valueKey] = serializer.serializeAttribute(value.$add, datatypes[key]);
      result.attributeNames[nameKey] = key;
    } else if (_.isPlainObject(value) && value.$del) {
      result.expressions.DELETE.push(`${nameKey} ${valueKey}`);
      result.values[valueKey] = serializer.serializeAttribute(value.$del, datatypes[key]);
      result.attributeNames[nameKey] = key;
    } else {
      result.expressions.SET.push(`${nameKey} = ${valueKey}`);
      result.values[valueKey] = serializer.serializeAttribute(value, datatypes[key]);
      result.attributeNames[nameKey] = key;
    }

    return result;
  }, memo);

  return result;
}

function stringify(expressions) {
  return _.reduce(expressions, (result, value, key) => {
    if (!_.isEmpty(value)) {
      if (_.isArray(value)) {
        result.push(`${key} ${value.join(', ')}`);
      } else {
        result.push(`${key} ${value}`);
      }
    }

    return result;
  }, []).join(' ');
}

function formatAttributeValue(val) {
  if (_.isDate(val)) {
    return val.toISOString();
  }

  return val;
}

function isFunctionOperator(operator) {
  return _.includes(['attribute_exists',
                   'attribute_not_exists',
                   'attribute_type',
                   'begins_with',
                   'contains',
                   'NOT contains',
                   'size'], operator);
}

function uniqAttributeValueName(key, existingValueNames) {
  const cleanedKey = key.replace(/\./g, '_').replace(/\W/g, '');
  let potentialName = `:${cleanedKey}`;
  let idx = 1;

  while (_.includes(existingValueNames, potentialName)) {
    idx++;
    potentialName = `:${cleanedKey}_${idx}`;
  }

  return potentialName;
}

function buildInFilterExpression(key, existingValueNames, values) {
  const path = `#${key}`;

  const attributeNames = {};
  attributeNames[path.split('.')[0]] = key.split('.')[0];

  const attributeValues = _.reduce(values, (result, val) => {
    const existing = _.keys(result).concat(existingValueNames);
    const p = uniqAttributeValueName(key, existing);
    result[p] = formatAttributeValue(val);
    return result;
  }, {});

  return {
    attributeNames: attributeNames,
    statement: `${path} IN (${_.keys(attributeValues)})`,
    attributeValues: attributeValues
  };
}

function buildFilterExpression(key, operator, existingValueNames, val1, val2) {
  // IN filter expression is unlike all the others where val1 is an array of values
  if (operator === 'IN') {
    return buildInFilterExpression(key, existingValueNames, val1);
  }

  let v1 = formatAttributeValue(val1);
  const v2 = formatAttributeValue(val2);

  if (operator === 'attribute_exists' && v1 === false) {
    operator = 'attribute_not_exists';
    v1 = null;
  } else if (operator === 'attribute_exists' && v1 === true) {
    v1 = null;
  }

  const keys = key.split('.');
  const path = `#${keys.join('.#').replace(/[^\w\.#]/g, '')}`;
  const v1ValueName = uniqAttributeValueName(key, existingValueNames);
  const v2ValueName = uniqAttributeValueName(key, [v1ValueName].concat(existingValueNames));

  let statement = '';

  if (isFunctionOperator(operator)) {
    if (!_.isNull(v1) && !_.isUndefined(v1)) {
      statement = `${operator}(${path}, ${v1ValueName})`;
    } else {
      statement = `${operator}(${path})`;
    }
  } else if (operator === 'BETWEEN') {
    statement = `${path} BETWEEN ${v1ValueName} AND ${v2ValueName}`;
  } else {
    statement = [path, operator, v1ValueName].join(' ');
  }

  const attributeValues = {};

  if (!_.isNull(v1) && !_.isUndefined(v1)) {
    attributeValues[v1ValueName] = v1;
  }

  if (!_.isNull(v2) && !_.isUndefined(v2)) {
    attributeValues[v2ValueName] = v2;
  }

  const attributeNames = {};
  keys.forEach((key) => {
    attributeNames[`#${key.replace(/[^\w\.]/g, '')}`] = key;
  });

  return {
    attributeNames: attributeNames,
    statement: statement,
    attributeValues: attributeValues
  };
}

module.exports = {
  parse: parse,
  serializeUpdateExpression: serializeUpdateExpression,
  stringify: stringify,
  buildFilterExpression: buildFilterExpression,
};
