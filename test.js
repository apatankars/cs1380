
let util = require("@brown-ds/distribution").util;

function serializeString(type, object) {
  const value = object.replace(/"/g, '\\"').replace(/\n/g, '\\n');
  return `{"type":"${type}","value":"${value}"}`;
}

function parseBoolean(value) {
  return value === 'true';
}

function serialize(object) {

  // First, we will being with the basic types
  if (typeof object === 'string') {
    return serializeString('string', object);
  }
  if (typeof object === 'number') {
    return serializeString('number', object.toString());
  }
  if (typeof object === 'boolean') {
    return serializeString('boolean', object.toString());
  }
  if (object === null) {
    return serializeString('null', '');
  }
  if (object === undefined) {
    return serializeString('undefined', '');
  }
  if (typeof object === 'function') {
    return serializeString('function', object.toString());
  }
  if (typeof object === 'object') {
    let serialized = {type: null, value: {}};
    if (Array.isArray(object)) {
      serialized.type = "array";
      for (let i = 0; i < object.length; i++) {
        serialized.value[i] = serialize(object[i]);
      }
      return JSON.stringify(serialized);
    } else if (object instanceof Date) {
      serialized.type = "date";
      serialized.value = object.toISOString();
      return JSON.stringify(serialized);
    } else if (object instanceof Error) {
      serialized.type = "error";
      serialized.value = {
        type: "object",
        value: {
          name: serializeString('string', object.name),
          message: serializeString('string', object.message),
          cause: object.cause? serializeString('string', object.cause) : serializeString('undefined', ''),
        }
      };
      return JSON.stringify(serialized);
    } else {
      serialized.type = "object";
      for (let key in object) {
        serialized.value[key] = serialize(object[key]);
      }
      return JSON.stringify(serialized);
    }
  }
  throw new Error(`Unknown type: ${typeof object}`);
}


function deserialize(string) {
  const json = JSON.parse(string);
  if (json.type === 'string') {
    return json.value;
  }
  if (json.type === 'number') {
    return parseFloat(json.value);
  }
  if (json.type === 'boolean') {
    return parseBoolean(json.value);
  }
  if (json.type === 'null') {
    return null;
  }
  if (json.type === 'undefined') {
    return undefined;
  }
  if (json.type === 'function') {
    return eval('(' + json.value + ')');
  }
  if (json.type === 'date') {
    return new Date(json.value);
  }
  if (json.type === 'error') {
    console.log(deserialize(json.value.value.message));
    return new Error(deserialize(json.value.value.message));
  }
  if (json.type === 'array') {
    let array = [];
    for (let val in json.value) {
      array.push(deserialize(json.value[val]));
    }
    return array;
  }
  if (json.type === 'object') {
    let object = {};
    for (let key in json.value) {
      object[key] = deserialize(json.value[key]);
    }
    return object;
  }
  throw new Error(`Unknown type: ${json.type}`);
}

const original = '\\string\n\t\r"';

const serialized = util.serialize(-0);
// const my_serialized = serialize(original);
const deserialized = util.deserialize(serialized);
// console.log(deserialized.func(42, 1));

console.log("Serialization")
console.log("--------------------------------")
// console.log("My Serialized: ", my_serialized);
console.log("Util Serialized: ", serialized);
// console.log("Equality Check: ", my_serialized === serialized);
console.log("Deserialization")
console.log("--------------------------------")
// console.log("Deserialized: ", deserialize(my_serialized).func.toString());
console.log("Util Deserialized: ", deserialized);
// console.log("Equality Check: ", deserialize(my_serialized) === deserialized);




// console.log(deserialize(serialized));
