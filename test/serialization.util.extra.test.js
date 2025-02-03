const distribution = require('../config.js');
const util = distribution.util;

const fs = require('fs');

beforeAll(() => {
  const original = {a: 1, b: 2, c: 3};
  const serialized = util.serialize(original);
  const deserialized = util.deserialize(serialized);
  expect(deserialized).not.toBe(original);
});

test('(5 pts) serializeCircularObject', () => {
  const object = {a: 1, b: 2, c: 3};
  object.self = object;
  const serialized = util.serialize(object);
  const deserialized = util.deserialize(serialized);
  expect(deserialized).toEqual(object);
});

test('(5 pts) serializeNativeFunction', () => {
  const fn = fs.readFile;
  const serialized = util.serialize(fn);
  const deserialized = util.deserialize(serialized);
  // Native function serialization might not work as expected
  expect(deserialized).toBe(fs.readFile);
});

test('(5 pts) serializeAnotherNativeFunction', () => {
  const fn = require('console').log;
  const serialized = util.serialize(fn);
  const deserialized = util.deserialize(serialized);
  // Native function serialization might not work as expected
  expect(deserialized).toBe(fn);
});

test('(5 pts) serializeObjectWithNativeFunctions', () => {
  const object = {a: fs.readFile};
  const serialized = util.serialize(object);
  const deserialized = util.deserialize(serialized);
  // Native function serialization might not work as expected
  expect(deserialized.a).toBe(fs.readFile);
});

test('(5 pts) serializeRainbowObjectCirc', () => {
  const object = {
    n: 1,
    s: 'Hello, World!',
    a: [1, 2, 3, 4, 5],
    e: new Error('Hello, World!'),
    d: new Date(),
    o: {x: 1, y: 2, z: 3},
    n: null,
    u: undefined,
  };

  object.self = object;

  const serialized = util.serialize(object);
  const deserialized = util.deserialize(serialized);

  expect(deserialized).toEqual(object);
});

test('(5 pts) serialize and deserialize structure with cycle-like reference', () => {
  const x = { a: 1, b: 2, c: 3};
  const original = { a: x, b: x};
  const serialized = util.serialize(original);
  const deserialized = util.deserialize(serialized);

  expect(deserialized).toEqual(original);
});

test('(5 pts) serialize and deserialize cyclic structure with function', () => {
  const f = function f() {};
  const original = [f, f];
  const serialized = util.serialize(original);
  const deserialized = util.deserialize(serialized);
  expect(Array.isArray(deserialized)).toEqual(true);
  expect(typeof deserialized[0] === 'function').toEqual(true);
  expect(deserialized[0].name).toBe('f');
});

test('(5 pts) serialize and deserialize object with function', () => {
  const f = function f() {};
  const original = {a: f, b: f};
  let serialized = util.serialize(original);
  serialized = util.deserialize(serialized);
  expect(typeof serialized === 'object').toEqual(true);
  expect(typeof serialized.a === 'function').toEqual(true);
  expect(serialized.a.name).toBe('f');
});

test('(5 pts) serialize and deserialize complex cyclic structure', () => {
  const f = function f() {};
  let original = {a: f, b: f};
  original = [original, f, [original, f]];
  let serialized = util.serialize(original);
  serialized = util.deserialize(serialized);
  expect(Array.isArray(serialized)).toEqual(true);
  expect(typeof serialized[0] === 'object').toEqual(true);
  expect(typeof serialized[1] === 'function').toEqual(true);
  expect(serialized[1].name).toBe('f');
  expect(Array.isArray(serialized[2])).toBe(true);
});
