/*
    In this file, add your own test cases that correspond to functionality introduced for each milestone.
    You should fill out each test case so it adequately tests the functionality you implemented.
    You are left to decide what the complexity of each test case should be, but trivial test cases that abuse this flexibility might be subject to deductions.

    Imporant: Do not modify any of the test headers (i.e., the test('header', ...) part). Doing so will result in grading penalties.
*/

const distribution = require('../../config.js');
const util = distribution.util;

test('(1 pts) student test', () => {
  // Test serializing and deserializing a nest function
  const fn = (a, b) => {
    const nested = (c, d) => {
      return a + b + c + d;
    }
    return nested;
  }
  const object = {func: fn};
  const serialized = util.serialize(object);
  const deserialized = util.deserialize(serialized);
  expect(deserialized.func(1, 2)(3, 4)).toBe(10);
});


test('(1 pts) student test', () => {
  // Test serializing and deserializing an object will null values
  const object = {a: null, b: null};
  const serialized = util.serialize(object);
  const deserialized = util.deserialize(serialized);
  expect(deserialized.a).toBe(null);
  expect(deserialized.b).toBe(null);
});


test('(1 pts) student test', () => {
  // Test serializing and deserializing a large object with large integers
  const object = {a: 12345678901234567890, b: 12345678901234567890};
  const serialized = util.serialize(object);
  const deserialized = util.deserialize(serialized);
  expect(deserialized.a).toBe(12345678901234567890);
  expect(deserialized.b).toBe(12345678901234567890);
});

test('(1 pts) student test', () => {
  let object = { a: { b: { c: { d: { e: { f: { g: "deep" } } } } } } };

  const serialized = util.serialize(object);
  const deserialized = util.deserialize(serialized);

  expect(deserialized).toEqual(object);
});

test('(1 pts) student test', () => {
  let object = { a: Infinity, b: -Infinity, c: NaN, d: 0 };

  const serialized = util.serialize(object);
  const deserialized = util.deserialize(serialized);

  expect(deserialized.a).toBe(Infinity);
  expect(deserialized.b).toBe(-Infinity);
  expect(Number.isNaN(deserialized.c)).toBe(true);
  expect(Object.is(deserialized.d, 0)).toBe(true);
});


