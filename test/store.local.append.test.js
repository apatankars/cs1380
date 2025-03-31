// test/store.local.append.test.js
const distribution = require('../config.js');
const local = distribution.local;
const id = distribution.util.id;

test('(1 pts) local.store.append error on null state', (done) => {
  const key = 'testAppendKey';
  
  distribution.local.store.append(null, key, (e, v) => {
    try {
      expect(e).toBeInstanceOf(Error);
      expect(v).toBeFalsy();
      done();
    } catch (error) {
      done(error);
    }
  });
});

test('(1 pts) local.store.append basic functionality', (done) => {
  const data = {count: 5};
  const key = 'basicAppend';
  
  distribution.local.store.append(data, key, (e, v) => {
    try {
      expect(e).toBeFalsy();
      expect(v).toHaveProperty('count');
      expect(v.count).toEqual([5]);
      done();
    } catch (error) {
      done(error);
    }
  });
});

test('(2 pts) local.store.append multiple times aggregates values', (done) => {
  const data1 = {score: 10};
  const data2 = {score: 20};
  const data3 = {scape: 10};
  const key = 'aggregateAppend';
  
  distribution.local.store.append(data1, key, (e, v) => {
    distribution.local.store.append(data2, key, (e, v) => {
      distribution.local.store.append(data3, key, (e, v) => {
        try {
          expect(e).toBeFalsy();
          expect(v).toHaveProperty('score');
          expect(v.score).toEqual([10, 20]);
          done();
        } catch (error) {
          done(error);
        }
      })
    });
  });
});

test('(1 pts) local.store.append with multiple keys', (done) => {
  const data = {points: 5, user: "bob"};
  const key = 'multiKeyAppend';
  
  distribution.local.store.append(data, key, (e, v) => {
    try {
      expect(e).toBeFalsy();
      expect(v).toHaveProperty('points');
      expect(v).toHaveProperty('user');
      expect(v.points).toEqual([5]);
      expect(v.user).toEqual(["bob"]);
      done();
    } catch (error) {
      done(error);
    }
  });
});

test('(1 pts) local.store.append/get', (done) => {
  const data = {clicks: 15};
  const key = 'appendGet';
  
  distribution.local.store.append(data, key, (e, v) => {
    distribution.local.store.get(key, (e, v) => {
      try {
        expect(e).toBeFalsy();
        expect(v).toHaveProperty('clicks');
        expect(v.clicks).toEqual([15]);
        done();
      } catch (error) {
        done(error);
      }
    });
  });
});

test('(2 pts) local.store.append with null key uses hash', (done) => {
  const data = {name: "test"};
  
  distribution.local.store.append(data, null, (e, v) => {
    distribution.local.store.get(id.getID(data), (e, v) => {
      try {
        expect(e).toBeFalsy();
        expect(v).toHaveProperty('name');
        expect(v.name).toEqual(["test"]);
        done();
      } catch (error) {
        done(error);
      }
    });
  });
});