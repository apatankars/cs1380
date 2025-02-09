/*
    In this file, add your own test cases that correspond to functionality introduced for each milestone.
    You should fill out each test case so it adequately tests the functionality you implemented.
    You are left to decide what the complexity of each test case should be, but trivial test cases that abuse this flexibility might be subject to deductions.

    Imporant: Do not modify any of the test headers (i.e., the test('header', ...) part). Doing so will result in grading penalties.
*/

const config = {ip: '127.0.0.1', port: 1234};
const distribution = require('../../config.js')(config);
const local = distribution.local;
const routes = distribution.local.routes;
const id = distribution.util.id;

test.only('(1 pts) student test', (done) => {
  let results = [];
  const node = distribution.node.config;
  const remote = {node: node, service: 'status', method: 'get'};
  const message = ['invalid'];

  local.status.get(message, (e, v) => {
    try {
      expect(e).toBeTruthy();
      expect(v).toBeFalsy();
      results.push(true);
    } catch (error) {
      done(error);
    }
  });

  local.comm.send(message, remote, (e, v) => {
    try {
      expect(e).toBeTruthy();
      expect(e).toBeInstanceOf(Error);
      expect(v).toBeFalsy();
      results.push(true);
    } catch (error) {
      done(error);
    }
  });

  const message2 = ['nid', 'sid', 'counts', 'ip', 'port', 'heapTotal', 'heapUsed'];
  local.comm.send(message2, remote, (e, v) => {
    try {
      expect(e).toBeTruthy();
      expect(e).toBeInstanceOf(Error);
      expect(v).toBeFalsy();
      results.push(true);
    } catch (error) {
      done(error);
    }
  });

  setTimeout(() => {
    expect(results.length).toBe(3);
    done();
  }, 1000);
});


test.only('(1 pts) student test', (done) => {
  let results = [];
  const node = distribution.node.config;
  const remote = {node: node, service: 'routes', method: 'put'};
  const message = ['invalid'];

  local.routes.put('invalid', null, (e, v) => {
    try {
      expect(e).toBeTruthy();
      expect(v).toBeFalsy();
      results.push(true);
    } catch (error) {
      done(error);
    }
  })

  const tempService = { foo: () => {} };
  local.routes.put(tempService, "temp", (err, name) => {
    if (err) return done(err);

    local.routes.rem("temp", (remErr) => {
      if (remErr) return done(remErr);

      // Now try to get it right away
      local.routes.get("temp", (getErr, service) => {
        if (!getErr) {
          return done(new Error("Expected error after removing service, but got none" + service));
        }
        if (service !== undefined && service !== null) {
          return done(new Error("Expected undefined service, got something else" + service));
        }
        results.push(true);
        done();
      });
    });
  });

  setTimeout(() => {
    expect(results.length).toBe(2);
    done();
  }, 1000);
});


test('(1 pts) student test', (done) => {
  // Fill out this test case...
    done(new Error('Not implemented'));
});

test('(1 pts) student test', (done) => {
  // Fill out this test case...
    done(new Error('Not implemented'));
});

test('(1 pts) student test', (done) => {
  // Fill out this test case...
    done(new Error('Not implemented'));
});

/* Test infrastructure */

let localServer = null;

beforeAll((done) => {
  distribution.node.start((server) => {
    localServer = server;
    done();
  });
});

afterAll((done) => {
  localServer.close();
  done();
});
