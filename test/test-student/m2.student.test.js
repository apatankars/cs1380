/*
    In this file, add your own test cases that correspond to functionality introduced for each milestone.
    You should fill out each test case so it adequately tests the functionality you implemented.
    You are left to decide what the complexity of each test case should be, but trivial test cases that abuse this flexibility might be subject to deductions.

    Imporant: Do not modify any of the test headers (i.e., the test('header', ...) part). Doing so will result in grading penalties.
*/

const util = require("@brown-ds/distribution/distribution/util/util.js");

const config = { ip: "127.0.0.1", port: 1234 };
const distribution = require("../../config.js")(config);
const local = distribution.local;
const routes = distribution.local.routes;
const id = distribution.util.id;

let checkIfDone = (results, expectedResults ,done) => {
  if (results.length === expectedResults) {
    done();
  }
}

test("(1 pts) student test", (done) => {
  let results = [];
  const node = distribution.node.config;
  const remote = { node: node, service: "status", method: "get" };
  const message = ["invalid"];

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

  const message2 = [
    "nid",
    "sid",
    "counts",
    "ip",
    "port",
    "heapTotal",
    "heapUsed",
  ];
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

test("(1 pts) student test", (done) => {
  let results = [];

  local.routes.put("invalid", null, (e, v) => {
    try {
      expect(e).toBeTruthy();
      expect(v).toBeFalsy();
      results.push(true);
    } catch (error) {
      done(error);
    }
  });

  const tempService = { foo: () => {} };
  local.routes.put(tempService, "temp", (err, name) => {
    if (err) return done(err);

    local.routes.rem("temp", (remErr) => {
      if (remErr) return done(remErr);

      local.routes.get("temp", (getErr, service) => {
        if (!getErr) {
          return done(
            getErr
          );
        }
        if (service !== undefined && service !== null) {
          return done(
            new Error(
              "Expected undefined service, got something else" + service
            )
          );
        }
        results.push(true);
      });
    });
  });

  setTimeout(() => {
    checkIfDone(results, 2, done);
  }, 1000);
});

test("(1 pts) student test", (done) => {
  let results = [];
  const expectedResults = 2; 

  local.routes.put("invalid", null, (e, v) => {
    try {
      expect(e).toBeTruthy();
      expect(v).toBeFalsy();
      results.push(true);
    } catch (error) {
      done(error);
    }
  });

  const tempService = { foo: () => {} };
  local.routes.put(tempService, "temp", (err, name) => {
    if (err) return done(err);

    local.routes.rem("temp", (remErr) => {
      if (remErr) return done(remErr);

      local.routes.get("temp", (getErr, service) => {
        try {
          expect(getErr).toBeTruthy();
          expect(service).toBeFalsy();
          results.push(true);
          checkIfDone(results, expectedResults, done);
        } catch (error) {
          done(error);
        }
      });
    });
  });
});

test("(1 pts) student test", (done) => {
  let results = [];
  const expectedResults = 2; // We expect two results

  const babyService = { methodA: () => {} };
  routes.put(babyService, "babyService", (err) => {
    if (err) return done(err);

    const babiedService = { methodB: () => {} };
    routes.put(babiedService, "babyService", (err2) => {
      // Assuming overwrite is allowed:
      if (err2) return done(err2);

      // Confirm that "get" now returns the secondService
      routes.get("babyService", (err3, service) => {
        try {
          expect(err3).toBeFalsy();
          expect(service).toBeTruthy();
          expect(service.methodB).toBeDefined();
          results.push(true);
        } catch (error) {
          done(error);
        }
      });
    });
  });

  const badService = null; // or { notAFunction: 123 } or any invalid shape

  routes.put(badService, "bad", (err) => {
    try {
      expect(err).toBeTruthy();
      results.push(true);
      checkIfDone(results, expectedResults, done);
    } catch (error) {
      done(error);
    }
  });
});

test("(1 pts) student test", (done) => {
  let results = [];
  const expectedResults = 2;

  routes.rem("neverExisted", (err) => {
    try {
      expect(err).toBeTruthy();
      results.push(true);
    } catch (error) {
      return done(error);
    }
  });

  const lockedService = { busy: true };
  routes.put(lockedService, "lockedService", (putErr) => {
    if (putErr) return done(putErr);

    routes.rem("lockedService", (remErr) => {
      if (remErr) return done(remErr);

      routes.get("lockedService", (getErr, service) => {
        try {
          expect(getErr).toBeTruthy();
          expect(service).toBeFalsy();
          results.push(true);
          checkIfDone(results, expectedResults, done);
        } catch (error) {
          done(error);
        }
      });
    });
  });

});

test("(1 pts) student test", (done) => {
  let testsCompleted = 0;
  let results = [];

    const nodeConfig = distribution.node.config;
    const message1 = null; 
    const remote1 = {
      node: nodeConfig,
      service: 'status',
      method: 'get' 
    };

    local.comm.send(message1, remote1, (err, val) => {
      try {
        expect(val).toBe(util.id.getNID(nodeConfig));
        testsCompleted++;
        results.push(true);
      } catch (error) {
        done(error);
      }
    });

    const message2 = ['dummy'];
    const remote2 = {
      node: nodeConfig,
      service: 'status',
      method: 'fakeMetho'  
    };

    local.comm.send(message2, remote2, (err, val) => {
      try {
        expect(err).toBeInstanceOf(Error);
        testsCompleted++;
        results.push(true);
        checkIfDone(results, testsCompleted, done);
      } catch (error) {
        done(error);
      }
    });
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
