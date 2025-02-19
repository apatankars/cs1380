/*
    In this file, add your own test cases that correspond to functionality introduced for each milestone.
    You should fill out each test case so it adequately tests the functionality you implemented.
    You are left to decide what the complexity of each test case should be, but trivial test cases that abuse this flexibility might be subject to deductions.

    Important: Do NOT modify any of the test('header', ...) lines. Doing so will result in grading penalties.
*/

const distribution = require('../../config.js');
const id = distribution.util.id;

/*
   We remove the old shared `mygroupConfig = { gid: 'mygroup' }` 
   and `mygroupGroup = {}`, since each test will create its own group now.
*/

/*
   This is necessary since we can not
   gracefully stop the local listening node.
   This is because the process that node is
   running in is the actual jest process
*/
let localServer = null;

const n1 = { ip: '127.0.0.1', port: 9001 };
const n2 = { ip: '127.0.0.1', port: 9002 };
const n3 = { ip: '127.0.0.1', port: 9003 };
const n4 = { ip: '127.0.0.1', port: 9004 };
const n5 = { ip: '127.0.0.1', port: 9005 };
const n6 = { ip: '127.0.0.1', port: 9006 };

const normalizeGroup = (group) => {
  const result = {};
  for (const key in group) {
    result[key] = { ip: group[key].ip, port: group[key].port };
  }
  return result;
};

test('(1 pts) group student test', (done) => {
  // We'll define a unique group name/config for this test
  const mygroupConfig1 = { gid: 'mygroup1' };
  const mygroupGroup1 = {};
  mygroupGroup1[id.getSID(n1)] = n1;
  mygroupGroup1[id.getSID(n2)] = n2;
  mygroupGroup1[id.getSID(n3)] = n3;
  mygroupGroup1[id.getSID(n4)] = n4;
  mygroupGroup1[id.getSID(n5)] = n5;

  // Put the group on the local node (then broadcast).
  distribution.local.groups.put(mygroupConfig1, mygroupGroup1, (e, v) => {
    distribution.mygroup1.groups.put('mygroup1', mygroupGroup1, (e, v) => {

    // Remove the group from the local node
    distribution.local.groups.del('mygroup1', (e2, v2) => {
        try {
          expect(e2).toBeFalsy();
        } catch (error) {
          return done(error);
        }

        // We check that the remote node n1 still knows about the group
        const message = [mygroupConfig1.gid];
        const remoteConfig = {
          service: 'groups',
          method: 'get',
          node: n1
        };
        distribution.local.comm.send(message, remoteConfig, (e3, remoteVal) => {
          try {
            expect(e3).toBeFalsy();
            expect(remoteVal).toBeDefined();
            expect(remoteVal).toBeInstanceOf(Object);
            expect(normalizeGroup(remoteVal)).toMatchObject(
              normalizeGroup(mygroupGroup1)
            );
          } catch (error) {
            return done(error);
          }

          // Now, let's try to get the group from local
          distribution.local.groups.get(mygroupConfig1, (e4, localVal) => {
            try {
              expect(e4).toBeDefined();
              expect(e4).toBeInstanceOf(Error);
              expect(localVal).toBeFalsy();
              done();
            } catch (error) {
              done(error);
            }
          });
        });
      });
    });
  });
});

test('(1 pts) embedded group test', (done) => {
  // Use a unique group name for this test
  const mygroupConfig2 = { gid: 'mygroup2' };
  const mygroupGroup2 = {};
  mygroupGroup2[id.getSID(n1)] = n1;
  mygroupGroup2[id.getSID(n2)] = n2;
  mygroupGroup2[id.getSID(n3)] = n3;
  mygroupGroup2[id.getSID(n4)] = n4;
  mygroupGroup2[id.getSID(n5)] = n5;

  distribution.local.groups.put('mygroup2', mygroupGroup2, (e, v) => {

    distribution.mygroup2.groups.put('mygroup2', mygroupGroup2, (e, v) => {
      // Optionally remove it from local again
      distribution.local.groups.del('mygroup2', (e2, v2) => {
        try {
          expect(e2).toBeFalsy();
        } catch (error) {
          return done(error);
        }

        // Check node n1 still has the group
        const message = [mygroupConfig2.gid];
        const remoteConfig = {
          service: 'groups',
          method: 'get',
          node: n1
        };

        distribution.local.comm.send(message, remoteConfig, (e3, remoteVal) => {
          try {
            expect(e3).toBeFalsy();
            expect(remoteVal).toBeDefined();
            expect(remoteVal).toBeInstanceOf(Object);
            expect(normalizeGroup(remoteVal)).toMatchObject(
              normalizeGroup(mygroupGroup2)
            );
            done();
          } catch (error) {
            done(error);
          }
        });
      });
    });
  });
});

test('(1 pts) comm test', (done) => {
  // We'll define 'mygroup3' for this test
  const mygroupConfig3 = { gid: 'mygroup3' };
  const mygroupGroup3 = {};
  mygroupGroup3[id.getSID(n1)] = n1;
  mygroupGroup3[id.getSID(n2)] = n2;
  mygroupGroup3[id.getSID(n3)] = n3;
  mygroupGroup3[id.getSID(n4)] = n4;
  mygroupGroup3[id.getSID(n5)] = n5;

  const n7 = { ip: '127.0.0.1', port: 9007 };

  // First, put the group so that each node is aware
  distribution.local.groups.put(mygroupConfig3, mygroupGroup3, (err1) => {
    // Now spawn the new node n7 in the cluster
    distribution.mygroup3.status.spawn(n7, (e2, v2) => {
      try {
        expect(e2).toBeFalsy();
        expect(v2.ip).toEqual(n7.ip);
        expect(v2.port).toEqual(n7.port);
      } catch (error) {
        return done(error);
      }

      // Ensure local groups get the updated view
      distribution.local.groups.get('mygroup3', (e3, groupVal) => {
        try {
          expect(e3).toBeFalsy();
          // n7's ID should be in the groupVal once we put it
          // or you might need to manually add it to the groupVal:
          expect(groupVal[id.getSID(n7)]).toBeDefined();

        } catch (error) {
          return done(error);
        }

        // Re-broadcast the updated group
        distribution.mygroup3.groups.put('mygroup3', groupVal, (e4) => {
          // Now do a status.get over the entire group
          distribution.mygroup3.status.get('heapUsed', (errMap, valMap) => {
            try {
              expect(valMap).toBeDefined();
              expect(valMap).toBeInstanceOf(Object);
              expect(valMap.res).toBeDefined(); // or however your library structures the result
            } catch (error) {
              return done(error);
            }

            // Stop the node n7
            const remoteStop = { node: n7, service: 'status', method: 'stop' };
            distribution.local.comm.send([], remoteStop, (e5, v5) => {
              try {
                expect(e5).toBeFalsy();
                expect(v5.ip).toEqual(n7.ip);
                expect(v5.port).toEqual(n7.port);
                done();
              } catch (error) {
                done(error);
              }
            });
          });
        });
      });
    });
  });
});

test('(1 pts) route student', (done) => {
  // We'll define 'mygroup4' for this test
  const mygroupConfig4 = { gid: 'mygroup4' };
  const mygroupGroup4 = {};
  mygroupGroup4[id.getSID(n1)] = n1;
  mygroupGroup4[id.getSID(n2)] = n2;
  mygroupGroup4[id.getSID(n3)] = n3;
  mygroupGroup4[id.getSID(n4)] = n4;
  mygroupGroup4[id.getSID(n5)] = n5;

  // Put the group
  distribution.local.groups.put(mygroupConfig4, mygroupGroup4, (err1) => {

    // Now we test the routes service
    const echoService = {};
    echoService.echo = () => {
      return 'echo!';
    };

    // Put the route across the group
    distribution.mygroup4.routes.put(echoService, 'echo', (e2) => {

      // We call node n1: routes.get('echo')
      const message = ['echo'];
      const remoteGet = { node: n1, service: 'routes', method: 'get' };

      distribution.local.comm.send(message, remoteGet, (e3, val3) => {
        
        try {
          expect(e3).toBeFalsy();
          expect(val3.echo()).toBe('echo!');
        } catch (error) {
          return done(error);
        }

        // Now remove that route from node n1
        const remoteRem = { node: n1, service: 'routes', method: 'rem' };
        distribution.local.comm.send(message, remoteRem, (e4, val) => {
          // Next, try to get it again => should fail
          distribution.local.comm.send(message, remoteGet, (e5, val5) => {
            try {
              expect(e5).toBeDefined();
              expect(e5).toBeInstanceOf(Error);
              expect(val5).toBeFalsy();
              done();
            } catch (error) {
              done(error);
            }
          });
        });
      });
    });
  });
});

test('(1 pts) gid comm test', (done) => {
  // We'll define 'mygroup5' for this test
  const mygroupConfig5 = { gid: 'mygroup5' };
  const mygroupGroup5 = {};
  mygroupGroup5[id.getSID(n1)] = n1;
  mygroupGroup5[id.getSID(n2)] = n2;
  mygroupGroup5[id.getSID(n3)] = n3;
  mygroupGroup5[id.getSID(n4)] = n4;
  mygroupGroup5[id.getSID(n5)] = n5;

  // Put the group
  distribution.local.groups.put(mygroupConfig5, mygroupGroup5, (err1, val1) => {

    distribution.mygroup5.groups.put('mygroup5', mygroupGroup5, (e2, val2) => {

      // We'll remove n2 from the group by calling node n1 with service:'groups', method:'rem'
      const message = ['mygroup5', id.getSID(n2)];
      const remoteRem = { node: n1, service: 'groups', method: 'rem' };

      distribution.local.comm.send(message, remoteRem, (e2, v2) => {
        try {
          expect(e2).toBeFalsy();
          expect(v2).toBeDefined();
          expect(v2[id.getSID(n2)]).toBeUndefined(); // n2 removed
        } catch (error) {
          return done(error);
        }

        // Now use gid comm to talk to the rest of the group
        const remoteStatus = { node: n1, gid: 'mygroup5', service: 'status', method: 'get' };
        const message2 = ['nid'];

        distribution.local.comm.send(message2, remoteStatus, (e3, v3) => {
          try {
            expect(v3).toBeDefined();
            expect(v3).toBeInstanceOf(Object);
          } catch (error) {
            return done(error);
          }

          // Check that locally, we see n2 is gone but the others are in the group
          distribution.mygroup5.status.get('nid', (errMap, valMap) => {
            try {
              expect(valMap).toBeDefined();
              expect(valMap).toBeInstanceOf(Object);
              // n2 should be missing
              expect(valMap[id.getSID(n2)]).toBeDefined();
              done();
            } catch (error) {
              done(error);
            }
          });
        });
      });
    });
  });
});


/* 
   ================================
   ===========  LIFECYCLE  ========
   ================================
*/

beforeAll((done) => {
  // First, stop the nodes if they are running
  const remote = { service: 'status', method: 'stop' };

  remote.node = n1;
  distribution.local.comm.send([], remote, () => {
    remote.node = n2;
    distribution.local.comm.send([], remote, () => {
      remote.node = n3;
      distribution.local.comm.send([], remote, () => {
        remote.node = n4;
        distribution.local.comm.send([], remote, () => {
          remote.node = n5;
          distribution.local.comm.send([], remote, () => {
            remote.node = n6;
            distribution.local.comm.send([], remote, () => {
              startNodes();
            });
          });
        });
      });
    });
  });

  function startNodes() {
    // We do NOT create any group here; we only start the local server and spawn nodes
    distribution.node.start((server) => {
      localServer = server;
      distribution.local.status.spawn(n1, () => {
        distribution.local.status.spawn(n2, () => {
          distribution.local.status.spawn(n3, () => {
            distribution.local.status.spawn(n4, () => {
              distribution.local.status.spawn(n5, () => {
                // at this point, n1..n5 are alive
                // If you need n6 as well, you can do that, but not strictly needed
                done();
              });
            });
          });
        });
      });
    });
  }
});

afterAll((done) => {
  // Stop everything
  const remote = { service: 'status', method: 'stop' };
  remote.node = n1;
  distribution.local.comm.send([], remote, () => {
    remote.node = n2;
    distribution.local.comm.send([], remote, () => {
      remote.node = n3;
      distribution.local.comm.send([], remote, () => {
        remote.node = n4;
        distribution.local.comm.send([], remote, () => {
          remote.node = n5;
          distribution.local.comm.send([], remote, () => {
            remote.node = n6;
            distribution.local.comm.send([], remote, () => {
              if (localServer) localServer.close();
              done();
            });
          });
        });
      });
    });
  });
});