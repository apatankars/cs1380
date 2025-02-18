/*
    In this file, add your own test cases that correspond to functionality introduced for each milestone.
    You should fill out each test case so it adequately tests the functionality you implemented.
    You are left to decide what the complexity of each test case should be, but trivial test cases that abuse this flexibility might be subject to deductions.

    Imporant: Do not modify any of the test headers (i.e., the test('header', ...) part). Doing so will result in grading penalties.
*/

const distribution = require('../../config.js');
const id = distribution.util.id;

const mygroupConfig = {gid: 'mygroup'};
const mygroupGroup = {};

/*
   This is necessary since we can not
   gracefully stop the local listening node.
   This is because the process that node is
   running in is the actual jest process
*/
let localServer = null;

const n1 = {ip: '127.0.0.1', port: 9001};
const n2 = {ip: '127.0.0.1', port: 9002};
const n3 = {ip: '127.0.0.1', port: 9003};
const n4 = {ip: '127.0.0.1', port: 9004};
const n5 = {ip: '127.0.0.1', port: 9005};
const n6 = {ip: '127.0.0.1', port: 9006};

const normalizeGroup = (group) => {
  const result = {};
  for (const key in group) {
  result[key] = { ip: group[key].ip, port: group[key].port };
  }
  return result;
};

test('(1 pts) student test', (done) => {
  // In this test, we will try to create a group locally and then
  // create that same group for each its node's views

  // Then, I will remove the group from the local node's view, but
  // the group should still exist for each of the other nodes

  // Starting off, the mygroup exists from the testing infrastructure
  // and contains nodes n1, n2, n3, n4, n5

  // This broadcasts the group to all nodes within the group (all nodes now know about the group)
  distribution.mygroup.groups.put(mygroupConfig, mygroupGroup, (e, v) => {

    try {
      expect(e).toEqual({});
    } catch (error) {
      done(error);
    }

    distribution.local.groups.del(mygroupConfig, (e, v) => {

      try {
        expect(e).toBeFalsy();
      } catch (error) {
        done(error);
      }

      const message = [mygroupConfig.gid];
      const remoteConfig = {
        service: 'groups',
        method: 'get',
        node: n1,
      }
      distribution.local.comm.send(message, remoteConfig, (e,v) => {
        try {
          expect(e).toBeFalsy();
          expect(v).toBeDefined();
          expect(v).toBeInstanceOf(Object);
          expect(normalizeGroup(v)).toMatchObject(normalizeGroup(mygroupGroup));
        } catch (error) {
          done(error);
        }

        // Now, let's try to get the group from the local node
        distribution.local.groups.get(mygroupConfig, (e, v) => {
          try {
            expect(e).toBeDefined();
            expect(e).toBeInstanceOf(Error);
            expect(v).toBeFalsy();
            done();
          } catch (error) {
            done(error);
          }
        });
      })
    })
  });
});


test.only('(1 pts) student test', (done) => {
  // This is the test case for the status functionality
  jest.setTimeout(10000);
  
  // We will begin by sending a distributed call to spawn a node
  distribution.mygroup.status.spawn(n6, (e, v) => {
    try {
      expect(e).toEqual(null);
      expect(v).toBeDefined();
      expect(v.port).toEqual(n6.port);
    } catch (error) {
      done(error);
    }
    // Now, let's try to get the status of the group
    distribution.mygroup.status.get('nid', (e, v) => {
      try {
        // expect(e).toBe({});
        expect(v).toBeDefined();
        expect(v).toBeInstanceOf(Object);
        expect(v[id.getSID(n6)]).toBeDefined();
      } catch (error) {
        done(error);
      }
      // Now, let's make sure n1 is aware of the new node
      const message = [mygroupConfig.gid];
      const remoteConfig = {
        service: 'groups',
        method: 'get',
        node: n1,
      }
      distribution.local.comm.send(message, remoteConfig, (e,v) => {
        try {
          expect(e).toBeFalsy();
          expect(v).toBeDefined();
          expect(v).toBeInstanceOf(Object);
          expect(v[id.getSID(n6)]).toBeDefined();
        } catch (error) {
          done(error);
        }
      })
    });


  })
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

beforeAll((done) => {
  // First, stop the nodes if they are running
  const remote = {service: 'status', method: 'stop'};

  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
        remote.node = n4;
        distribution.local.comm.send([], remote, (e, v) => {
          remote.node = n5;
          distribution.local.comm.send([], remote, (e, v) => {
            remote.node = n6;
            distribution.local.comm.send([], remote, (e, v) => {
              startNodes();
            });
          });
        });
      });
    });
  });

  const startNodes = () => {
    mygroupGroup[id.getSID(n1)] = n1;
    mygroupGroup[id.getSID(n2)] = n2;
    mygroupGroup[id.getSID(n3)] = n3;
    mygroupGroup[id.getSID(n4)] = n4;
    mygroupGroup[id.getSID(n5)] = n5;


    const groupInstantiation = () => {
      // Create the groups
      distribution.local.groups
          .put(mygroupConfig, mygroupGroup, (e, v) => {
            done();
          });
    };


    // Now, start the nodes listening node
    distribution.node.start((server) => {
      localServer = server;

      // Start the nodes
      distribution.local.status.spawn(n1, (e, v) => {
        distribution.local.status.spawn(n2, (e, v) => {
          distribution.local.status.spawn(n3, (e, v) => {
            distribution.local.status.spawn(n4, (e, v) => {
              distribution.local.status.spawn(n5, (e, v) => {
                  groupInstantiation();
              });
            });
          });
        });
      });
    }); ;
  };
});

afterAll((done) => {
  const remote = {service: 'status', method: 'stop'};
  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
        remote.node = n4;
        distribution.local.comm.send([], remote, (e, v) => {
          remote.node = n5;
          distribution.local.comm.send([], remote, (e, v) => {
            remote.node = n6;
            distribution.local.comm.send([], remote, (e, v) => {
              localServer.close();
              done();
            });
          });
        });
      });
    });
  });
});


