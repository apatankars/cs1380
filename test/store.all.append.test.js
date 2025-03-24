// test/store.all.append.test.js
const distribution = require('../config.js');
const id = distribution.util.id;

test('(1 pts) all.store.append error on null state', (done) => {
  const key = 'testAppendKeyAll';
  
  distribution.mygroup.store.append(null, key, (e, v) => {
    try {
      expect(e).toBeInstanceOf(Error);
      expect(v).toBeFalsy();
      done();
    } catch (error) {
      done(error);
    }
  });
});

test('(1 pts) all.store.append basic functionality', (done) => {
  const data = {count: 5};
  const key = 'basicAppendAll';
  
  distribution.mygroup.store.append(data, key, (e, v) => {
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

test('(2 pts) all.store.append multiple times aggregates values', (done) => {
  const data1 = {score: 10};
  const data2 = {score: 20};
  const key = 'aggregateAppendAll';
  
  distribution.mygroup.store.append(data1, key, (e, v) => {
    distribution.mygroup.store.append(data2, key, (e, v) => {
      try {
        expect(e).toBeFalsy();
        expect(v).toHaveProperty('score');
        expect(v.score).toEqual([10, 20]);
        done();
      } catch (error) {
        done(error);
      }
    });
  });
});

test('(1 pts) all.store.append with multiple keys', (done) => {
  const data = {points: 5, user: "bob"};
  const key = 'multiKeyAppendAll';
  
  distribution.mygroup.store.append(data, key, (e, v) => {
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

test('(1 pts) all.store.append/get', (done) => {
  const data = {clicks: 15};
  const key = 'appendGetAll';
  
  distribution.mygroup.store.append(data, key, (e, v) => {
    distribution.mygroup.store.get(key, (e, v) => {
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

test('(2 pts) all.store.append with null key uses hash', (done) => {
  const data = {name: "test"};
  
  distribution.mygroup.store.append(data, null, (e, v) => {
    distribution.mygroup.store.get(id.getID(data), (e, v) => {
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

test('(2 pts) all.store.append complex aggregation', (done) => {
  const data1 = {user: "alice", score: 10};
  const data2 = {user: "alice", score: 20};
  const data3 = {user: "alice", time: 30};
  const key = 'complexAppendAll';
  
  distribution.mygroup.store.append(data1, key, (e, v) => {
    distribution.mygroup.store.append(data2, key, (e, v) => {
      distribution.mygroup.store.append(data3, key, (e, v) => {
        try {
          expect(e).toBeFalsy();
          expect(v).toHaveProperty('user');
          expect(v).toHaveProperty('score');
          expect(v).toHaveProperty('time');
          expect(v.user).toEqual(["alice", "alice", "alice"]);
          expect(v.score).toEqual([10, 20]);
          expect(v.time).toEqual([30]);
          done();
        } catch (error) {
          done(error);
        }
      });
    });
  });
});

test('(2 pts) all.store.append works across different nodes', (done) => {
  // This test verifies that append works properly in the distributed system
  const data1 = {metric: 100};
  const data2 = {metric: 200};
  const key = 'crossNodeAppend';
  
  // First append to mygroup (one hash function)
  distribution.mygroup.store.append(data1, key, (e, v) => {
    // Then append to mygroupB (different hash function)
    distribution.mygroupB.store.append(data2, key, (e, v) => {
      // Each group should have its own copy with only its own appends
      distribution.mygroup.store.get(key, (e1, v1) => {
        distribution.mygroupB.store.get(key, (e2, v2) => {
          try {
            // mygroup should only have the first append
            expect(e1).toBeFalsy();
            expect(v1).toHaveProperty('metric');
            expect(v1.metric).toEqual([100]);
            
            // mygroupB should only have the second append
            expect(e2).toBeFalsy();
            expect(v2).toHaveProperty('metric');
            expect(v2.metric).toEqual([200]);
            done();
          } catch (error) {
            done(error);
          }
        });
      });
    });
  });
});

/*
    Following is the setup for the tests.
*/

const mygroupGroup = {};
const mygroupBGroup = {};

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

beforeAll((done) => {
  // First, stop the nodes if they are running
  const remote = {service: 'status', method: 'stop'};

  const fs = require('fs');
  const path = require('path');

  fs.rmSync(path.join(__dirname, '../store'), {recursive: true, force: true});
  fs.mkdirSync(path.join(__dirname, '../store'));

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

    mygroupBGroup[id.getSID(n1)] = n1;
    mygroupBGroup[id.getSID(n2)] = n2;
    mygroupBGroup[id.getSID(n3)] = n3;
    mygroupBGroup[id.getSID(n4)] = n4;
    mygroupBGroup[id.getSID(n5)] = n5;

    // Now, start the nodes listening node
    distribution.node.start((server) => {
      localServer = server;

      const groupInstantiation = () => {
        const mygroupConfig = {gid: 'mygroup'};
        const mygroupBConfig = {gid: 'mygroupB', hash: id.rendezvousHash};

        // Create the groups
        distribution.local.groups.put(mygroupBConfig, mygroupBGroup, (e, v) => {
          distribution.local.groups.put(mygroupConfig, mygroupGroup, (e, v) => {
            distribution.mygroup.groups.put(mygroupConfig, mygroupGroup, (e, v) => {
              done();
            });
          });
        });
      };

      // Start the nodes
      distribution.local.status.spawn(n1, (e, v) => {
        distribution.local.status.spawn(n2, (e, v) => {
          distribution.local.status.spawn(n3, (e, v) => {
            distribution.local.status.spawn(n4, (e, v) => {
              distribution.local.status.spawn(n5, (e, v) => {
                distribution.local.status.spawn(n6, (e, v) => {
                  groupInstantiation();
                });
              });
            });
          });
        });
      });
    });
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