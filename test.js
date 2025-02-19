const { util } = require('@brown-ds/distribution');
const distribution = require('../app/config.js');
const id = distribution.util.id;


// This group is used for testing most of the functionality
const mygroupGroup = {};
// These groups are used for testing hashing
const group1Group = {};
const group2Group = {};
const group4Group = {};
const group3Group = {};

/*
   This hack is necessary since we can not
   gracefully stop the local listening node.
   This is because the process that node is
   running in is the actual jest process
*/
let localServer = null;

const n1 = {ip: '127.0.0.1', port: 9000};
const n2 = {ip: '127.0.0.1', port: 9001};
const n3 = {ip: '127.0.0.1', port: 9002};
const n4 = {ip: '127.0.0.1', port: 9003};
const n5 = {ip: '127.0.0.1', port: 9004};
const n6 = {ip: '127.0.0.1', port: 9005};
const n7 = {ip: '127.0.0.1', port: 9006};
const n8 = {ip: '127.0.0.1', port: 9007};
const n9 = {ip: '127.0.0.1', port: 9008};
const n10 = {ip: '127.0.0.1', port: 9009};



const startNodes = () => {
  mygroupGroup[id.getSID(n1)] = n1;
  mygroupGroup[id.getSID(n2)] = n2;
  mygroupGroup[id.getSID(n3)] = n3;

  group4Group[id.getSID(n1)] = n1;
  group4Group[id.getSID(n2)] = n2;
  group4Group[id.getSID(n4)] = n4;

  // Now, start the base listening node
  distribution.node.start((server) => {
    localServer = server;

    const groupInstantiation = (e, v) => {
      const mygroupConfig = {gid: 'mygroup'};
      const group4Config = {gid: 'group4'};

      // Create some groups
      distribution.local.groups
          .put(mygroupConfig, mygroupGroup, (e, v) => {
            distribution.local.groups
                .put(group4Config, group4Group, (e, v) => {
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
                groupInstantiation(e, v);

                  cleanUp();
              });
            });
          });
        });
      });
    });
  });
  };

const cleanUp = () => {
  // distribution.mygroup.status.stop((e, v) => {
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
                remote.node = n7;
                distribution.local.comm.send([], remote, (e, v) => {
                  remote.node = n8;
                  distribution.local.comm.send([], remote, (e, v) => {
                    remote.node = n9;
                    distribution.local.comm.send([], remote, (e, v) => {
                      remote.node = n10;
                      distribution.local.comm.send([], remote, (e, v) => {
                        // Now stop the local server
                        // localServer.close();
                        // console.log('Local server closed');
                        // done();
                        localServer.close();
                      });
                    });
                  });
                });
                
              });
            // });
          });
        });
      });
    });
  });
}

// Test the groups
// startNodes();
// cleanUp();
const NODES = [
  {
    ip: "127.0.0.1",
    port: 9007,
    onStart: (s) => console.log("Node started at 9007"),
  },
  {
    ip: "127.0.0.1",
    port: 9090,
    onStart: (s) => console.log("Node started at 9090"),
  },
  {
    ip: "127.0.0.1",
    port: 8002,
    onStart: (s) => console.log("Node started at 8002"),
  },
  {
    ip: "127.0.0.1",
    port: 8003,
    onStart: (s) => console.log("Node started at 8003"),
  },
  {
    ip: "127.0.0.1",
    port: 8004,
    onStart: (s) => console.log("Node started at 8004"),
  },
  {
    ip: "127.0.0.1",
    port: 8005,
    onStart: (s) => console.log("Node started at 8005"),
  },
];

// for(const node of NODES) {
//   console.log(util.serialize(node));
// }

// console.log(util.serialize({ip: "127.0.0.1", port: 8080, onStart: (s) => console.log("Node 1 Started")}))





