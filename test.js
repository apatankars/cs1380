const distribution = require('../app/config.js');
const id = distribution.util.id;

const n1 = {ip: '127.0.0.1', port: 7001};
const n2 = {ip: '127.0.0.1', port: 7002};
const n3 = {ip: '127.0.0.1', port: 7003};
const allNodes = [n1, n2, n3];

const myGroupConfig = { gid: "mygroup" };
const myGroupNodes = {};

// 5) Start the local “server” that receives messages.
distribution.node.start((server) => {
//   distribution.local.status.spawn(n1, (e, v) => {
//   });
//   distribution.local.status.spawn(n2, (e, v) => {
//   });
//   distribution.local.status.spawn(n3, (e, v) => {
//   });
//   // distribution.local.groups.put(myGroupConfig, myGroupNodes, (e, v) => {
//   //   if (e) {
//   //     console.error(e);
//   //   } else {
//   //     console.log("Group created successfully");
//   //   }
//   // });
distribution.local.comm.send(['nid'], {node: 2, service: 'status', method: 'get'}, (e, v) => {
  if (e) {
    console.error(e);
  } else {
    console.log(v);
  }
});

})