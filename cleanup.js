// const { node } = require("@brown-ds/distribution");
const distribution = require("./config.js");

const n1 = { ip: "127.0.0.1", port: 7110 };
const n2 = { ip: "127.0.0.1", port: 7111 };
const n3 = { ip: "127.0.0.1", port: 7112 };
const n4 = { ip: '127.0.0.1', port: 7113 };
const n5 = { ip: '127.0.0.1', port: 7114 };
const n6 = { ip: '127.0.0.1', port: 7115 };
const n7 = { ip: '127.0.0.1', port: 7116 };
const n8 = { ip: '127.0.0.1', port: 7117 };

const remote = { service: "status", method: "stop" };
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
                   process.exit(0);
            
                });  
              });
          });
        });
      });
    });
  });
});
