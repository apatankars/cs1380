const distribution = require('../config.js');

const n1 = { ip: '127.0.0.1', port: 9001 };
const n2 = { ip: '127.0.0.1', port: 9002 };
const n3 = { ip: '127.0.0.1', port: 9003 };

const remote = {service: 'status', method: 'stop'};
remote.node = n1;
distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
        remote.node = n3;
            distribution.local.comm.send([], remote, (e, v) => {
                process.exit(0);
        })
    });
});