const distribution = require('../app/config.js');
const local = distribution.local;
const util = distribution.util;

const cb = (e, v) => {
    console.log(e, v);
    if (e) {
        console.error(e);
    } else {
        console.log(v);
    }
};

distribution.node.start(() => {
    const node = distribution.node.config;
  const remote = {node: node, service: 'routes', method: 'put'};
  const statusService = {get: local.status.get};
//   const message = [statusService, 'status'];
    const message = ['invalid'];
    local.comm.send(message, remote, cb);

    // const remote2 = {node: node, service: 'routes', method: 'get'};
    // const message2 = ['status'];
    // local.comm.send(message2, remote2, (err, service) => {
    //     if (err) {
    //         console.error(err);
    //     } else {
    //         service.get('nid', (e, v) => {
    //             console.log(e, v);
    //         });
    //     }
    // });
});

//  const message = ['nid'];
// console.log(JSON.stringify(util.deserialize(util.serialize(message))));
// console.log(Array.isArray(util.deserialize("{\"id\":\"54fe19e7-f4f9-4934-bccf-be9b4df06e49\",\"type\":\"array\",\"value\":{\"0\":\"{\\\"type\\\":\\\"null\\\",\\\"value\\\":\\\"\\\"}\",\"1\":\"{\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"8cf1b7dfcc03aaad55ac5448d8afd324e697b35c1e95eca61bfa4125a9c8419e\\\"}\"}}")));
