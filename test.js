const distribution = require("../app/config.js");
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

let testsCompleted = 0;

distribution.node.start(() => {
    const nodeConfig = distribution.node.config;

    // Alternative Edge Case 1: Server returns response that's not an array of length 2
    // To simulate this, we need to modify the server's behavior.
    // However, since we might not be able to modify the server's response directly,
    // we'll attempt to trigger an invalid response by sending an invalid request.

    const message1 = null; // Sending null as message
    const remote1 = {
      node: nodeConfig,
      service: 'status', // Assuming 'status' is a valid service
      method: 'get'      // Assuming 'get' is a valid method
    };

    distribution.local.comm.send(message1, remote1, (err, val) => {
      try {
        // Depending on implementation, sending null might cause an error
        console.log(err);
        console.log(val);
        testsCompleted++;
        // checkDone();
      } catch (error) {
        // done(error);
        console.error(error);
      }
    });

    // Alternative Edge Case 2: Server returns an error in the response
    // Let's simulate a server-side error by invoking a method that throws an error.

    const message2 = ['triggerError'];
    const remote2 = {
      node: nodeConfig,
      service: 'status',   // Assuming 'status' is a valid service
      method: 'errorTest'  // Assuming 'errorTest' method does not exist and will cause an error
    };

    distribution.local.comm.send(message2, remote2, (err, val) => {
      try {
        console.error("error" + err);
        testsCompleted++;
      } catch (error) {
        console.error(error);
      }
    });
  });