#!/usr/bin/env node

const distribution = require('../config.js');
const id = distribution.util.id;

// Slice off the first two elements (node and script path)
const args = process.argv.slice(2);

// Check if an argument was provided
if (args.length === 0) {
  console.error('Please provide an index argument.');
  process.exit(1);
}

const index = args[0];

const n1 = {ip: '172.31.8.158', port: 9001};
const n2 = {ip: '172.31.5.55', port: 9002};
const n3 = {ip: '172.31.3.4', port: 9003};

let spawnNode = null;

function checkNodeReady(node) {
  return new Promise((resolve) => {
    // For example, try an HTTP GET to a known “ready” endpoint
    http.get({ host: node.ip, port: node.port, path: '/status' }, (res) => {
      // You can add additional checks on res.statusCode or response body
      resolve(true);
    }).on('error', () => {
      resolve(false);
    });
  });
}

async function waitForNodes(nodes, timeout = 30000, interval = 1000) {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    const statuses = await Promise.all(nodes.map(checkNodeReady));
    if (statuses.every(Boolean)) {
      return;
    }
    await new Promise(r => setTimeout(r, interval));
  }
  throw new Error("Timeout waiting for nodes to be ready");
}

distribution.node.start(async (server) => {
  // Determine which node to spawn based on index
  switch (index) {
    case '1':
      console.log('Running code for index 1');
      spawnNode = n1;
      break;
    case '2':
      console.log('Running code for index 2');
      spawnNode = n2;
      break;
    case '3':
      console.log('Running code for index 3');
      spawnNode = n3;
      break;
    default:
      console.error(`No code associated with index: ${index}`);
      process.exit(1);
  }

  distribution.local.status.spawn(spawnNode, async (e, v) => {
    if (e) {
      console.error(e);
      process.exit(1);
    }
    console.log('Spawned node: ', spawnNode);

    // If this is node 1, wait until nodes 2 and 3 are ready
    if (index === '1') {
      try {
        await waitForNodes([n2, n3]);
        console.log("All nodes are ready. Proceeding to create the group.");

        const groupConfig = { gid: 'mygroup', hash: id.consistentHash };
        const groupObj = {};
        groupObj[id.getSID(n1)] = n1;
        groupObj[id.getSID(n2)] = n2;
        groupObj[id.getSID(n3)] = n3;

        distribution.local.groups.put(groupConfig, groupObj, (e, v) => {
          if (e) {
            console.error(e);
            process.exit(1);
          }
          console.log('Group created: ', groupObj);
          // Optionally, propagate the group to other services/nodes
          distribution.mygroup.groups.put(groupConfig, groupObj, (e, v) => {
            if (e) {
              console.error(e);
              process.exit(1);
            }
            console.log('Group propagated: ', groupObj);
          });
        });
      } catch (err) {
        console.error("Error waiting for nodes: ", err);
        process.exit(1);
      }
    }

    // After a delay, try retrieving the group (this is common for all nodes)
    setTimeout(() => {
      distribution.local.groups.get('mygroup', (e, v) => {
        if (e) {
          console.error(e);
          process.exit(1);
        }
        console.log('Group retrieved: ', v);
      });
    }, 15000); // Increased delay to ensure group creation completes
  });
});