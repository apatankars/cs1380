#!/usr/bin/env node

const distribution = require('../config.js');
const { id } = distribution.util;


// First, we define the AWS nodes and the group we want them to be apart of
const node1 = { ip: '18.220.184.192', port: 1234 };
const node2 = { ip: '18.117.173.221', port: 1234 };
const node3 = { ip: '18.220.188.68', port: 1234 };

const groupName = 'mygroup';
const groupConfig = { gid: groupName, hash: id.naiveHash };

const groupObj = {};
groupObj[id.getSID(node1)] = node1;
groupObj[id.getSID(node2)] = node2;
groupObj[id.getSID(node3)] = node3;


// This is the random test data i generated which just generates string at random
const NUM_ITEMS = 1000;
const testData = [];
function randomString(length) {
  let chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let result = '';
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}
// Pre-generate to avoid including generation time in measurement
for (let i = 0; i < NUM_ITEMS; i++) {
  const key = 'key_' + randomString(8) + '_' + i;
  const value = { index: i, random: randomString(16) };
  testData.push({ key, value });
}

function aggregateAndPrintResults(service, operation, latencies, totalTime) {
  const avgLatency = latencies.reduce((a, b) => a + b, 0) / latencies.length;
  const timeInSeconds = totalTime / 1000;
  const throughput = NUM_ITEMS / timeInSeconds;

  console.log(`\n${service.toUpperCase()} ${operation} Results:`);
  console.log(`  Total ops:          ${NUM_ITEMS}`);
  console.log(`  Total time (ms):    ${totalTime}`);
  console.log(`  Avg latency (ms):   ${avgLatency.toFixed(2)}`);
  console.log(`  Throughput (ops/s): ${throughput.toFixed(2)}`);
}

/**
 * Helper function to measure throughput of an async “all at once” operation:
 * We’ll queue all operations in parallel, measure latencies individually, and
 * then compute average latency + total throughput.
 */
function measurePuts(service, callback) {
  const latencies = [];
  let completed = 0;
  const startTime = Date.now();

  console.log(`\nStarting PUT operations for ${service.toUpperCase()} service`);

  testData.forEach(({ key, value }, idx) => {
    const t0 = Date.now();
    // The distributed call:
    distribution[groupName][service].put(value, key, (err, ret) => {
      const t1 = Date.now();
      latencies.push(t1 - t0);
      completed++;

      // Does it matter if I place this before or after the `Date.now` call for timing??
      if (err) {
        console.error(`Failed on the ${service}.put command for the key=${key} and got the error: `, err);
      }

      if (completed === NUM_ITEMS) {
        const totalTime = Date.now() - startTime; // ms
        aggregateAndPrintResults(service, 'PUT', latencies, totalTime);
        callback();
      }
    });
  });
}

function measureGets(service, callback) {
  const latencies = [];
  let completed = 0;
  const startTime = Date.now();

  console.log(`\nStarting GET operations for ${service.toUpperCase()} service`);

  testData.forEach(({ key }, idx) => {
    const t0 = Date.now();
    // The distributed call:
    distribution[groupName][service].get(key, (err, val) => {
      const t1 = Date.now();
      latencies.push(t1 - t0);
      completed++;

      if (err) {
        console.error(`Failed ${service}.get command for the key ${key} and got the error: `, err);
      }

      if (completed === NUM_ITEMS) {
        const totalTime = Date.now() - startTime;
        aggregateAndPrintResults(service, 'GET', latencies, totalTime);
        callback();
      }
    });
  });
}

distribution.node.start(() => {
  console.log(`Local node to begin communication started on ${global.nodeConfig.ip}:${global.nodeConfig.port}`);

  // This is all just for fun and to actually make sure i can see progress
  let nodeIdx = 1;
  for (const node in groupObj) {
    console.log(`Node ${nodeIdx} is at ${groupObj[node].ip}:${groupObj[node].port}`);
    nodeIdx++;
  }

  // Ok now the meat and potatoes. We first create the group so that when we make the calls
  // they are actually using the distributed version of these services
  distribution.local.groups.put(groupConfig, groupObj, (err, val) => {
    if (err) {
      console.error('Error creating group in local config:', err);
      process.exit(1);
    }
    console.log('mygroup created successfully with nodes: ', Object.keys(val));

    console.log('\nFirst testing in mem service');
    measurePuts('mem', () => {
      measureGets('mem', () => {
        console.log('\nNow testing store service');
        measurePuts('store', () => {
          measureGets('store', () => {
            console.log('\nPerformance test completed :)');
            process.exit(0);
          });
        });
      });
    });
  });
});