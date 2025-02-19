const distribution = require('../config.js');
const { performance } = require('perf_hooks');

// Configuration for test nodes
const numNodesToLaunch = 10;
const testNodes = [
    {ip: '127.0.0.1', port: 9000, onStart: (s) => console.log("Node started at 9000")},
    {ip: '127.0.0.1', port: 9001, onStart: (s) => console.log("Node started at 9001")},
    {ip: '127.0.0.1', port: 9002, onStart: (s) => console.log("Node started at 9002")},
    {ip: '127.0.0.1', port: 9003, onStart: (s) => console.log("Node started at 9003")},
    {ip: '127.0.0.1', port: 9004, onStart: (s) => console.log("Node started at 9004")},
    {ip: '127.0.0.1', port: 9005, onStart: (s) => console.log("Node started at 9005")},
    {ip: '127.0.0.1', port: 9006, onStart: (s) => console.log("Node started at 9006")},
    {ip: '127.0.0.1', port: 9007, onStart: (s) => console.log("Node started at 9007")},
    {ip: '127.0.0.1', port: 9008, onStart: (s) => console.log("Node started at 9008")},
    {ip: '127.0.0.1', port: 9009, onStart: (s) => console.log("Node started at 9009")}
];

// Store performance metrics
const metrics = {
  startTimes: new Map(),
  endTimes: new Map(),
  latencies: new Map(),
  completedNodes: 0
};

// Promise-based node spawn
function spawnNode(nodeConfig) {
  return new Promise((resolve, reject) => {
    const ip = nodeConfig.ip;
    const port = nodeConfig.port;
    const nodeId = `${ip}:${port}`;
    const startTime = performance.now();
    metrics.startTimes.set(nodeId, startTime);
    
    console.log(`Starting spawn for node ID: ${nodeId}`);
    
    distribution.local.status.spawn({...nodeConfig}, (error, value) => {
      if (error) {
        return reject(error);
      }
      console.log(`Node ${nodeId} spawn completed`);

      const endTime = performance.now();
      metrics.endTimes.set(nodeId, endTime);
      metrics.latencies.set(nodeId, endTime - startTime);
      metrics.completedNodes++;

      resolve(value);
    });
  });
}

// Promise-based node cleanup
function cleanupNode(node) {
  return new Promise((resolve, reject) => {
    console.log(`Cleaning up node ${node.ip}:${node.port}`);
    const remote = { service: 'status', method: 'stop', node };
    
    distribution.local.comm.send([], remote, (error) => {
      if (error) {
        console.error(`Error stopping node ${node.ip}:${node.port}:`, error);
        return reject(error);
      }
      console.log(`Node ${node.ip}:${node.port} cleaned up successfully`);
      resolve();
    });
  });
}

// Sequential node launch and cleanup
async function launchNodesSequentially(nodes) {
  for (const node of nodes) {
    try {
      // Spawn the node and wait for completion
      console.log(`\nLaunching node at port ${node.port}`);
      await spawnNode(node);
      
      // Clean up this node before moving to the next one
      await cleanupNode(node);
      
      // Add a small delay between operations to ensure clean separation
      await new Promise(resolve => setTimeout(resolve, 100));
    } catch (error) {
      console.error(`Error processing node ${node.ip}:${node.port}:`, error);
      throw error;
    }
  }
}

// Main test function using async/await
async function measureNodeLaunchPerformance() {
  console.log('Starting sequential performance measurement...');

  return new Promise((resolve, reject) => {
    // Start the base listening node
    distribution.node.start(async (baseServer) => {
      try {
        console.log('Base node started');
        const startTime = performance.now();

        // Launch and clean up nodes sequentially
        await launchNodesSequentially(testNodes);

        const totalTime = performance.now() - startTime;

        // Calculate and report metrics
        const latencyValues = Array.from(metrics.latencies.values());
        const averageLatency = 
          latencyValues.reduce((a, b) => a + b, 0) / latencyValues.length;
        const throughput = (numNodesToLaunch / totalTime) * 1000; // nodes per second

        console.log('\nPerformance Results:');
        console.log(`Total nodes processed: ${metrics.completedNodes}`);
        console.log(`Average latency: ${averageLatency.toFixed(2)}ms`);
        console.log(`Throughput: ${throughput.toFixed(2)} nodes/second`);
        console.log(`Total time: ${totalTime.toFixed(2)}ms`);

        console.log('\nIndividual Node Latencies:');
        metrics.latencies.forEach((latency, nodeId) => {
          console.log(`Node ${nodeId}: ${latency.toFixed(2)}ms`);
        });

        // Close the base server
        if (baseServer && baseServer.close) {
          baseServer.close();
        }
        
        resolve();
      } catch (error) {
        console.error('Error during performance test:', error);
        if (baseServer && baseServer.close) {
          baseServer.close();
        }
        reject(error);
      }
    });
  });
}

// Run the performance test
measureNodeLaunchPerformance()
  .then(() => console.log('Performance test completed successfully'))
  .catch(error => console.error('Performance test failed:', error));