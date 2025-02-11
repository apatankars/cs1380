const distribution = require("../config.js");

const TOTAL_REQUESTS = 1000;

distribution.node.start(() => {
  const nodeConfig = distribution.node.config;

  let finished = 0;
  const startTime = Date.now();

  for (let i = 0; i < TOTAL_REQUESTS; i++) {
    const message = ["counts"];
    const node = {
      node: nodeConfig,
      service: "status",
      method: "get"
    };

    distribution.local.comm.send(message, node, (err, val) => {
      if (err) {
        console.error("Error from comm.send:", err);
      }

      finished++;

      if (finished === TOTAL_REQUESTS) {
        const endTime = Date.now();
        const totalTimeMs = endTime - startTime;
        const throughputReqPerSec = (TOTAL_REQUESTS / totalTimeMs) * 1000;
        const avgLatencyMs = totalTimeMs / TOTAL_REQUESTS;

        console.log("=== COMM Performance Results ===");
        console.log(`Total Time:         ${totalTimeMs} ms`);
        console.log(`Throughput:         ${throughputReqPerSec.toFixed(2)} req/s`);
        console.log(`Avg Latency:        ${avgLatencyMs.toFixed(2)} ms`);
      }
    });
  }
});