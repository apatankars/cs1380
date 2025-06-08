const inNodes = [
  { ip: "127.0.0.1", port: 7112 },
  { ip: "127.0.0.1", port: 7113 },
  { ip: "127.0.0.1", port: 7114 },
  { ip: "127.0.0.1", port: 7115 },
];

const outNodes = [
  { ip: "127.0.0.1", port: 7112 },
  { ip: "127.0.0.1", port: 7113 },
  { ip: "127.0.0.1", port: 7114 },
  { ip: "127.0.0.1", port: 7115 },
  { ip: "127.0.0.1", port: 7116 },
  { ip: "127.0.0.1", port: 7117 },
  { ip: "127.0.0.1", port: 7118 },
  { ip: "127.0.0.1", port: 7119 },
];

// Create a combined list of unique node configurations for spawning
const nodes = [];
const uniqueNodes = new Map();

for (const nodeConfig of inNodes) {
    const sid = id.getSID(nodeConfig);
    const nid = id.getNID(nodeConfig);
    sourceGroup[sid] = nodeConfig;
    console.log(`Adding node ${nid} (${nodeConfig.ip}:${nodeConfig.port}) to groups`)
    // Ensure unique nodes for spawning
    if (!uniqueNodes.has(sid)) {
        uniqueNodes.set(sid, nodeConfig);
        nodes.push(nodeConfig); // Add to the spawn list
    }
}

for (const nodeConfig of outNodes) {
    const sid = id.getSID(nodeConfig);
    const nid = id.getNID(nodeConfig);
    destGroup[sid] = nodeConfig;
    console.log(`Adding node ${nid} (${nodeConfig.ip}:${nodeConfig.port}) to groups`)
    // Ensure unique nodes for spawning
    if (!uniqueNodes.has(sid)) {
        uniqueNodes.set(sid, nodeConfig);
        nodes.push(nodeConfig); // Add to the spawn list
    }
}