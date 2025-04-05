const distribution = require('./config.js');
const id = distribution.util.id;


// console.log(distribution.util.serialize({ a: {c: {d: { e: { f: 2}}}}, b: 2 }));
const storeDir = '/usr/src/app/store';
const path = require('path');


// const fileContent = fs.readFileSync(filePath, 'utf8');

// console.log("Processing file:", filePath);

// let deser = distribution.util.deserialize(fileContent);

// console.log("Deserialized content:", deser.url);

console.log(distribution.util.id.getNID({ ip: "127.0.0.1", port: 7112 }))
console.log(distribution.util.id.getNID({ ip: "127.0.0.1", port: 7113 }))
console.log(distribution.util.id.getNID({ ip: "127.0.0.1", port: 7114 }))
console.log(distribution.util.id.getNID({ ip: "127.0.0.1", port: 7115 }))
console.log(distribution.util.id.getNID({ ip: "127.0.0.1", port: 7116 }))
console.log(distribution.util.id.getNID({ ip: "127.0.0.1", port: 7117 }))
const num_nodes = 4;
const nids = [];
const nodes = [];
const testGroup = {};
const testConfig = { gid: "tfidf" };
// testGroup[distribution.util.id.getSID(node)] = node;
for(let i = 0; i < num_nodes; i++) {
    nodes.push({ ip: '127.0.0.1', port: 7112 + i });
    nids.push(id.getNID(nodes[i]));
    testGroup[id.getSID(nodes[i])] = nodes[i];
}

distribution.node.start(async (server) => {

  console.log("SETTING UP TF-IDF TEST NODE...");

  // Helper function to spawn a node
  const spawn_node = (node) =>
    new Promise((resolve, reject) =>
      distribution.local.status.spawn(node, (e, v) => {
        console.log(
          `Spawned node at ${node.ip}:${node.port} ${distribution.util.id.getNID(node)} with result:`,
          e ? e : v
        );
        resolve(e, v);
      })
    );

  // Helper function to stop a node
  const stop_node = (node) =>
    new Promise((resolve, reject) =>
      distribution.local.comm.send(
        [],
        { service: "status", method: "stop", node: node },
        (e, v) => resolve(e, v)
      )
    );

  // Start the node
  for (let i = 0; i < nodes.length; i++) {
    const node = nodes[i];
    try {
      await spawn_node(node);
      console.log(`Node started at ${node.ip}:${node.port}`);
    } catch (e) {
      console.error(`Failed to start node at ${node.ip}:${node.port}`, e);
      finish();
      return;
    }
  }

    // Now we can test the store functionality
    distribution.local.groups.put(testConfig, testGroup, (err, group) => {
      if (err) {
        console.error("Failed to get group:", err);
        return;
      }
      console.log("Put group:", testGroup);

      distribution.tfidf.groups.put(testConfig, testGroup, (errPut, valPut) => {
      
        console.log("Successfully put TF-IDF group:", valPut);

        distribution.tfidf.store.get({key: null}, (errGet, valGet) => {
          if (errGet) {
            console.error("Failed to get TF-IDF store:", errGet);
            return;
          }
          console.log("Successfully got TF-IDF store:", valGet, 
            `Total entries: ${valGet.length}`); // Log the number of entries in the store
          //  const remote = { service: "status", method: "stop" };
          // remote.node = node; // Use the spawned node to send the stop command

          

          // const message = [valGet[0]]; // Use the first entry in the TF-IDF store for testing
          console.log(
            `Attempting to retrieve TF-IDF store entry for key: ${valGet[0]}`
          );

          distribution.tfidf.store.get("-wiki--C3-97-Chitalpa-tashkentensis", (err, val) => {
            console.log("Successfully retrieved TF-IDF store entry:", val.article_words.length);

            // distribution.local.comm.send([], remote, (e, v) => {
              server.close(
                () => {
                  console.log(
                    `Server closed after stopping.`
                    
                  );
                  process.exit(0);
                }
              )

              // Exit the process after stopping the node
              
            
          })
        })

       
      });
    });
  })

