const distribution = require('./config.js');
const id = distribution.util.id;


const nodes = [
  { ip: "3.144.96.104", port: 1234 },
  { ip: "3.21.106.86", port: 1234 },
  { ip: "3.148.233.41", port: 1234 },
  { ip: "13.59.147.228", port: 1234 },
  { ip: "3.148.221.252", port: 1234 },
  { ip: "3.137.162.13", port: 1234 },
  { ip: "3.138.138.167", port: 1234 },
  { ip: "18.189.188.238", port: 1234 },
];

const groupConfig = {
  gid: "tfidf", // Group ID for the distributed operation
};

const tfidfGroup = {}; // This will hold the node mappings for the tfidf group

for(let i = 0; i < nodes.length; i++) {
  let nodeConfig = nodes[i];
  tfidfGroup[id.getSID(nodeConfig)] = nodeConfig; // Use the SID as key for the node
}

function isEmptyObject(obj) {
  return obj && typeof obj === 'object' && Object.keys(obj).length === 0;
}


distribution.node.start(async (server) => {

  console.log("Node started for testing distributed operations.");

  distribution.local.groups.put(groupConfig, tfidfGroup, (e, v) => {
    if (e) {
      console.error("Error putting group:", e);
      server.close();
       console.log("Server stopped due to tfidf local group put error.");
      return;
    }

    console.log(
      "Successfully set up the tfidf group with nodes:",
      v
    );

    console.log("Successfully set up the tfidf group with nodes.");

    distribution.tfidf.groups.put(groupConfig, tfidfGroup, (e2, v2) => {
      if (e2 && !isEmptyObject(e2)) {
        console.error("Error putting tfidf group:", e2);
        server.close();
        console.log("Server stopped due to tfidf service group put an error.");
        return;
      }

      console.log("Successfully set up the tfidf group in the distribution module.");

      distribution.tfidf.status.get(['nid'], (e3, statusDict) => {
        if (e3 && !isEmptyObject(e3)) {
          console.error("Error getting tfidf status:", e3);
          server.close();
        console.log("Server stopped due to tfidf status put an error.");
          return;
        }
        Object.keys(statusDict).forEach((nid) => {
          const status = statusDict[nid];
          if (status && status.error) {
            console.error(`Error in node ${nid}:`, status.error);
          } else {
            console.log(`Node ${nid} status:`, status);
          }
        });
        console.log("All nodes' tfidf status retrieved successfully.");
        // Now we can safely stop the server after checking the status

        // Now we want to check that all of the keys have been distributed accross the nodes in the tfidf group
        distribution.tfidf.store.get({key: null}, (e4, v4) => {
          if (e4) {
            console.log("Error retrieving tfidf store:", e4);
          } else {
            console.log("Successfully retreived all keys from the tfidf store.");
            console.log("Total keys in tfidf store:", v4.length);
            console.log("Sample of keys in tfidf store:", v4.slice(0, 10)); // Log a sample of the keys to verify distribution
          }

          function parseArticleData(rawString) {
            try {
              // First parse the outer JSON structure
              const outerObject = JSON.parse(rawString);

              // console.log("Outer object:", outerObject);
              if (outerObject.url) {
                return outerObject;
              }
              
              // Now parse the inner JSON string contained in the value property
              if (outerObject && outerObject.type === 'string' &&outerObject.value) {
                const innerObject = JSON.parse(outerObject.value);
                console.log("Inner object:", innerObject);
                return innerObject;
              } else {
                console.error("Unexpected data format");
                return null;
              }
            } catch (error) {
              console.error("Error parsing JSON:", error);
              console.log("First 100 chars:", rawString.substring(0, 100));
              return null;
            }
          }

          distribution.tfidf.store.get("-wiki--C3-97-Beruladium-procurrens", (e5, v5) => {
            if (e5) {
              console.error("Error retrieving specific key from tfidf store:", e5);
            } else {
              console.log("Successfully retrieved specific raw key from tfidf store.");
              const parsedData = parseArticleData(v5);
              console.log("Parsed data for -wiki--C3-97-Beruladium-procurrens:", parsedData);
            }
            server.close()
            console.log("Server closed after successful tfidf status check.");
          });
        });
        
      })
    });

    
  });
  
});