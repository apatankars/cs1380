// const distribution = require('./config.js');
// const id = distribution.util.id;


// const nodes = [
//   { ip: "3.144.96.104", port: 1234 },
//   { ip: "3.21.106.86", port: 1234 },
//   { ip: "3.148.233.41", port: 1234 },
//   { ip: "13.59.147.228", port: 1234 },
//   { ip: "3.148.221.252", port: 1234 },
//   { ip: "3.137.162.13", port: 1234 },
//   { ip: "3.138.138.167", port: 1234 },
//   { ip: "18.189.188.238", port: 1234 },
// ];

// const groupConfig = {
//   gid: "tfidf", // Group ID for the distributed operation
// };

// const tfidfGroup = {}; // This will hold the node mappings for the tfidf group

// for(let i = 0; i < nodes.length; i++) {
//   let nodeConfig = nodes[i];
//   tfidfGroup[id.getSID(nodeConfig)] = nodeConfig; // Use the SID as key for the node
// }

// function isEmptyObject(obj) {
//   return obj && typeof obj === 'object' && Object.keys(obj).length === 0;
// }


// distribution.node.start(async (server) => {

//   console.log("Node started for testing distributed operations.");

//   distribution.local.groups.put(groupConfig, tfidfGroup, (e, v) => {
//     if (e) {
//       console.error("Error putting group:", e);
//       server.close();
//        console.log("Server stopped due to tfidf local group put error.");
//       return;
//     }

//     console.log(
//       "Successfully set up the tfidf group with nodes:",
//       v
//     );

//     console.log("Successfully set up the tfidf group with nodes.");

//     distribution.tfidf.groups.put(groupConfig, tfidfGroup, (e2, v2) => {
//       if (e2 && !isEmptyObject(e2)) {
//         console.error("Error putting tfidf group:", e2);
//         server.close();
//         console.log("Server stopped due to tfidf service group put an error.");
//         return;
//       }

//       console.log("Successfully set up the tfidf group in the distribution module.");

//       distribution.tfidf.status.get(['nid'], (e3, statusDict) => {
//         if (e3 && !isEmptyObject(e3)) {
//           console.error("Error getting tfidf status:", e3);
//           server.close();
//         console.log("Server stopped due to tfidf status put an error.");
//           return;
//         }
//         Object.keys(statusDict).forEach((nid) => {
//           const status = statusDict[nid];
//           if (status && status.error) {
//             console.error(`Error in node ${nid}:`, status.error);
//           } else {
//             console.log(`Node ${nid} status:`, status);
//           }
//         });
//         console.log("All nodes' tfidf status retrieved successfully.");
//         // Now we can safely stop the server after checking the status

//         // Now we want to check that all of the keys have been distributed accross the nodes in the tfidf group
//         distribution.tfidf.store.get({key: null}, (e4, v4) => {
//           if (e4) {
//             console.log("Error retrieving tfidf store:", e4);
//           } else {
//             console.log("Successfully retreived all keys from the tfidf store.");
//             console.log("Total keys in tfidf store:", v4.length);
//             console.log("Sample of keys in tfidf store:", v4.slice(0, 10)); // Log a sample of the keys to verify distribution
//           }

//           function parseArticleData(rawString) {
//             try {
//               // First parse the outer JSON structure
//               const outerObject = JSON.parse(rawString);

//               // console.log("Outer object:", outerObject);
//               if (outerObject.url) {
//                 return outerObject;
//               }
              
//               // Now parse the inner JSON string contained in the value property
//               if (outerObject && outerObject.type === 'string' &&outerObject.value) {
//                 const innerObject = JSON.parse(outerObject.value);
//                 console.log("Inner object:", innerObject);
//                 return innerObject;
//               } else {
//                 console.error("Unexpected data format");
//                 return null;
//               }
//             } catch (error) {
//               console.error("Error parsing JSON:", error);
//               console.log("First 100 chars:", rawString.substring(0, 100));
//               return null;
//             }
//           }

//           distribution.tfidf.store.get("-wiki--C3-97-Beruladium-procurrens", (e5, v5) => {
//             if (e5) {
//               console.error("Error retrieving specific key from tfidf store:", e5);
//             } else {
//               console.log("Successfully retrieved specific raw key from tfidf store.");
//               const parsedData = parseArticleData(v5);
//               console.log("Parsed data for -wiki--C3-97-Beruladium-procurrens:", parsedData);
//             }
//             server.close()
//             console.log("Server closed after successful tfidf status check.");
//           });
//         });
        
//       })
//     });

    
//   });
  

// });

const distribution = require("./config.js");
const id = distribution.util.id;

const outNodes = [
  { ip: "127.0.0.1", port: 8110 },
  { ip: "127.0.0.1", port: 8111 },
  { ip: "127.0.0.1", port: 8112 },
  { ip: "127.0.0.1", port: 8113 },
  { ip: "127.0.0.1", port: 8114 },
  { ip: "127.0.0.1", port: 8115 },
  { ip: "127.0.0.1", port: 8116 },
  { ip: "127.0.0.1", port: 8117 },
];

for(let i = 0; i < outNodes.length; i++) {
  let nodeConfig = outNodes[i];
  let nid = id.getNID(nodeConfig);
  console.log(`Node ${i}: Port: ${nodeConfig.port} with NID: ${nid}`);
}

// const notify = (config, callback) => {
//       const phase_map = {
//         MAP: "SHUFFLE",
//         SHUFFLE: "REDUCE"
//       };
      
//       if (config.status === "ERROR") {
//         console.error(`[MR-${mrId}] Error in phase ${config.phase}: ${config.error}`);
//         callback(Error(config.error), null);
//         return;
//       } 

//       function logPrefix(phase, nodeId = global.nodeConfig.port, subBatch = null) {
//         const batchInfo = `BATCH:${state_dict.batchIndex}`;
//         const subBatchInfo = subBatch ? `[SUB:${subBatch}]` : '';
//         return `[MR-${mrId}][${phase}][NODE:${nodeId}][${batchInfo}]${subBatchInfo}`;
//       }

//       console.log(`[MR-${mrId}] Received notification for phase ${config.phase} from node ${config.nodeId}. Status: ${config.status}`);

//       // Get the local group node count
//       distribution.local.groups.get(config.gid, (err, group) => {
//         if (err) {
//           console.error(`[MR-${mrId}] Error getting group nodes: ${err.message}`);
//           callback(err, null);
//           return;
//         }
//         let groupNodeCount = Object.keys(group).length;

//         // Increment the counter for responses received
//         state_dict.phase_count = state_dict.phase_count + 1;

//         if (config.phase !== state_dict.phase) {
//           console.error(`[MR-${mrId}] Phase mismatch. Expected ${state_dict.phase}, got ${config.phase}`);
//           callback(
//             Error(
//               `Error: Phase mismatch. Expected ${state_dict.phase}, got ${config.phase}`
//             ),
//             null
//           );
//           return;
//         }
        
//         // Track processing stats for each phase
//         if (config.phase === "MAP") {
//           if (config.processedKeys && Array.isArray(config.processedKeys)) {
//             // Add all processed keys to the global set
//             config.processedKeys.forEach(key => keyTrackingMap.processedKeys.add(key));
//             keyTrackingMap.mapPhaseStats.totalKeysProcessed += config.processedKeys.length;
//           }
//         } else if (config.phase === "SHUFFLE") {
//           if (config.shuffleStats) {
//             keyTrackingMap.shufflePhaseStats.totalEntries += config.shuffleStats.entriesProcessed || 0;
//           }
//         } else if (config.phase === "REDUCE") {
//           if (config.reduceStats) {
//             keyTrackingMap.reducePhaseStats.totalKeysProcessed += config.reduceStats.keysProcessed || 0;
//           }
//         }

//         // Log progress
//         const memUsage = process.memoryUsage();
//         console.log(
//           `[MR-${mrId}] Node ${global.nodeConfig.port}: Received notification for phase ${config.phase}. Current count: ${state_dict.phase_count}/${groupNodeCount}, batch: ${batchInfo.current}/${batchInfo.total}. Memory usage: heap=${Math.round(memUsage.heapUsed/1024/1024)}MB/${Math.round(memUsage.heapTotal/1024/1024)}MB (${Math.round((memUsage.heapUsed/memUsage.heapTotal)*100)}% used)`
//         );
        
//         // Collect reduce results
//         if (state_dict.phase === "REDUCE" && config.results) {
//           console.log(`${logPrefix('REDUCE-COLLECT')} Collecting results from ${config.nodeId}. Received ${config.results.length} results.`);
          
//           // Add to results directly for this single batch
//           results = results.concat(config.results);
          
//           console.log(`[MR-${mrId}] Total collected results: ${results.length}`);
//         }

//         // When all nodes have responded for the current phase
//         if (state_dict.phase_count === groupNodeCount) {
//           // Calculate phase duration
//           const phaseDuration = Date.now() - state_dict.batch_start_time;
//           console.log(`${logPrefix(state_dict.phase + '-COMPLETE')} Phase complete in ${phaseDuration}ms. Received ${state_dict.phase_count}/${groupNodeCount} responses.`);
          
//           // Update phase duration statistics
//           if (state_dict.phase === "MAP") {
//             keyTrackingMap.mapPhaseStats.totalDuration += phaseDuration;
//           } else if (state_dict.phase === "SHUFFLE") {
//             keyTrackingMap.shufflePhaseStats.totalDuration += phaseDuration;
//           } else if (state_dict.phase === "REDUCE") {
//             keyTrackingMap.reducePhaseStats.totalDuration += phaseDuration;
//           }
          
//           // Handle phase completion
//           if (state_dict.phase === "REDUCE") {
//             // Calculate batch duration
//             const batchDuration = Date.now() - state_dict.job_start_time;
//             console.log(`${logPrefix('COMPLETE')} Job completed in ${batchDuration}ms with ${results.length} results.`);
            
//             // Print key processing summary
//             const totalProcessed = keyTrackingMap.processedKeys.size;
            
//             console.log(`
// [MR-${mrId}] KEY PROCESSING SUMMARY:
// - Total processed keys: ${totalProcessed}

// Phase Statistics:
// - Map phase: ${keyTrackingMap.mapPhaseStats.totalKeysProcessed} keys processed in ${keyTrackingMap.mapPhaseStats.totalDuration}ms
// - Shuffle phase: ${keyTrackingMap.shufflePhaseStats.totalEntries} entries processed in ${keyTrackingMap.shufflePhaseStats.totalDuration}ms
// - Reduce phase: ${keyTrackingMap.reducePhaseStats.totalKeysProcessed} keys processed in ${keyTrackingMap.reducePhaseStats.totalDuration}ms
// `);
            
//             // Clean up and return results
//             distribution[context.gid].comm.send([config.jid], {service: 'routes', method: 'rem'}, (e, v) => {
//               console.log(`[MR-${mrId}] Job service removed, returning ${results.length} results`);
//               cb(null, results);
//             });
//           } else {
//             // Move to the next phase
//             let new_phase = phase_map[state_dict.phase];
//             state_dict.phase = new_phase;
//             state_dict.phase_count = 0;
//             state_dict.batch_start_time = Date.now(); // Reset time for new phase
            
//             console.log(`[MR-${mrId}] Node ${global.nodeConfig.port}: Moving to phase ${new_phase} for job: ${config.jid}`);
            
//             let method = state_dict.phase.toLowerCase();
//             let phaseConfig = {
//               gid: config.gid,
//               jid: config.jid
//             };
            
//             distribution[context.gid].comm.send([phaseConfig], {service: config.jid, method: method}, (err, val) => {
//               // No callback handling needed here
//             });
//           }
//         }
//       });
//     };

// let notifyRPC = distribution.util.wire.createRPC(distribution.util.wire.toAsync(notify));

// console.log("Notify RPC function created:", notifyRPC.toString());

