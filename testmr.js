/** @typedef {import("../types").Callback} Callback */
// const { log } = require("console");

const { local } = require("@brown-ds/distribution");


/**
 * Map functions used for mapreduce
 * @callback Mapper
 * @param {any} key
 * @param {any} value
 * @returns {object[]}
 */

/**
 * Reduce functions used for mapreduce
 * @callback Reducer
 * @param {any} key
 * @param {Array} value
 * @returns {object}
 */

/**
 * @typedef {Object} MRConfig
 * @property {Mapper} map
 * @property {Reducer} reduce
 * @property {string[]} keys
 */

/*
  Note: The only method explicitly exposed in the `mr` service is `exec`.
  Other methods, such as `map`, `shuffle`, and `reduce`, should be dynamically
  installed on the remote nodes and not necessarily exposed to the user.
*/

function mr(config) {
  const context = {
    gid: config.gid || "all",
  };

  /**
   * @param {MRConfig} configuration
   * @param {Callback} cb
   * @return {void}
   */
  function exec(configuration, cb) {
    const keys = configuration.keys
    const mapper = configuration.map;
    const reducer = configuration.reduce;
    const distribution = require("../../config");
    const mrId = require("crypto").randomUUID().substring(0, 8); // Get first 8 chars as ID
    const mrServiceName = `mr@${mrId}`; // mr@<uuid>

    // Configuration parameters - add these for fine-tuning
    // const BATCH_SIZE = 100; // Number of documents to process in one batch
    // const USE_MEMORY = true; // Use memory instead of disk storage
    // const SHUFFLE_BATCH_SIZE = 50; // Number of items to shuffle at once

    let results = [];

    let state_dict = {
      phase: "MAP",
      phase_count: 0,
    };
    /**
     * This is the notify service method which is called by each worker node whenever they are done with
     * a stage of the MapReduce. This method tracks the number of responses until it reaches the group size
     * at which point it makes a call each worker node to start the next part of the service. When the 
     * reducer returns, it provides its outputs which are then returned by the exec method
     * @param {*} config 
     *    phase: string of "MAP", "REDUCE", "SHUFFLE"
     *    status: string of "COMPLETED", "ERROR"
     *    gid: string of the group ID
     *    jid: string of the job ID (mr@<uuid>)
     *  
     * @param {*} cb 
     */
    const notify = (config, callback) => {
      const phase_map = {
        MAP: "SHUFFLE",
        SHUFFLE: "REDUCE",
        REDUCE: "DONE",
      };
      
      if (config.status === "ERROR") {
        callback(Error(config.error), null);
        return;
      } 
      
      // Special case for SETUP - initiates the map phase on all nodes
      if (config.phase === "SETUP") {
        // console.log(`Starting setup phase for all nodes`);
        const remote = {
          service: config.jid,
          method: 'map',
        }
        const setupConfig = {
          gid: config.gid,
          jid: config.jid
        }
        const message = [setupConfig];
        state_dict.phase = "MAP";
        state_dict.phase_count = 0;
        // console.log(`State dictionary updated to ${state_dict}`);
        distribution[context.gid].comm.send(message, remote, callback);
        return;
      }
      // Otherwise we get the local group node count by making a call to the group
      distribution.local.groups.get(config.gid, (err, group) => {
        if (err) {
          callback(err, null);
          return;
        }
        let groupNodeCount = Object.keys(group).length;

        // Increment the counter for responses received
        state_dict.phase_count = state_dict.phase_count + 1;

        if (config.phase !== state_dict.phase) {
          callback(
            Error(
              `Error: Phase mismatch. Expected ${state_dict.phase}, got ${config.phase}`
            ),
            null
          );
          return;
        }

        // Collect reduce results
        if (state_dict.phase === "REDUCE") {
          if (config.results) {
            // console.log(`Node ${global.nodeConfig.port}: Collecting reduce results:`, config.results);
            results = results.concat(config.results);
          }
        }

        // When all nodes have responded for the current phase
        console.log(
          `Node ${global.nodeConfig.port}: Phase ${state_dict.phase} complete. Received ${state_dict.phase_count} of ${groupNodeCount} responses.`
        );
        if (state_dict.phase_count === groupNodeCount) {
          // If we've finished reducing, return the results
          if (state_dict.phase === "REDUCE") {
            distribution[context.gid].comm.send([config.jid], {service: 'routes', method: 'rem'}, (e, v) => {
              // console.log(`COMPLETING ORCHESTRATION (deregistered custom route ${config.jid})`)
              cb(null, results);
              return;
            });
          }
          
          // Otherwise, move to the next phase
          let new_phase = phase_map[state_dict.phase];
          // console.log(`Moving to phase: ${new_phase}`);
          // Notify all nodes of the new phase
          state_dict.phase = new_phase;
          state_dict.phase_count = 0;
          console.log(`Notifying all nodes of new phase: ${new_phase} for job: ${config.jid}`);

          let endPoint = config.jid;
          let method = state_dict.phase.toLowerCase();

          let remote = {
            service: endPoint,
            method: method,
          }
          
          let phaseConfig = {
            gid: config.gid,
            jid: config.jid
          }
          
          const message = [phaseConfig];
          distribution[context.gid].comm.send(message, remote, (err, val) => {

          });
        }
      });
    };

    /**
     * 
     * @param {*} config
     *    mapper: this is the serialized version of the user provided mapper
     *    gid: this is the groupID 
     *    jid: this is the jobID (mr@<uuid>)
     * @param {*} cb 
     */
    const map = (config, callback) => {
      const gid = config.gid;
      const job_id = config.jid;
      const BATCH_SIZE = 100; 

      distribution.local.routes.get(job_id, (err, service) => {
        if (err) {
          callback(err, null);
          return;
        }

        const mapper = service.mapper;
        const storageService =  distribution.local.mem;
        
        // Get all keys first
        distribution.local.store.get({gid: gid, key: null}, (err, localKeys) => {
          if (err) {
            console.error(`Error retrieving keys for gid ${gid}: ${err.message}`);
            callback(err, null);
            return;
          }

          let filteredKeys = localKeys.filter(key => !key.includes('.DS_Store'));
          
          if (filteredKeys.length === 0) {
            console.log(`Node ${global.nodeConfig.port}: No keys found for gid ${gid}. Cannot proceed with map phase.`);
            service.notify({phase: "MAP", status: "COMPLETED", gid: gid, jid: job_id}, callback);
            return;
          }

          console.log(`Node ${global.nodeConfig.port}: Starting map phase with ${localKeys.length} keys`);
          
          // Process keys in batches
          let mapResults = [];
          let currentBatch = 0;
          const totalBatches = Math.ceil(filteredKeys.length / BATCH_SIZE);

          console.log(`Node ${global.nodeConfig.port}: Total batches to process: ${totalBatches} (BATCH_SIZE: ${BATCH_SIZE})`);
          
          const processBatch = () => {
            const startIdx = currentBatch * BATCH_SIZE;
            const endIdx = Math.min(startIdx + BATCH_SIZE, filteredKeys.length);
            const batchKeys = filteredKeys.slice(startIdx, endIdx);
            
            console.log(`Node ${global.nodeConfig.port}: Processing batch ${currentBatch + 1}/${totalBatches} with ${batchKeys.length} keys`);
            
            let batchResults = [];
            let keysProcessed = 0;
            
            if (batchKeys.length === 0) {
              // No more keys to process
              const mapResultName = "map@" + job_id;
              storageService.put(mapResults, {key: mapResultName, gid: gid}, (err, val) => {
                if (err) {
                  callback(err, null);
                  return;
                }
                service.notify({phase: "MAP", status: "COMPLETED", gid: gid, jid: job_id}, callback);
              });
              return;
            }
            
            // Process each key in the batch
            batchKeys.forEach(key => {
              distribution.local.store.get({key: key, gid: gid}, (err, val) => {
                if (err) {
                  // Just log the error and continue with other files
                  console.error(`Error processing ${key}: ${err.message}`);
                  keysProcessed++;
                } else {
                  try {
                    let res = mapper(key, val);
                    
                    if (!Array.isArray(res)) {
                      res = [res];
                    }
                    
                    batchResults = batchResults.concat(res);
                    
                  } catch (mapError) {
                    console.error(`Error mapping ${key}: ${mapError.message}`);
                  }
                  
                  keysProcessed++;
                  
                }

                if (keysProcessed >= batchKeys.length - 10 && currentBatch === totalBatches -1) {
                  
                }
                
                // Check if batch is complete
                if (keysProcessed === batchKeys.length) {
                  // Add batch results to total results
                  console.log(`Node ${global.nodeConfig.port}: Processing Batch[${currentBatch}/${totalBatches}]: Key ${key} for example ${keysProcessed}/${batchKeys.length}.`);
                  mapResults = mapResults.concat(batchResults);
                  currentBatch++;
                  
                  if (currentBatch < totalBatches) {
                    // Process next batch
                    processBatch();
                  } else {
                    // All batches processed, save results
                    const mapResultName = "map@" + job_id;
                    storageService.put(mapResults, {key: mapResultName, gid: gid}, (err, val) => {
                      if (err) {
                        callback(err, null);
                        return;
                      }
                      console.log(`Node ${global.nodeConfig.port}: Completed mapping with ${mapResults.length} total results`);
                      service.notify({phase: "MAP", status: "COMPLETED", gid: gid, jid: job_id}, callback);
                    });
                  }
                }
              });
            });
          };
          
          // Start processing the first batch
          processBatch();
        });
      });
    };
 // This is the end of the map method

    /**
     * For each node it should send the results of the map phase to the designated node 
     * using the given hash function provided by the user
     * @param {*} config 
     *    gid: this is the groupID
     *    jid: this is the jobID (mr@<uuid>)
     * @param {*} cb 
     */
    const shuffle = (config, callback) => {
      const gid = config.gid;
      const jid = config.jid;
      const SHUFFLE_BATCH_SIZE = 50; // Number of items to shuffle at once
      
      distribution.local.routes.get(jid, (err, service) => {
        if (err) {
          callback(err, null);
          return;
        }
        
        // Get map results from either memory or store
        const storageService =distribution.local.mem
        const mapResultName = "map@" + jid;
        
        storageService.get({key: mapResultName, gid: gid}, (err, mapResults) => {
          if (!mapResults || mapResults.length === 0) {
            service.notify({phase: "SHUFFLE", status: "COMPLETED", gid: gid, jid: jid}, callback);
            return;
          }
          
          console.log(`Node ${global.nodeConfig.port}: Shuffling ${mapResults.length} results`);
          
          // Group results by target node to minimize network calls
          distribution.local.groups.get(gid, (err, groupNodes) => {
            if (err) {
              console.error(`Error retrieving group nodes for gid ${gid}: ${err.message}`);
              callback(err, null);
              return;
            }

            // console.log(`Node ${global.nodeConfig.port}: Found ${Object.keys(groupNodes).length} group nodes for shuffling`);
            
            // Calculate target node for each key
            let nodeTargets = {};
            mapResults.forEach((entry) => {
              const key = Object.keys(entry)[0];
              
              // Determine target node for this key (using the same hash function as in store.js)
              const nodeConfigs = Object.values(groupNodes);
              const nids = nodeConfigs.map((nc) => distribution.util.id.getNID(nc));
              const kid = distribution.util.id.getID(key);
              // console.log(`Node ${global.nodeConfig.port}: Calculating target NID for key: ${key} (kid: ${kid}) for NIDs: ${nids.join(", ")}`);
              const targetNID = distribution.util.id.consistentHash(kid, nids);
              // console.log(`Node ${global.nodeConfig.port}: Key ${key} hashed to target NID ${targetNID}`);
              const targetNode = nodeConfigs.find((nc) => distribution.util.id.getNID(nc) === targetNID);

              // console.log(`Node ${global.nodeConfig.port}: Key ${key} hashed to target NID ${targetNID}`);
              
              if (!targetNode) {
                console.error(`No target node found for key ${key}`);
                return;
              }
              
              const targetNodeId = distribution.util.id.getSID(targetNode);

              // console.log(`Node ${global.nodeConfig.port}: Key ${key} hashed to target node ${targetNodeId} (NID: ${targetNID})`);
              
              // Initialize array for this target if it doesn't exist
              if (!nodeTargets[targetNodeId]) {
                nodeTargets[targetNodeId] = [];
              }
              
              nodeTargets[targetNodeId].push({
                key: key,
                entry: entry,
                jid: jid
              });
            });

            // console.log("Node:  ", global.nodeConfig.port,
            //   `NodeTargets: `, Object.keys(nodeTargets).length,
            //   `target nodes:`, Object.values(nodeTargets).join(", ")
            // );
            
            // Process each target node's batch
            const targetNodeIds = Object.keys(nodeTargets);
            // console.log(`Node ${global.nodeConfig.port}: Found ${targetNodeIds.length} target nodes for shuffling`);
            let nodesProcessed = 0;
            
            if (targetNodeIds.length === 0) {
              service.notify({phase: "SHUFFLE", status: "COMPLETED", gid: gid, jid: jid}, callback);
              return;
            }
            
            targetNodeIds.forEach(targetNodeId => {
              const entries = nodeTargets[targetNodeId];
              const targetNodeConfig = groupNodes[targetNodeId];

              console.log(`Node ${global.nodeConfig.port}: Processing ${entries.length} entries for target node ${targetNodeId}`);
              
              // Process entries in batches
              let batchesProcessed = 0;
              const totalBatches = Math.ceil(entries.length / SHUFFLE_BATCH_SIZE);
              
              const processBatch = (batchIndex) => {
                const startIdx = batchIndex * SHUFFLE_BATCH_SIZE;
                const endIdx = Math.min(startIdx + SHUFFLE_BATCH_SIZE, entries.length);
                const batchEntries = entries.slice(startIdx, endIdx);
                
                // Send batch to target node
                const batchData = {
                  entries: batchEntries,
                  jid: jid,
                  gid: gid
                };
                
                // Use a new bulk_append method that we'll add to store.js
                const config = {
                  service: 'mem',
                  method: 'bulk_append',
                  node: targetNodeConfig
                };
                
                distribution.local.comm.send([batchData], config, (err, result) => {
                  if (err) {
                    console.error(`Error sending batch to node ${targetNodeId}: ${err.message}`);
                  }
                  
                  batchesProcessed++;
                  
                  if (batchesProcessed < totalBatches) {
                    // Process next batch
                    processBatch(batchesProcessed);
                  } else {
                    // All batches for this node processed
                    nodesProcessed++;
                    
                    if (nodesProcessed === targetNodeIds.length) {
                      // All nodes processed
                      console.log(`Node ${global.nodeConfig.port}: Completed shuffling`);
                      service.notify({phase: "SHUFFLE", status: "COMPLETED", gid: gid, jid: jid}, callback);
                    }
                  }
                });
              };
              
              // Start processing the first batch
              processBatch(0);
            });
          });
        });
      });
    };


    /**
     * The reduce function should pull all of the local information and then call the user provided reduce function
     * @param {*} config 
     *    reducer: user provided reducer
     *    gid: this is the groupID
     *    jid: this is the jobID (mr@<uuid>)
     * @param {*} cb 
     */
    const reduce = (config, callback) => {
      const gid = config.gid;
      const job_id = config.jid;
      
      distribution.local.routes.get(job_id, (err, service) => {
        if (err) {
          callback(err, null);
          return;
        }
        
        const reducer = service.reducer;
        const shuffleResultName = "reduce@" + job_id;
        const storageService = distribution.local.mem;
        
        storageService.get({gid: gid, key: shuffleResultName}, (err, shuffleResults) => {
          if (err || !shuffleResults || Object.keys(shuffleResults).length === 0) {
            console.error(`Node ${global.nodeConfig.port}: Error retrieving shuffle results for gid ${gid} and job ${job_id}: ${err ? err.message : "no results found"}`);
            service.notify({phase: "REDUCE", status: "COMPLETED", results: [], gid: gid, jid: job_id}, callback);
            return;
          }
          
          const reduceKeys = Object.keys(shuffleResults);
          console.log(`Node ${global.nodeConfig.port}: Reducing ${reduceKeys.length} keys`);
          
          // Process keys in batches for better memory management
          const REDUCE_BATCH_SIZE = Math.min(50, reduceKeys.length);
          let currentBatch = 0;
          const totalBatches = Math.ceil(reduceKeys.length / REDUCE_BATCH_SIZE);
          let reduceResults = [];
          
          const processBatch = () => {
            const startIdx = currentBatch * REDUCE_BATCH_SIZE;
            const endIdx = Math.min(startIdx + REDUCE_BATCH_SIZE, reduceKeys.length);
            const batchKeys = reduceKeys.slice(startIdx, endIdx);
            
            let batchResults = [];
            let keysProcessed = 0;
            
            batchKeys.forEach(key => {
              let values = shuffleResults[key];
              
              if (!Array.isArray(values)) {
                values = [values];
              }
              
              try {
                const result = reducer(key, values);
                batchResults.push(result);
              } catch (reduceError) {
                console.error(`Error reducing key ${key}: ${reduceError.message}`);
              }
              
              keysProcessed++;
              
              if (keysProcessed === batchKeys.length) {
                // Add batch results to total results
                reduceResults = reduceResults.concat(batchResults);
                currentBatch++;
                
                if (currentBatch < totalBatches) {
                  // Process next batch
                  processBatch();
                } else {
                  // All batches processed, notify completion
                  console.log(`Node ${global.nodeConfig.port}: Completed reducing with ${reduceResults.length} results`);
                  service.notify({
                    phase: "REDUCE", 
                    status: "COMPLETED", 
                    results: reduceResults,
                    gid: gid, 
                    jid: job_id
                  }, callback);
                }
              }
            });
          };
          
          // Start processing the first batch
          processBatch();
        });
      });
    };

    // Create an RPC version of the notify method so it runs on the coordinator
    let notifyRPC = distribution.util.wire.createRPC(distribution.util.wire.toAsync(notify));
    // let asyncMap = util.wire.toAsync(map);


    // Create the service object with all methods
    let mrServiceObject = {
      notify: notifyRPC,
      map: map,
      mapper: mapper,
      reducer: reducer,
      shuffle: shuffle,
      reduce: reduce
    };
    
    // Register the service on all nodes in the group
    console.log("EXEC STARTS", global.nodeConfig, 'with keys', keys);
    console.log(global.nodeConfig.port, ": ", global.groupsTable);
    distribution[context.gid].routes.put(mrServiceObject, mrServiceName, (err, res) => {
      if (err) {
        cb(err, null);
        return;
      }

      // console.log(global.nodeConfig.port, ": ", global.groupsTable);

      // console.log(`Successfuly placed service object for group ${context.gid}`);
      
      // distribution.local.routes.get({gid: context.gid, service: mrServiceName}, (err, service) => {
      //   console.log(`local service object: ${service}`)
      //   service.notify({phase: "SETUP", status: "START", gid: gid, jid: mrServiceName}, (err, val) => {
      //     cb(null, val);
      //   });
      // })
      const setupConfig = {
          gid: context.gid,
          jid: mrServiceName,
          keys: keys
        }
      const message = [setupConfig];
      distribution[context.gid].comm.send(message, {gid: 'local', service: mrServiceName, method: 'map'}, (e, v) => {

      })
    });


  }

  return { exec };
}

module.exports = mr;