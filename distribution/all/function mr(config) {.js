function mr(config) {
  const context = {
    gid: config.gid || "all",
  };

  function exec(configuration, cb) {
    const keys = configuration.keys;
    const mapper = configuration.map;
    const reducer = configuration.reduce;
    const distribution = require("../../config");
    const mrId = require("crypto").randomUUID().substring(0, 8);
    const mrServiceName = `mr@${mrId}`;

    // Configuration parameters
    // const INPUT_BATCH_SIZE = 100; // Number of documents to process in one batch
    // const OUTPUT_CHUNK_SIZE = 10000; // Maximum results to store together
    // const USE_MEMORY = false; // Use disk to avoid memory pressure
    
    console.log(`Starting MapReduce job ${mrId} with ${keys.length} input keys`);
    
    let results = [];
    let state_dict = { 
      phase: "MAP", 
      phase_count: 0,
      checkpoints: {
        map_started: Date.now(),
        map_chunks_processed: 0,
        map_completed: 0,
        shuffle_started: 0,
        shuffle_completed: 0,
        reduce_started: 0,
        reduce_completed: 0
      }
    };
    
    // Log memory usage at checkpoints
    function logMemoryUsage(stage) {
      const mem = process.memoryUsage();
      console.log(`CHECKPOINT [${stage}] - Memory: RSS ${Math.round(mem.rss/1024/1024)}MB, Heap ${Math.round(mem.heapUsed/1024/1024)}/${Math.round(mem.heapTotal/1024/1024)}MB`);
    }

    const notify = (config, callback) => {
      // Existing phase transition logic
      const phase_map = {
        MAP: "SHUFFLE",
        SHUFFLE: "REDUCE",
        REDUCE: "DONE",
      };
      
      if (config.status === "ERROR") {
        console.error(`ERROR in ${config.phase}: ${config.error}`);
        callback(Error(config.error), null);
        return;
      } 
      
      if (config.phase === "SETUP") {
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
        state_dict.checkpoints.map_started = Date.now();
        logMemoryUsage("MAP_START");
        
        distribution[context.gid].comm.send(message, remote, callback);
        return;
      }
      
      // Get node count for tracking completion
      distribution.local.groups.get(config.gid, (err, group) => {
        if (err) {
          callback(err, null);
          return;
        }
        
        let groupNodeCount = Object.keys(group).length;
        state_dict.phase_count = state_dict.phase_count + 1;
        
        // CHECKPOINT: Log progress
        console.log(`CHECKPOINT: Node ${config.nodeId || 'unknown'} completed ${config.phase} phase (${state_dict.phase_count}/${groupNodeCount})`);
        
        if (config.stats) {
          console.log(`Node stats: ${JSON.stringify(config.stats)}`);
        }

        if (config.phase !== state_dict.phase) {
          callback(Error(`Phase mismatch. Expected ${state_dict.phase}, got ${config.phase}`), null);
          return;
        }

        // Collect reduce results
        if (state_dict.phase === "REDUCE") {
          if (config.results) {
            results = results.concat(config.results);
          }
        }

        // When all nodes have responded for the current phase
        if (state_dict.phase_count === groupNodeCount) {
          // CHECKPOINT: Phase completed
          state_dict.checkpoints[`${state_dict.phase.toLowerCase()}_completed`] = Date.now();
          logMemoryUsage(`${state_dict.phase}_COMPLETE`);
          
          const duration = (Date.now() - state_dict.checkpoints[`${state_dict.phase.toLowerCase()}_started`]) / 1000;
          console.log(`CHECKPOINT: ${state_dict.phase} phase completed in ${duration.toFixed(2)}s`);
          
          // If we've finished reducing, return the results
          if (state_dict.phase === "REDUCE") {
            distribution[context.gid].comm.send([config.jid], {service: 'routes', method: 'rem'}, (e, v) => {
              console.log(`CHECKPOINT: MapReduce job ${mrId} completed with ${results.length} results`);
              cb(null, results);
              return;
            });
          } else {
            // Move to the next phase
            let new_phase = phase_map[state_dict.phase];
            state_dict.phase = new_phase;
            state_dict.phase_count = 0;
            state_dict.checkpoints[`${new_phase.toLowerCase()}_started`] = Date.now();
            logMemoryUsage(`${new_phase}_START`);
            
            console.log(`CHECKPOINT: Starting ${new_phase} phase`);

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
              if (err) {
                console.error(`Error starting ${new_phase} phase: ${err.message}`);
              }
            });
          }
        }
      });
    };

    const map = (config, callback) => {
      const gid = config.gid;
      const job_id = config.jid;
      const nodeId = global.nodeConfig.port;
      const INPUT_BATCH_SIZE = 100
      const OUTPUT_CHUNK_SIZE = 10000;
      
      console.log(`Node ${nodeId}: Starting map phase with checkpoint tracking`);
      const nodeStartTime = Date.now();

      distribution.local.routes.get(job_id, (err, service) => {
        if (err) {
          console.error(`Node ${nodeId}: Error getting service: ${err.message}`);
          callback(err, null);
          return;
        }

        const mapper = service.mapper;
        
        // CHECKPOINT: Report before getting keys
        console.log(`Node ${nodeId}: Getting keys from store (Checkpoint 1)`);
        const getKeysStart = Date.now();
        
        distribution.local.store.get({gid: gid, key: null}, (err, localKeys) => {
          if (err) {
            console.error(`Node ${nodeId}: Error getting keys: ${err.message}`);
            callback(err, null);
            return;
          }
          
          // CHECKPOINT: Report key retrieval time
          console.log(`Node ${nodeId}: Found ${localKeys.length} keys in ${Date.now() - getKeysStart}ms (Checkpoint 2)`);
          
          if (localKeys.length === 0) {
            console.log(`Node ${nodeId}: No keys to process`);
            service.notify({
              phase: "MAP", 
              status: "COMPLETED", 
              nodeId: nodeId,
              gid: gid, 
              jid: job_id
            }, callback);
            return;
          }
          
          // Process keys in input batches
          const totalBatches = Math.ceil(localKeys.length / INPUT_BATCH_SIZE);
          console.log(`Node ${nodeId}: Total batches to process: ${totalBatches} (BATCH_SIZE: ${INPUT_BATCH_SIZE})`);
          
          // Track output chunks - critical addition
          let currentChunk = [];
          let chunkCount = 0;
          
          // Process one input batch at a time
          function processNextBatch(batchIndex) {
            if (batchIndex >= totalBatches) {
              // All input batches processed
              
              // Store final chunk if not empty
              if (currentChunk.length > 0) {
                storeResultChunk(currentChunk, chunkCount, true);
              } else {
                // All chunks stored already
                finishMapPhase();
              }
              return;
            }
            
            const startIdx = batchIndex * INPUT_BATCH_SIZE;
            const endIdx = Math.min(startIdx + INPUT_BATCH_SIZE, localKeys.length);
            const batchKeys = localKeys.slice(startIdx, endIdx);
            
            console.log(`Node ${nodeId}: Processing batch ${batchIndex + 1}/${totalBatches} with ${batchKeys.length} keys`);
            
            let keysProcessed = 0;
            let batchStartTime = Date.now();
            
            // Process each key in the batch
            batchKeys.forEach(key => {
              distribution.local.store.get({key: key, gid: gid}, (err, val) => {
                if (err) {
                  console.error(`Node ${nodeId}: Error processing key ${key}: ${err.message}`);
                  keysProcessed++;
                } else {
                  try {
                    // Apply mapper function
                    let res = mapper(key, val);
                    
                    if (!Array.isArray(res)) {
                      res = [res];
                    }
                    
                    // Add results to current chunk
                    currentChunk = currentChunk.concat(res);
                    
                    // If chunk size exceeds limit, store it
                    if (currentChunk.length >= OUTPUT_CHUNK_SIZE) {
                      storeResultChunk(currentChunk, chunkCount, false);
                      chunkCount++;
                      currentChunk = []; // Reset for next chunk
                    }
                    
                    keysProcessed++;
                  } catch (mapError) {
                    console.error(`Node ${nodeId}: Error mapping key ${key}: ${mapError.message}`);
                    keysProcessed++;
                  }
                }
                
                // Check if batch is complete
                if (keysProcessed === batchKeys.length) {
                  // CHECKPOINT: Batch completed
                  const batchTime = Date.now() - batchStartTime;
                  console.log(`Node ${nodeId}: Completed batch ${batchIndex + 1} in ${batchTime}ms, processed ${startIdx + keysProcessed}/${localKeys.length} keys`);
                  
                  // Process next batch
                  processNextBatch(batchIndex + 1);
                }
              });
            });
          }
          
          // Helper to store a chunk of results
          function storeResultChunk(chunk, chunkIndex, isFinal) {
            const chunkName = `map@${job_id}_chunk_${chunkIndex}`;
            
            // CHECKPOINT: Store chunk
            console.log(`Node ${nodeId}: Storing result chunk ${chunkIndex} with ${chunk.length} entries`);
            const storeStart = Date.now();
            
            // Use store instead of mem for large datasets
            distribution.local.store.put(chunk, {key: chunkName, gid: gid}, (err) => {
              if (err) {
                console.error(`Node ${nodeId}: Error storing chunk ${chunkIndex}: ${err.message}`);
              } else {
                console.log(`Node ${nodeId}: Stored chunk ${chunkIndex} in ${Date.now() - storeStart}ms`);
              }
              
              // If this is the final chunk, update metadata and finish
              if (isFinal) {
                // Store metadata for shuffle phase
                const metadata = {
                  totalChunks: chunkCount + 1,
                  lastUpdated: Date.now()
                };
                
                distribution.local.store.put(metadata, {key: `map@${job_id}_meta`, gid: gid}, (err) => {
                  if (err) {
                    console.error(`Node ${nodeId}: Error storing metadata: ${err.message}`);
                  }
                  
                  finishMapPhase();
                });
              }
            });
          }
          
          // Helper to finish the map phase
          function finishMapPhase() {
            // CHECKPOINT: Map phase completed
            const totalTime = Date.now() - nodeStartTime;
            console.log(`Node ${nodeId}: Completed mapping in ${totalTime}ms with ${chunkCount + 1} chunks`);
            
            logMemoryUsage(`MAP_NODE_${nodeId}_COMPLETE`);
            
            // Notify completion
            service.notify({
              phase: "MAP",
              status: "COMPLETED",
              nodeId: nodeId,
              stats: {
                chunks: chunkCount + 1,
                processingTime: totalTime,
                keysProcessed: localKeys.length
              },
              gid: gid,
              jid: job_id
            }, callback);
          }
          
          // Start processing the first batch
          processNextBatch(0);
        });
      });
    };

    const shuffle = (config, callback) => {
      const gid = config.gid;
      const jid = config.jid;
      const nodeId = global.nodeConfig.port;
      
      console.log(`Node ${nodeId}: Starting shuffle phase`);
      const nodeStartTime = Date.now();
      
      distribution.local.routes.get(jid, (err, service) => {
        if (err) {
          console.error(`Node ${nodeId}: Error getting service: ${err.message}`);
          callback(err, null);
          return;
        }
        
        // CHECKPOINT: Get node info
        distribution.local.groups.get(gid, (err, groupNodes) => {
          if (err) {
            console.error(`Node ${nodeId}: Error getting group nodes: ${err.message}`);
            callback(err, null);
            return;
          }
          
          // Get metadata for map results
          distribution.local.store.get({key: `map@${jid}_meta`, gid: gid}, (err, metadata) => {
            if (err || !metadata) {
              console.log(`Node ${nodeId}: No map metadata, nothing to shuffle`);
              
              service.notify({
                phase: "SHUFFLE",
                status: "COMPLETED",
                nodeId: nodeId,
                gid: gid,
                jid: jid
              }, callback);
              return;
            }
            
            const totalChunks = metadata.totalChunks || 0;
            console.log(`Node ${nodeId}: Found ${totalChunks} map result chunks to shuffle`);
            
            if (totalChunks === 0) {
              service.notify({
                phase: "SHUFFLE",
                status: "COMPLETED",
                nodeId: nodeId,
                gid: gid,
                jid: jid
              }, callback);
              return;
            }
            
            // Process chunks sequentially to avoid memory issues
            function processNextChunk(chunkIndex) {
              if (chunkIndex >= totalChunks) {
                // All chunks processed
                const totalTime = Date.now() - nodeStartTime;
                console.log(`Node ${nodeId}: Completed shuffling all chunks in ${totalTime}ms`);
                
                service.notify({
                  phase: "SHUFFLE",
                  status: "COMPLETED",
                  nodeId: nodeId,
                  stats: {
                    chunksProcessed: totalChunks,
                    processingTime: totalTime
                  },
                  gid: gid,
                  jid: jid
                }, callback);
                return;
              }
              
              // CHECKPOINT: Get chunk
              const chunkName = `map@${jid}_chunk_${chunkIndex}`;
              console.log(`Node ${nodeId}: Processing chunk ${chunkIndex + 1}/${totalChunks}`);
              
              distribution.local.store.get({key: chunkName, gid: gid}, (err, mapResults) => {
                if (err || !mapResults) {
                  console.error(`Node ${nodeId}: Error retrieving chunk ${chunkIndex}: ${err ? err.message : 'No results'}`);
                  // Skip to next chunk
                  processNextChunk(chunkIndex + 1);
                  return;
                }
                
                // CHECKPOINT: Group by target node
                console.log(`Node ${nodeId}: Grouping ${mapResults.length} results by target node`);
                
                // Calculate target node for each key
                let nodeTargets = {};
                
                mapResults.forEach((entry) => {
                  const key = Object.keys(entry)[0];
                  
                  // Determine target node
                  const nodeConfigs = Object.values(groupNodes);
                  const nids = nodeConfigs.map((nc) => distribution.util.id.getNID(nc));
                  const kid = distribution.util.id.getID(key);
                  const targetNID = distribution.util.id.naiveHash(kid, nids);
                  const targetNode = nodeConfigs.find((nc) => distribution.util.id.getNID(nc) === targetNID);
                  
                  if (!targetNode) {
                    console.error(`Node ${nodeId}: No target node found for key ${key}`);
                    return;
                  }
                  
                  const targetNodeId = distribution.util.id.getSID(targetNode);
                  
                  if (!nodeTargets[targetNodeId]) {
                    nodeTargets[targetNodeId] = [];
                  }
                  
                  nodeTargets[targetNodeId].push({
                    key: key,
                    entry: entry,
                    jid: jid
                  });
                });
                
                const targetNodeIds = Object.keys(nodeTargets);
                
                // CHECKPOINT: Sending to target nodes
                console.log(`Node ${nodeId}: Sending results to ${targetNodeIds.length} target nodes`);
                
                // Track node processing
                let nodesProcessed = 0;
                
                // For each target node, process entries in batches to avoid overwhelming
                targetNodeIds.forEach(targetNodeId => {
                  const entries = nodeTargets[targetNodeId];
                  const targetNodeConfig = groupNodes[targetNodeId];
                  
                  // Send in small batches of 1000
                  const SEND_BATCH_SIZE = 1000;
                  let batchesSent = 0;
                  const totalSendBatches = Math.ceil(entries.length / SEND_BATCH_SIZE);
                  
                  function sendNextBatch() {
                    if (batchesSent >= totalSendBatches) {
                      // All batches sent to this node
                      nodesProcessed++;
                      
                      if (nodesProcessed === targetNodeIds.length) {
                        // All nodes processed for this chunk
                        
                        // CHECKPOINT: Chunk fully processed
                        console.log(`Node ${nodeId}: Completed shuffle distribution for chunk ${chunkIndex + 1}`);
                        
                        // Move to next chunk with a slight delay to prevent overload
                        setTimeout(() => {
                          processNextChunk(chunkIndex + 1);
                        }, 100);
                      }
                      return;
                    }
                    
                    const startIdx = batchesSent * SEND_BATCH_SIZE;
                    const endIdx = Math.min(startIdx + SEND_BATCH_SIZE, entries.length);
                    const batch = entries.slice(startIdx, endIdx);
                    
                    // CHECKPOINT: Sending batch
                    console.log(`Node ${nodeId}: Sending batch ${batchesSent + 1}/${totalSendBatches} to node ${targetNodeId} (${batch.length} entries)`);
                    
                    // Send batch to target node
                    const batchData = {
                      entries: batch,
                      jid: jid,
                      gid: gid
                    };
                    
                    const config = {
                      service: 'store',
                      method: 'bulk_append',
                      node: targetNodeConfig
                    };
                    
                    distribution.local.comm.send([batchData], config, (err, res) => {
                      if (err) {
                        console.error(`Node ${nodeId}: Error sending batch to node ${targetNodeId}: ${err.message}`);
                      } else {
                        // CHECKPOINT: Batch sent successfully
                        console.log(`Node ${nodeId}: Successfully sent batch ${batchesSent + 1} to node ${targetNodeId}`);
                      }
                      
                      batchesSent++;
                      
                      // Send next batch with a small delay to prevent overload
                      setTimeout(sendNextBatch, 50);
                    });
                  }
                  
                  // Start sending batches
                  sendNextBatch();
                });
              });
            }
            
            // Start with the first chunk
            processNextChunk(0);
          });
        });
      });
    };

    const reduce = (config, callback) => {
      const gid = config.gid;
      const job_id = config.jid;
      const nodeId = global.nodeConfig.port;
      
      console.log(`Node ${nodeId}: Starting reduce phase`);
      const nodeStartTime = Date.now();
      
      distribution.local.routes.get(job_id, (err, service) => {
        if (err) {
          console.error(`Node ${nodeId}: Error getting service: ${err.message}`);
          callback(err, null);
          return;
        }
        
        const reducer = service.reducer;
        const shuffleResultName = "reduce@" + job_id;
        
        // CHECKPOINT: Find all shuffle result files
        const nodeConfig = global.nodeConfig;
        const nodeID = util.id.getNID(nodeConfig);
        const groupDir = path.join('store', nodeID, gid);
        
        // Find shuffle result files using a pattern
        const filePattern = new RegExp(`^${shuffleResultName}_.*\\.json$`);
        
        fs.readdir(groupDir, (err, files) => {
          if (err) {
            console.error(`Node ${nodeId}: Error reading directory: ${err.message}`);
            callback(err, null);
            return;
          }
          
          // Filter for shuffle result files
          const shuffleFiles = files.filter(file => filePattern.test(file));
          
          // CHECKPOINT: Report found files
          console.log(`Node ${nodeId}: Found ${shuffleFiles.length} shuffle result files`);
          
          if (shuffleFiles.length === 0) {
            // No shuffle results to reduce
            service.notify({
              phase: "REDUCE",
              status: "COMPLETED",
              results: [],
              nodeId: nodeId,
              gid: gid,
              jid: job_id
            }, callback);
            return;
          }
          
          // Process files in batches
          const FILE_BATCH_SIZE = 5; // Process 5 files at a time to limit memory use
          let fileIndex = 0;
          let reduceResults = [];
          
          // CHECKPOINT: Report start of file processing
          console.log(`Node ${nodeId}: Starting to process files in batches of ${FILE_BATCH_SIZE}`);
          
          function processNextFileBatch() {
            if (fileIndex >= shuffleFiles.length) {
              // All files processed
              const totalTime = Date.now() - nodeStartTime;
              
              // CHECKPOINT: Report reduction complete
              console.log(`Node ${nodeId}: Reduce completed in ${totalTime}ms with ${reduceResults.length} results`);
              
              service.notify({
                phase: "REDUCE",
                status: "COMPLETED",
                results: reduceResults,
                nodeId: nodeId,
                stats: {
                  filesProcessed: shuffleFiles.length,
                  resultsGenerated: reduceResults.length,
                  processingTime: totalTime
                },
                gid: gid,
                jid: job_id
              }, callback);
              return;
            }
            
            const endIndex = Math.min(fileIndex + FILE_BATCH_SIZE, shuffleFiles.length);
            const batch = shuffleFiles.slice(fileIndex, endIndex);
            
            // CHECKPOINT: Report batch processing
            console.log(`Node ${nodeId}: Processing file batch ${Math.floor(fileIndex/FILE_BATCH_SIZE) + 1}/${Math.ceil(shuffleFiles.length/FILE_BATCH_SIZE)} (${batch.length} files)`);
            
            let filesProcessed = 0;
            let batchResults = [];
            
            batch.forEach(file => {
              const filePath = path.join(groupDir, file);
              
              // CHECKPOINT: Report file reading
              console.log(`Node ${nodeId}: Reading file ${file}`);
              
              fs.readFile(filePath, 'utf8', (err, data) => {
                if (err) {
                  console.error(`Node ${nodeId}: Error reading file ${file}: ${err.message}`);
                  filesProcessed++;
                  
                  if (filesProcessed === batch.length) {
                    // Batch complete
                    reduceResults = reduceResults.concat(batchResults);
                    fileIndex += batch.length;
                    
                    // CHECKPOINT: Report batch complete
                    console.log(`Node ${nodeId}: Completed file batch with ${batchResults.length} results`);
                    
                    // Process next batch with a small delay
                    setTimeout(processNextFileBatch, 100);
                  }
                  return;
                }
                
                try {
                  // Parse file content
                  const parsed = JSON.parse(data);
                  const keyData = util.deserialize(parsed);
                  
                  // CHECKPOINT: Report key processing
                  console.log(`Node ${nodeId}: Processing ${Object.keys(keyData).length} keys from file ${file}`);
                  
                  // Process each key
                  Object.keys(keyData).forEach(key => {
                    const values = keyData[key];
                    const valuesArray = Array.isArray(values) ? values : [values];
                    
                    try {
                      // Apply reducer
                      const result = reducer(key, valuesArray);
                      batchResults.push(result);
                    } catch (reduceError) {
                      console.error(`Node ${nodeId}: Error reducing key ${key}: ${reduceError.message}`);
                    }
                  });
                  
                  filesProcessed++;
                  
                  if (filesProcessed === batch.length) {
                    // Batch complete
                    reduceResults = reduceResults.concat(batchResults);
                    fileIndex += batch.length;
                    
                    // CHECKPOINT: Report batch complete
                    console.log(`Node ${nodeId}: Completed file batch with ${batchResults.length} results`);
                    
                    // Process next batch with a small delay
                    setTimeout(processNextFileBatch, 100);
                  }
                } catch (parseError) {
                  console.error(`Node ${nodeId}: Error parsing file ${file}: ${parseError.message}`);
                  filesProcessed++;
                  
                  if (filesProcessed === batch.length) {
                    // Batch complete despite error
                    reduceResults = reduceResults.concat(batchResults);
                    fileIndex += batch.length;
                    
                    // Process next batch
                    setTimeout(processNextFileBatch, 100);
                  }
                }
              });
            });
          }
          
          // Start processing the first batch of files
          processNextFileBatch();
        });
      });
    };

    // Implementation of bulk_append
    const bulk_append = (data, callback) => {
      const entries = data.entries;
      const jid = data.jid;
      const gid = data.gid || 'local';
      
      const nodeId = global.nodeConfig.port;
      
      // CHECKPOINT: Receiving entries
      console.log(`Node ${nodeId}: Received ${entries.length} entries for bulk append`);
      
      // Group entries by key to reduce file operations
      const entriesByKey = {};
      
      entries.forEach(entry => {
        const key = entry.key;
        
        if (!entriesByKey[key]) {
          entriesByKey[key] = [];
        }
        
        entriesByKey[key].push(entry.entry[key]);
      });
      
      const keys = Object.keys(entriesByKey);
      
      // CHECKPOINT: Processing grouped entries
      console.log(`Node ${nodeId}: Processing ${keys.length} unique keys`);
      
      // Store in separate files for each key
      const nodeConfig = global.nodeConfig;
      const nodeID = util.id.getNID(nodeConfig);
      const groupDir = path.join('store', nodeID, gid);
      
      // Create directory if needed
      fs.mkdirSync(groupDir, { recursive: true });
      
      let keysProcessed = 0;
      
      keys.forEach(key => {
        // Use key-specific files for better performance
        const keyHash = distribution.util.id.getID(key).substring(0, 8);
        const keyFileName = `${shuffleResultName}_${keyHash}.json`;
        const keyFilePath = path.join(groupDir, keyFileName);
        
        try {
          let keyData = {};
          
          // Read existing data if file exists
          if (fs.existsSync(keyFilePath)) {
            const fileContent = fs.readFileSync(keyFilePath, 'utf8');
            keyData = JSON.parse(fileContent);
            keyData = util.deserialize(keyData);
          }
          
          // Update with new values
          if (!keyData[key]) {
            // First entry for this key
            const values = entriesByKey[key];
            keyData[key] = values.length === 1 ? values[0] : values;
          } else if (Array.isArray(keyData[key])) {
            // Already have array of values, append new ones
            keyData[key] = keyData[key].concat(entriesByKey[key]);
          } else {
            // Have a single value, convert to array with new values
            keyData[key] = [keyData[key], ...entriesByKey[key]];
          }
          
          // Write back
          const serialized = util.serialize(keyData);
          fs.writeFileSync(keyFilePath, JSON.stringify(serialized));
          
          keysProcessed++;
          
          if (keysProcessed === keys.length) {
            // CHECKPOINT: All keys processed
            console.log(`Node ${nodeId}: Completed bulk append of ${entries.length} entries`);
            callback(null, { processed: entries.length });
          }
        } catch (error) {
          console.error(`Node ${nodeId}: Error processing key ${key}: ${error.message}`);
          keysProcessed++;
          
          if (keysProcessed === keys.length) {
            // All keys processed despite errors
            callback(null, { processed: entries.length });
          }
        }
      });
    };

    // Create service object with all methods
    let mrServiceObject = {
      notify: distribution.util.wire.createRPC(distribution.util.wire.toAsync(notify)),
      map: map,
      mapper: mapper,
      reducer: reducer,
      shuffle: shuffle,
      reduce: reduce,
      bulk_append: bulk_append
    };
    
    // CHECKPOINT: Register service
    console.log(`CHECKPOINT: Registering MapReduce service ${mrServiceName}`);
    logMemoryUsage("SERVICE_REGISTRATION");
    
    distribution[context.gid].routes.put(mrServiceObject, mrServiceName, (err, res) => {
      if (err) {
        console.error(`Error registering service: ${err.message}`);
        cb(err, null);
        return;
      }
      
      console.log(`CHECKPOINT: Service registered, starting MapReduce job ${mrId}`);
      
      const setupConfig = {
          gid: context.gid,
          jid: mrServiceName,
          keys: keys
      };
      const message = [setupConfig];
      
      distribution[context.gid].comm.send(message, {gid: 'local', service: mrServiceName, method: 'map'}, (e, v) => {
        if (e) {
          console.error(`Error starting map phase: ${e.message}`);
        }
      });
    });
  }

  return { exec };
}