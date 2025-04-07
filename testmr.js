const mem = require("./mem");

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
    const checkPointID = configuration.checkPointID || mrId; // Fallback to mrServiceName if no checkpoint ID provided

    // Create checkpoint manager
    const checkpointManager = {
      saveInterval: configuration.checkpoint_interval || 5, // Save every N batches
      enabled: configuration.enable_checkpoints !== false, // Enable by default
      lastSaveTime: Date.now(),
      checkpointPath: `./checkpoints/mr_${mrId}`,
      oldCheckpointPath: `./checkpoints/${checkPointID}`, // Fallback for old checkpoints
      partialResults: [],
      
      // Save checkpoint with current state and partial results
      save: function(state, partialResults, callback) {
        if (!this.enabled) return callback && callback();
        
        // Create checkpoint directory if it doesn't exist
        const checkpointDir = require('path').dirname(this.checkpointPath);
        if (!require('fs').existsSync(checkpointDir)) {
          require('fs').mkdirSync(checkpointDir, { recursive: true });
        }
        
        const checkpoint = {
          state: JSON.parse(JSON.stringify(state)), // Clone state
          keyTrackingMap: JSON.parse(JSON.stringify(keyTrackingMap)), // Clone tracking data
          partialResults: partialResults || [],
          timestamp: Date.now()
        };
        
        console.log(`[MR-${mrId}] Saving checkpoint at batch ${state.batch_num}/${state.num_batches}`);
        
        // Save to file
        require('fs').writeFile(
          `${this.checkpointPath}_state.json`, 
          JSON.stringify(checkpoint, null, 2), 
          (err) => {
            if (err) {
              console.error(`[MR-${mrId}] Error saving checkpoint state: ${err.message}`);
            } else {
              this.lastSaveTime = Date.now();
              console.log(`[MR-${mrId}] Checkpoint saved successfully`);
            }
            if (callback) callback(err);
          }
        );
        
        // For large result sets, save separately to avoid memory issues
        if (partialResults && partialResults.length > 0) {
          require('fs').writeFile(
            `${this.checkpointPath}_results.json`,
            JSON.stringify({ results: partialResults }, null, 2),
            (err) => {
              if (err) {
                console.error(`[MR-${mrId}] Error saving results: ${err.message}`);
              }
            }
          );
        }
      },
      
      // Load checkpoint
      load: function(callback) {
        if (!this.enabled) return callback && callback(null, null);
        
        // Check if checkpoint exists
        if (!require('fs').existsSync(`${this.oldCheckpointPath}_state.json`)) {
          return callback && callback(new Error('No checkpoint found'), null);
        }
        
        console.log(`[MR-${mrId}] Loading checkpoint...`);
        
        // Load state
        require('fs').readFile(`${this.oldCheckpointPath}_state.json`, 'utf8', (err, data) => {
          if (err) {
            console.error(`[MR-${mrId}] Error loading checkpoint state: ${err.message}`);
            return callback && callback(err, null);
          }
          
          try {
            const checkpoint = JSON.parse(data);
            console.log(`[MR-${mrId}] Checkpoint loaded from ${new Date(checkpoint.timestamp).toLocaleString()}`);
            
            // Check if results file exists
            if (require('fs').existsSync(`${this.oldCheckpointPath}_results.json`)) {
              require('fs').readFile(`${this.oldCheckpointPath}_results.json`, 'utf8', (err, resultsData) => {
                if (err) {
                  console.error(`[MR-${mrId}] Error loading results: ${err.message}`);
                  return callback && callback(null, checkpoint);
                }
                
                try {
                  const resultsObj = JSON.parse(resultsData);
                  checkpoint.partialResults = resultsObj.results || [];
                  callback && callback(null, checkpoint);
                } catch (parseErr) {
                  console.error(`[MR-${mrId}] Error parsing results: ${parseErr.message}`);
                  callback && callback(null, checkpoint);
                }
              });
            } else {
              callback && callback(null, checkpoint);
            }
          } catch (parseErr) {
            console.error(`[MR-${mrId}] Error parsing checkpoint: ${parseErr.message}`);
            callback && callback(parseErr, null);
          }
        });
      },
      
      // Clear checkpoint
      clear: function(callback) {
        if (!this.enabled) return callback && callback();
        
        console.log(`[MR-${mrId}] Clearing checkpoint...`);
        
        // Remove state file
        if (require('fs').existsSync(`${this.checkpointPath}_state.json`)) {
          require('fs').unlinkSync(`${this.checkpointPath}_state.json`);
        }
        
        // Remove results file
        if (require('fs').existsSync(`${this.checkpointPath}_results.json`)) {
          require('fs').unlinkSync(`${this.checkpointPath}_results.json`);
        }
        
        callback && callback();
      },
      
      // Check if it's time to save based on interval or memory pressure
      shouldSave: function(state) {
        if (!this.enabled) return false;
        
        // Check time interval (at least 2 minutes between saves)
        const timeBasedSave = (Date.now() - this.lastSaveTime) > 120000;
        
        // Check batch-based interval
        const batchBasedSave = state.batch_num % this.saveInterval === 0;
        
        // Check memory pressure (save if memory usage is high)
        const memUsage = process.memoryUsage();
        const memPressure = (memUsage.heapUsed / memUsage.heapTotal) > 0.7;
        
        return timeBasedSave || batchBasedSave || memPressure;
      }
    };

    // Configuration parameters
    const BATCH_SIZE = configuration.batch_size || 10;

    let results = [];

    const num_batches = Math.ceil(keys.length / BATCH_SIZE);
    
    console.log(`[MR-${mrId}] Starting MapReduce job with ${keys.length} keys, ${num_batches} batches, batch size: ${BATCH_SIZE}`);

    // NEW: Tracking structures for key processing
    const keyTrackingMap = {
      total: keys.length,
      batches: {},
      nodeStats: {},
      processedKeys: new Set(),
      mapPhaseStats: {
        totalKeysProcessed: 0,
        totalDuration: 0,
        batchStats: {}
      },
      shufflePhaseStats: {
        totalEntries: 0,
        totalDuration: 0,
        batchStats: {}
      },
      reducePhaseStats: {
        totalKeysProcessed: 0,
        totalDuration: 0,
        batchStats: {}
      }
    };

    let state_dict = {
      phase: "MAP",
      phase_count: 0,
      batch_num: 0,
      num_batches: num_batches,
      empty_batch_count: 0,
      empty_batch_threshold: 3,
      total_processed_keys: 0,
      batch_start_time: Date.now(),
      job_start_time: Date.now(),
    };
    
    // Log memory usage at start of job
    const memUsage = process.memoryUsage();
    console.log(`[MR-${mrId}] Initial memory usage: heap=${Math.round(memUsage.heapUsed/1024/1024)}MB, total=${Math.round(memUsage.heapTotal/1024/1024)}MB`);

    /**
     * Notify function - runs on coordinator node
     */
    const notify = (config, callback) => {
      const phase_map = {
        MAP: "SHUFFLE",
        SHUFFLE: "REDUCE",
        REDUCE: "MAP"
      };
      
      if (config.status === "ERROR") {
        console.error(`[MR-${mrId}] Error in phase ${config.phase}: ${config.error}`);
        callback(Error(config.error), null);
        return;
      } 

      function logPrefix(phase, nodeId = global.nodeConfig.port, subBatch = null) {
        const batchInfo = `BATCH:${state_dict.batch_num}/${state_dict.num_batches}`;
        const subBatchInfo = subBatch ? `[SUB:${subBatch}]` : '';
        return `[MR-${mrId}][${phase}][NODE:${nodeId}][${batchInfo}]${subBatchInfo}`;
      }

      // **Final aggregation function** – loads each batch's results in sequence to build final output
      function aggregateFinalResults(done) {
          if (!state_dict.resultMetadata || state_dict.resultMetadata.length === 0) {
              // No results (job ended early or no data)
              return done(null, []);
          }
          console.log(`[MR-${mrId}] Aggregating results from ${state_dict.resultMetadata.length} batches...`);
          const aggregatedTerms = new Map();  // using a Map to merge results by term (to avoid duplicates)
          let totalBatches = state_dict.resultMetadata.length;
          let batchesProcessed = 0;
          let termsProcessed = 0;

          // Process one batch at a time to keep memory usage low
          const processBatch = (index) => {
              if (index >= totalBatches) {
                  // All batch files processed – convert Map to array for final output
                  const finalResults = Array.from(aggregatedTerms.values());
                  // Force GC before returning (cleanup any leftover memory)
                  if (global.gc) global.gc();
                  return done(null, finalResults);
              }

              const batchMeta = state_dict.resultMetadata[index];
              const batchKey = String(batchMeta.key);
              console.log(`[MR-${mrId}] Loading batch ${index+1}/${totalBatches} from disk (key=${batchKey}, items=${batchMeta.count})...`);
              // Retrieve this batch's results from the disk store
              const retrieveStart = Date.now();
              distribution[context.gid].store.get(batchKey, (err, batchResults) => {
                  if (err) {
                      console.error(`[MR-${mrId}] Error reading batch ${batchMeta.batch} (${batchKey}): ${err.message}`);
                      // Skip this batch on error and continue
                      batchesProcessed++;
                      return setTimeout(() => processBatch(index+1), 0);
                  }
                  const loadTime = Date.now() - retrieveStart;
                  console.log(`[MR-${mrId}] Loaded ${batchResults.length} results from batch ${batchMeta.batch} in ${loadTime} ms`);
                  if (!batchResults || batchResults.length === 0) {
                      // Skip empty batch (should not normally happen unless empty batch)
                      batchesProcessed++;
                      return setTimeout(() => processBatch(index+1), 0);
                  }

                  // Merge this batch's results into the aggregated Map
                  for (const result of batchResults) {
                      termsProcessed++;
                      const termKey = result.word || result.key || result.term;
                      if (!termKey) continue;  // skip if result object is malformed
                      if (aggregatedTerms.has(termKey)) {
                          // Term already seen – merge new data into existing entry
                          const existing = aggregatedTerms.get(termKey);
                          // Example merge: combine document lists or scores, avoiding duplicates
                          if (result.scores && Array.isArray(result.scores)) {
                              if (!existing.scores) existing.scores = [];
                              // Use a map to ensure unique docs
                              const existingDocs = new Set(existing.scores.map(s => s.docId));
                              for (const score of result.scores) {
                                  if (score.docId && !existingDocs.has(score.docId)) {
                                      existing.scores.push(score);
                                  }
                                  // If docId already exists, you might update with max TF-IDF, etc.
                              }
                          }
                          // Update any other fields if needed (e.g., documentFrequency)
                          if (result.documentFrequency) {
                              existing.documentFrequency = Math.max(existing.documentFrequency || 0, result.documentFrequency);
                          }
                          aggregatedTerms.set(termKey, existing);
                      } else {
                          // First time seeing this term – add directly
                          aggregatedTerms.set(termKey, result);
                      }
                  }

                  // Delete the batch file from disk now that it's aggregated, to free disk space
                  distribution[context.gid].store.del(batchKey, (delErr) => {
                      if (delErr) {
                          console.warn(`[MR-${mrId}] Warning: could not delete ${batchKey} from store: ${delErr.message}`);
                      } else {
                          console.log(`[MR-${mrId}] Deleted temp file for batch ${batchMeta.batch}`);
                      }
                  });

                  batchesProcessed++;
                  // Monitor memory and force GC if needed between batches
                  const mem = process.memoryUsage();
                  const usedPercent = mem.heapUsed / mem.heapTotal;
                  if (usedPercent > 0.85 && global.gc) {
                      console.log(`[MR-${mrId}] High memory usage (${Math.round(usedPercent*100)}%), forcing GC...`);
                      global.gc();
                      const memAfterGC = process.memoryUsage();
                      console.log(`[MR-${mrId}] Memory after GC: ${Math.round(memAfterGC.heapUsed/1024/1024)}MB (freed ${Math.round((mem.heapUsed - memAfterGC.heapUsed)/1024/1024)}MB)`);
                  }

                  // Process next batch after a short delay to yield to event loop (prevent blocking)
                  setTimeout(() => processBatch(index+1), 0);
              });
          };

          processBatch(0);
      };
      
      // Get the local group node count
      distribution.local.groups.get(config.gid, (err, group) => {
        if (err) {
          console.error(`[MR-${mrId}] Error getting group nodes: ${err.message}`);
          callback(err, null);
          return;
        }
        let groupNodeCount = Object.keys(group).length;

        // Increment the counter for responses received
        state_dict.phase_count = state_dict.phase_count + 1;

        if (config.phase !== state_dict.phase) {
          console.error(`[MR-${mrId}] Phase mismatch. Expected ${state_dict.phase}, got ${config.phase}`);
          callback(
            Error(
              `Error: Phase mismatch. Expected ${state_dict.phase}, got ${config.phase}`
            ),
            null
          );
          return;
        }

        // Track node statistics
        const nodeId = config.nodeId || "unknown";
        if (!keyTrackingMap.nodeStats[nodeId]) {
          keyTrackingMap.nodeStats[nodeId] = {
            mapsProcessed: 0,
            keysProcessed: 0,
            emptyBatches: 0,
            shufflesProcessed: 0,
            reducesProcessed: 0
          };
        }

        // Track processing stats for each phase
        if (config.phase === "MAP") {
          if (config.processedKeys && Array.isArray(config.processedKeys)) {
            keyTrackingMap.nodeStats[nodeId].mapsProcessed++;
            keyTrackingMap.nodeStats[nodeId].keysProcessed += config.processedKeys.length;
            
            // Add all processed keys to the global set
            config.processedKeys.forEach(key => keyTrackingMap.processedKeys.add(key));
            
            // Update map phase statistics
            if (!keyTrackingMap.mapPhaseStats.batchStats[state_dict.batch_num]) {
              keyTrackingMap.mapPhaseStats.batchStats[state_dict.batch_num] = {
                keysProcessed: 0,
                nodeContributions: {}
              };
            }
            keyTrackingMap.mapPhaseStats.batchStats[state_dict.batch_num].keysProcessed += config.processedKeys.length;
            keyTrackingMap.mapPhaseStats.batchStats[state_dict.batch_num].nodeContributions[nodeId] = config.processedKeys.length;
            keyTrackingMap.mapPhaseStats.totalKeysProcessed += config.processedKeys.length;
          }
          
          if (config.noKeysToProcess) {
            keyTrackingMap.nodeStats[nodeId].emptyBatches++;
          }
        } else if (config.phase === "SHUFFLE") {
          keyTrackingMap.nodeStats[nodeId].shufflesProcessed++;
          
          // Update shuffle phase statistics
          if (config.shuffleStats) {
            if (!keyTrackingMap.shufflePhaseStats.batchStats[state_dict.batch_num]) {
              keyTrackingMap.shufflePhaseStats.batchStats[state_dict.batch_num] = {
                entriesProcessed: 0,
                nodeContributions: {}
              };
            }
            keyTrackingMap.shufflePhaseStats.batchStats[state_dict.batch_num].entriesProcessed += config.shuffleStats.entriesProcessed || 0;
            keyTrackingMap.shufflePhaseStats.batchStats[state_dict.batch_num].nodeContributions[nodeId] = config.shuffleStats.entriesProcessed || 0;
            keyTrackingMap.shufflePhaseStats.totalEntries += config.shuffleStats.entriesProcessed || 0;
          }
        } else if (config.phase === "REDUCE") {
          keyTrackingMap.nodeStats[nodeId].reducesProcessed++;
          
          // Update reduce phase statistics
          if (config.reduceStats) {
            if (!keyTrackingMap.reducePhaseStats.batchStats[state_dict.batch_num]) {
              keyTrackingMap.reducePhaseStats.batchStats[state_dict.batch_num] = {
                keysProcessed: 0,
                nodeContributions: {}
              };
            }
            keyTrackingMap.reducePhaseStats.batchStats[state_dict.batch_num].keysProcessed += config.reduceStats.keysProcessed || 0;
            keyTrackingMap.reducePhaseStats.batchStats[state_dict.batch_num].nodeContributions[nodeId] = config.reduceStats.keysProcessed || 0;
            keyTrackingMap.reducePhaseStats.totalKeysProcessed += config.reduceStats.keysProcessed || 0;
          }
        }

        // Track nodes with no keys to process
        if (config.noKeysToProcess) {
          if (!state_dict.noKeysNodes) {
            state_dict.noKeysNodes = new Set();
          }
          state_dict.noKeysNodes.add(nodeId);
        }

        // Log progress
        console.log(
          `[MR-${mrId}] Node ${global.nodeConfig.port}: Received notification for phase ${config.phase}. Current count: ${state_dict.phase_count}/${groupNodeCount}, batch: ${state_dict.batch_num}/${state_dict.num_batches}. Memory usage: heap=${Math.round(process.memoryUsage().heapUsed/1024/1024)}MB, total=${Math.round(process.memoryUsage().heapTotal/1024/1024)}MB, percent used: ${(process.memoryUsage().heapUsed / process.memoryUsage().heapTotal * 100).toFixed(2)}%`
        );
        
        // Collect reduce results
        if (state_dict.phase === "REDUCE" && config.results) {
          console.log(`${logPrefix('REDUCE-COLLECT')} Collecting results from ${config.nodeId}. Received ${config.results.length} results.`);
          console.log(`[MR-${mrId}] Node ${global.nodeConfig.port}: Memory usage during reduce phase: heap=${Math.round(process.memoryUsage().heapUsed/1024/1024)}MB, total=${Math.round(process.memoryUsage().heapTotal/1024/1024)}MB`);
          
          // Create a unique string key for this batch of results
          const batchResultsKey = `batch_results_${state_dict.batch_num}_${config.nodeId}`;
          
          // Initialize resultMetadata array if it doesn't exist
          if (!state_dict.resultMetadata) {
            state_dict.resultMetadata = [];
          }
          
          // Store this batch's results to disk
          // console.log("[MR-${mrId}] Saving batch results to storage with key:", batchResultsKey, config.results.length, "results");
          distribution[context.gid].store.put(config.results, batchResultsKey, (err, val) => {
            if (err) {
              console.error(`[MR-${mrId}] Error saving batch results: ${err.message}`);
            } else {
              // Record metadata about this batch
              state_dict.resultMetadata.push({
                batch: state_dict.batch_num,
                nodeId: config.nodeId,
                count: config.results.length,
                key: batchResultsKey,
                timestamp: Date.now()
              });

              if (state_dict.resultMetadata && state_dict.resultMetadata.length > 50) {
                 // Keep only the most recent 50 batches in memory
                const recentBatches = state_dict.resultMetadata.slice(-50);
                
                // Log the pruning operation
                console.log(`${logPrefix('METADATA-PRUNE')} Pruning metadata from ${state_dict.resultMetadata.length} to ${recentBatches.length} entries`);
                
                // Replace with the pruned list
                state_dict.resultMetadata = recentBatches;
                
                // Force garbage collection
                if (global.gc) {
                  global.gc();
                  console.log(`[MR-${mrId}] Memory after metadata pruning: heap=${Math.round(process.memoryUsage().heapUsed/1024/1024)}MB`);
                }
              }
              console.log(`${logPrefix('REDUCE-STORE', global.nodeConfig.port)} Saved ${config.results.length} results from node ${config.nodeId} to storage`);
              // console.log(`[MR-${mrId}] Total batches saved: ${state_dict.resultMetadata.length}/${state_dict.batch_num * 4}, total items stored: ${state_dict.resultMetadata.reduce((sum, meta) => sum + meta.count, 0)}`);
            }
          });
          
          // Track total items stored for logging purposes only
          if (!state_dict.totalStoredResults) {
            state_dict.totalStoredResults = 0;
          }
          state_dict.totalStoredResults += config.results.length;
          console.log(`[MR-${mrId}] Total stored results so far: ${state_dict.totalStoredResults}`);
        }

        // When all nodes have responded for the current phase
        if (state_dict.phase_count === groupNodeCount) {
          // Calculate phase duration
          const phaseDuration = Date.now() - state_dict.batch_start_time;
          console.log(`${logPrefix(state_dict.phase + '-COMPLETE')} Phase complete in ${phaseDuration}ms. Received ${state_dict.phase_count}/${groupNodeCount} responses.`);
          
          // Update phase duration statistics
          if (state_dict.phase === "MAP") {
            keyTrackingMap.mapPhaseStats.totalDuration += phaseDuration;
          } else if (state_dict.phase === "SHUFFLE") {
            keyTrackingMap.shufflePhaseStats.totalDuration += phaseDuration;
          } else if (state_dict.phase === "REDUCE") {
            keyTrackingMap.reducePhaseStats.totalDuration += phaseDuration;
          }
          
          // Handle phase completion
          if (state_dict.phase === "REDUCE") {
            // Calculate batch duration
            const batchDuration = Date.now() - state_dict.batch_start_time;
            console.log(`${logPrefix('BATCH-COMPLETE')} Batch completed in ${batchDuration}ms with ${state_dict.totalStoredResults || 0} total results stored.`);
            
            // Track batch statistics
            keyTrackingMap.batches[state_dict.batch_num] = {
              duration: batchDuration,
              mapKeys: keyTrackingMap.mapPhaseStats.batchStats[state_dict.batch_num]?.keysProcessed || 0,
              shuffleEntries: keyTrackingMap.shufflePhaseStats.batchStats[state_dict.batch_num]?.entriesProcessed || 0,
              reduceKeys: keyTrackingMap.reducePhaseStats.batchStats[state_dict.batch_num]?.keysProcessed || 0,
              resultCount: config.results ? config.results.length : 0
            };
            
            // Log batch results
            // console.log(`Results collected so far: ${results.length}`);
            const batchResults = config.results || [];
            console.log(`Terms in this batch: ${batchResults.length}`);
            
            // Calculate duplicate terms
            const termCounts = {};
            batchResults.forEach(result => {
              if (result.word) {
                termCounts[result.word] = (termCounts[result.word] || 0) + 1;
              }
            });
            const duplicates = Object.keys(termCounts).filter(term => termCounts[term] > 1).length;
            console.log(`Duplicate terms in this batch: ${duplicates}`);
            
            // Increment the batch counter
            state_dict.batch_num = state_dict.batch_num + 1;
            
            // Check if all nodes reported no keys
            const allNodesHaveNoKeys = state_dict.noKeysNodes && 
                                      state_dict.noKeysNodes.size === groupNodeCount;
            
            if (allNodesHaveNoKeys) {
              state_dict.empty_batch_count++;
              console.log(`[MR-${mrId}] All nodes reported no keys to process for batch ${state_dict.batch_num-1}. Empty batch count: ${state_dict.empty_batch_count}`);
              
              // Early termination if we've hit the threshold of empty batches
              if (state_dict.empty_batch_count >= state_dict.empty_batch_threshold) {
                console.log(`[MR-${mrId}] Reached ${state_dict.empty_batch_threshold} consecutive empty batches. Terminating job early.`);
                
                // Print key processing summary
                const totalProcessed = keyTrackingMap.processedKeys.size;
                const coverage = (totalProcessed / keyTrackingMap.total) * 100;
                
                console.log(`
    [MR-${mrId}] KEY PROCESSING SUMMARY:
    - Total keys: ${keyTrackingMap.total}
    - Total processed: ${totalProcessed} (${coverage.toFixed(2)}%)
    - Keys not processed: ${keyTrackingMap.total - totalProcessed}
    - Batches completed: ${Object.keys(keyTrackingMap.batches).length}/${num_batches}

    Phase Statistics:
    - Map phase: ${keyTrackingMap.mapPhaseStats.totalKeysProcessed} keys processed in ${keyTrackingMap.mapPhaseStats.totalDuration}ms
    - Shuffle phase: ${keyTrackingMap.shufflePhaseStats.totalEntries} entries processed in ${keyTrackingMap.shufflePhaseStats.totalDuration}ms
    - Reduce phase: ${keyTrackingMap.reducePhaseStats.totalKeysProcessed} keys processed in ${keyTrackingMap.reducePhaseStats.totalDuration}ms

    Node Statistics:`);
                
                // Print per-node statistics
                Object.keys(keyTrackingMap.nodeStats).forEach(nodeId => {
                  const stats = keyTrackingMap.nodeStats[nodeId];
                  console.log(`- Node ${nodeId}: ${stats.keysProcessed} keys processed in ${stats.mapsProcessed} map operations, ${stats.emptyBatches} empty batches`);
                });
                
                // Calculate total job duration
                const jobDuration = Date.now() - state_dict.job_start_time;
                console.log(`[MR-${mrId}] MapReduce job completed early in ${jobDuration}ms. Final result size: ${results.length}`);
                
                // Start the final aggregation even though we're terminating early
                console.log(`[MR-${mrId}] Starting final aggregation despite early termination`);
                
                // Make sure to keep nodes alive during aggregation
                
                
                aggregateFinalResults((err, finalResults) => {
                  // clearInterval(keepAliveInterval);
                  
                  if (err) {
                    console.error(`[MR-${mrId}] Error in final aggregation: ${err.message}`);
                    cb(err, []);
                    return;
                  }
                  
                  // Clean up the service when done
                  distribution[context.gid].comm.send([config.jid], {service: 'routes', method: 'rem'}, (e, v) => {
                    console.log(`[MR-${mrId}] Job service removed, returning ${finalResults.length} results`);
                    cb(null, finalResults);
                    return;
                  });
                });
                return;
              }
            } else {
              // Reset empty batch counter if we found data
              state_dict.empty_batch_count = 0;
            }

            // Save checkpoint if needed
            if (checkpointManager.shouldSave(state_dict)) {
              checkpointManager.save(state_dict, results, (err) => {
                if (err) {
                  console.error(`[MR-${mrId}] Failed to save checkpoint: ${err.message}`);
                }
              });
            }
            
            // Reset noKeysNodes for next batch
            state_dict.noKeysNodes = new Set();
            
            // Check if all batches are processed
            if (state_dict.batch_num >= state_dict.num_batches) {
              // Print key processing summary
              const totalProcessed = keyTrackingMap.processedKeys.size;
              const coverage = (totalProcessed / keyTrackingMap.total) * 100;
              
              console.log(`
    [MR-${mrId}] FINAL KEY PROCESSING SUMMARY:
    - Total keys: ${keyTrackingMap.total}
    - Total processed: ${totalProcessed} (${coverage.toFixed(2)}%)
    - Keys not processed: ${keyTrackingMap.total - totalProcessed}
    - Batches completed: ${Object.keys(keyTrackingMap.batches).length}/${num_batches}

    Phase Statistics:
    - Map phase: ${keyTrackingMap.mapPhaseStats.totalKeysProcessed} keys processed in ${keyTrackingMap.mapPhaseStats.totalDuration}ms
    - Shuffle phase: ${keyTrackingMap.shufflePhaseStats.totalEntries} entries processed in ${keyTrackingMap.shufflePhaseStats.totalDuration}ms
    - Reduce phase: ${keyTrackingMap.reducePhaseStats.totalKeysProcessed} keys processed in ${keyTrackingMap.reducePhaseStats.totalDuration}ms

    Node Statistics:`);
              
              // Print per-node statistics
              Object.keys(keyTrackingMap.nodeStats).forEach(nodeId => {
                const stats = keyTrackingMap.nodeStats[nodeId];
                console.log(`- Node ${nodeId}: ${stats.keysProcessed} keys processed in ${stats.mapsProcessed} map operations, ${stats.emptyBatches} empty batches`);
              });
              
              // Calculate total job duration
              const jobDuration = Date.now() - state_dict.job_start_time;
              console.log(`[MR-${mrId}] Node ${global.nodeConfig.port}: All batches completed in ${jobDuration}ms. Total stored results: ${state_dict.totalStoredResults || 0}`);
              
              // Memory usage at end of job
              const endMemUsage = process.memoryUsage();
              console.log(`[MR-${mrId}] Final memory usage: heap=${Math.round(endMemUsage.heapUsed/1024/1024)}MB, total=${Math.round(endMemUsage.heapTotal/1024/1024)}MB`);
              
              checkpointManager.clear();
              
              

              // Start the final aggregation
              console.log(`[MR-${mrId}] Starting final aggregation of ${state_dict.resultMetadata?.length || 0} result batches`);
              aggregateFinalResults((err, finalResults) => {
                // clearInterval(keepAliveInterval);
                
                if (err) {
                  console.error(`[MR-${mrId}] Error in final aggregation: ${err.message}`);
                  cb(err, []);
                  return;
                }
                
                console.log(`[MR-${mrId}] Final aggregation completed successfully with ${finalResults.length} total results`);
                
                // Clean up the service when done - delay to ensure all nodes have finished
                setTimeout(() => {
                  distribution[context.gid].comm.send([config.jid], {service: 'routes', method: 'rem'}, (e, v) => {
                    console.log(`[MR-${mrId}] Job service removed, returning ${finalResults.length} results`);
                    cb(null, finalResults);
                  });
                }, 2000);
              });
            } else {
              // Reset for next batch
              state_dict.phase_count = 0;
              state_dict.phase = "MAP";
              state_dict.batch_start_time = Date.now();
              
              console.log(`[MR-${mrId}] Node ${global.nodeConfig.port}: Starting next batch: ${state_dict.batch_num}/${state_dict.num_batches}`);

              

              if (state_dict.batch_num % 5 === 0) {
                // Want to timeout the next batch to allow for memory cleanup and garbage collection
                console.log(`[MR-${mrId}] Node ${global.nodeConfig.port}: Pausing for 5 seconds to allow memory cleanup before next batch`);
                console.log(`[MR-${mrId}] Node ${global.nodeConfig.port}: Current memory usage: heap=${Math.round(process.memoryUsage().heapUsed/1024/1024)}MB, total=${Math.round(process.memoryUsage().heapTotal/1024/1024)}MB`);

                setTimeout(() => {
                  // Trigger next batch map
                  distribution[context.gid].comm.send(["memory"], {service: "status", method: "get"}, (errMem, memInfo) => {
                    if (memInfo) {
                      Object.entries(memInfo).forEach(([node, memoryInfo]) => {
                        console.log(`[MR-${mrId}] Node ${node} memory before clear: heapUsed=${Math.round(memoryInfo.heapUsed/1024/1024)}MB, heapTotal=${Math.round(memoryInfo.heapTotal/1024/1024)}MB. Percent used: ${(memoryInfo.heapUsed / memoryInfo.heapTotal * 100).toFixed(2)}%`);
                      });
                    }
                    
                    distribution[context.gid].comm.send([{gid: context.gid}], {service: "mem", method: "clear"}, (err, val) => {
                      if (val && val.success) {
                        console.log(`[MR-${mrId}] Node ${global.nodeConfig.port}: Memory cleared successfully`);
                      } else {
                        console.error(`[MR-${mrId}] Node ${global.nodeConfig.port}: Error clearing memory:`, err);
                      }
                      
                      distribution[context.gid].comm.send(["memory"], {service: "status", method: "get"}, (errMem, memInfo) => {
                        if (memInfo) {
                          Object.entries(memInfo).forEach(([node, memoryInfo]) => {
                            console.log(`[MR-${mrId}] Node ${node} memory after clear: heapUsed=${Math.round(memoryInfo.heapUsed/1024/1024)}MB, heapTotal=${Math.round(memoryInfo.heapTotal/1024/1024)}MB. Percent used: ${(memoryInfo.heapUsed / memoryInfo.heapTotal * 100).toFixed(2)}%`);
                          });
                        }

                        // Proceed with the next batch after memory cleanup
                        config.results = [];
                        
                        
                        const setupConfig = {
                          gid: context.gid,
                          jid: mrServiceName,
                          keys: keys,
                          batch_num: state_dict.batch_num,
                          batch_size: BATCH_SIZE
                        };
                        
                        distribution[context.gid].comm.send([setupConfig], {gid: 'local', service: mrServiceName, method: 'map'}, (e, v) => {
                          // No callback handling needed here
                        });
                      });
                    });
                  });
                }, 5000);
              } else {
                // Trigger next batch map
                const setupConfig = {
                  gid: context.gid,
                  jid: mrServiceName,
                  keys: keys,
                  batch_num: state_dict.batch_num,
                  batch_size: BATCH_SIZE
                };
                
                distribution[context.gid].comm.send([setupConfig], {gid: 'local', service: mrServiceName, method: 'map'}, (e, v) => {
                  // No callback handling needed here
                });
              }
            }
          } else {
            // Move to the next phase in the current batch
            let new_phase = phase_map[state_dict.phase];
            state_dict.phase = new_phase;
            state_dict.phase_count = 0;
            state_dict.batch_start_time = Date.now(); // Reset time for new phase
            
            console.log(`[MR-${mrId}] Node ${global.nodeConfig.port}: Moving to phase ${new_phase} for job: ${config.jid}, batch: ${state_dict.batch_num}/${state_dict.num_batches}`);
            
            let method = state_dict.phase.toLowerCase();
            let phaseConfig = {
              gid: config.gid,
              jid: config.jid
            };
            
            distribution[context.gid].comm.send([phaseConfig], {service: config.jid, method: method}, (err, val) => {
              // No callback handling needed here
            });
          }
        }
      });
    };

    /**
     * Map function with enhanced key tracking
     */
    const map = (config, callback) => {
      const gid = config.gid;
      const job_id = config.jid;
      const BATCH_SIZE = config.batch_size || 10;
      const batch_num = config.batch_num || 0;
      const nodeId = global.nodeConfig.port; // Get current node ID for tracking

      // Performance metrics
      const mapStartTime = Date.now();
      const initialMemUsage = process.memoryUsage();
      console.log(`[MR-${job_id}] Node ${nodeId}: Starting map phase. Memory: heap=${Math.round(initialMemUsage.heapUsed/1024/1024)}MB`);

      distribution.local.routes.get(job_id, (err, service) => {
        if (err) {
          console.error(`[MR-${job_id}] Node ${nodeId}: Error getting service: ${err.message}`);
          callback(err, null);
          return;
        }

        const mapper = service.mapper;
        const storageService = distribution.local.mem;
        
        // Get all keys first
        distribution.local.store.get({gid: gid, key: null}, (err, localKeys) => {
          if (err) {
            console.error(`[MR-${job_id}] Node ${nodeId}: Error retrieving keys for gid ${gid}: ${err.message}`);
            callback(err, null);
            return;
          }

          let filteredKeys = localKeys.filter(key => !key.includes('.DS_Store'));
          console.log(`[MR-${job_id}] Node ${nodeId}: Found ${filteredKeys.length} total keys on this node`);

          const startIdx = batch_num * BATCH_SIZE;
          const endIdx = Math.min(startIdx + BATCH_SIZE, filteredKeys.length);

          let batchKeys = filteredKeys.slice(startIdx, endIdx);
          
          // NEW: Create array to track processed keys
          const processedKeys = [];
          
          if (batchKeys.length === 0) {
            console.log(`[MR-${job_id}] Node ${nodeId}: No keys to process in batch ${batch_num}/${Math.ceil(filteredKeys.length/BATCH_SIZE)}. Proceeding to shuffle phase.`);
            
            storageService.put([], {key: "map@" + job_id, gid: gid}, (err, val) => {
              if (err) {
                callback(err, null);
                return;
              }
              service.notify({
                phase: "MAP", 
                status: "COMPLETED", 
                gid: gid, 
                jid: job_id,
                nodeId: nodeId,
                noKeysToProcess: true,
                processedKeys: processedKeys // Empty array in this case
              }, callback);
            });
            return;
          }

          console.log(`[MR-${job_id}] Node ${nodeId}: Processing ${batchKeys.length} keys in batch ${batch_num} (range ${startIdx}-${endIdx-1})`);
          
          // Process keys in batches
          let mapResults = [];
          let currentBatch = 0;
          const totalBatches = Math.ceil(batchKeys.length / 4);

          console.log(`[MR-${job_id}] Node ${nodeId}: Total sub-batches to process: ${totalBatches}, BATCH_SIZE: ${BATCH_SIZE}`);
          
          const processBatch = () => {
            const batchStartTime = Date.now();
            const bstartIdx = currentBatch * 4;
            const bendIdx = Math.min(bstartIdx + 4, batchKeys.length);
            const batchedKeys = batchKeys.slice(bstartIdx, bendIdx);
            
            console.log(`[MR-${job_id}] Node ${nodeId}: Processing sub-batch ${currentBatch + 1}/${totalBatches} with ${batchedKeys.length} keys`);
            
            let batchResults = [];
            let keysProcessed = 0;
            
            if (batchedKeys.length === 0) {
              // No more keys to process
              const mapResultName = "map@" + job_id;
              storageService.put(mapResults, {key: mapResultName, gid: gid}, (err, val) => {
                if (err) {
                  callback(err, null);
                  return;
                }
                
                const mapDuration = Date.now() - mapStartTime;
                const finalMemUsage = process.memoryUsage();
                console.log(`[MR-${job_id}] Node ${nodeId}: Map phase completed in ${mapDuration}ms. Results: ${mapResults.length}, Memory: heap=${Math.round(finalMemUsage.heapUsed/1024/1024)}MB`);
                
                service.notify({
                  phase: "MAP", 
                  status: "COMPLETED", 
                  gid: gid, 
                  jid: job_id,
                  nodeId: nodeId,
                  noKeysToProcess: false,
                  processedKeys: processedKeys,
                  keysProcessed: processedKeys.length
                }, callback);
              });
              return;
            }
            
            // Process each key in the batch
            batchedKeys.forEach(key => {
              distribution.local.store.get({key: key, gid: gid}, (err, val) => {
                if (err) {
                  // Just log the error and continue with other files
                  console.error(`[MR-${job_id}] Node ${nodeId}: Error processing ${key}: ${err.message}`);
                  keysProcessed++;
                } else {
                  try {
                    const mapStartMs = Date.now();
                    let res = mapper(key, val);
                    const mapDurationMs = Date.now() - mapStartMs;
                    
                    // NEW: Add to processed keys list
                    processedKeys.push(key);
                    
                    if (mapDurationMs > 500) { // Log slow mapper operations
                      console.log(`[MR-${job_id}] Node ${nodeId}: Slow mapper for key ${key}: ${mapDurationMs}ms`);
                    }
                    
                    if (!Array.isArray(res)) {
                      res = [res];
                    }
                    
                    batchResults = batchResults.concat(res);
                    
                  } catch (mapError) {
                    console.error(`[MR-${job_id}] Node ${nodeId}: Error mapping ${key}: ${mapError.message}`);
                  }
                  
                  keysProcessed++;
                }
                
                // Check if batch is complete
                if (keysProcessed === batchedKeys.length) {
                  // Add batch results to total results
                  const batchDuration = Date.now() - batchStartTime;
                  console.log(`[MR-${job_id}] Node ${nodeId}: Sub-batch ${currentBatch+1}/${totalBatches} completed in ${batchDuration}ms with ${batchResults.length} results`);
                  
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
                      
                      const mapDuration = Date.now() - mapStartTime;
                      const finalMemUsage = process.memoryUsage();
                      console.log(`[MR-${job_id}] Node ${nodeId}: Map phase completed in ${mapDuration}ms. Results: ${mapResults.length}, Processed keys: ${processedKeys.length}, Memory: heap=${Math.round(finalMemUsage.heapUsed/1024/1024)}MB, total=${Math.round(finalMemUsage.heapTotal/1024/1024)}MB (${Math.round((finalMemUsage.heapUsed/finalMemUsage.heapTotal)*100)}% used)`);
                      
                      service.notify({
                        phase: "MAP", 
                        status: "COMPLETED", 
                        gid: gid, 
                        jid: job_id,
                        nodeId: nodeId,
                        noKeysToProcess: false,
                        processedKeys: processedKeys,
                        keysProcessed: processedKeys.length
                      }, callback);
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

    /**
     * Shuffle function with enhanced tracking
     */
    /**
 * Shuffle function with comprehensive memory tracking and advanced logging
 */
const shuffle = (config, callback) => {
  const gid = config.gid;
  const jid = config.jid;
  // Reduce batch size to help with memory pressure
  const SHUFFLE_BATCH_SIZE = 500; // Reduced from 1000
  const nodeId = global.nodeConfig.port;
  
  // Memory tracking
  const shuffleStartTime = Date.now();
  const initialMemUsage = process.memoryUsage();
  console.log(`[MR-${jid}] Node ${nodeId}: SHUFFLE PHASE STARTING - Memory: heap=${Math.round(initialMemUsage.heapUsed/1024/1024)}MB/${Math.round(initialMemUsage.heapTotal/1024/1024)}MB (${Math.round((initialMemUsage.heapUsed/initialMemUsage.heapTotal)*100)}% used)`);
  
  // Detailed tracking variables
  let entriesProcessed = 0;
  let entriesSent = 0;
  let targetsFound = 0;
  let largestBatchSize = 0;
  let largestBatchTarget = '';
  const targetStats = {};
  const phaseTimings = {
    mapResultsRetrieval: 0,
    nodeTargeting: 0,
    batchSending: 0,
    total: 0
  };
  const memorySnapshots = {
    afterMapRetrieval: 0,
    afterNodeTargeting: 0,
    afterBatchSending: 0
  };
  
  // Track errors
  let errors = {
    count: 0,
    details: []
  };
  
  // Get the start time for map results retrieval
  const mapRetrievalStartTime = Date.now();
  
  distribution.local.routes.get(jid, (err, service) => {
    if (err) {
      console.error(`[MR-${jid}] Node ${nodeId}: Error getting service: ${err.message}`);
      callback(err, null);
      return;
    }
    
    // Get map results from memory
    const storageService = distribution.local.mem;
    const mapResultName = "map@" + jid;
    
    storageService.get({key: mapResultName, gid: gid}, (err, mapResults) => {
      // Track time to retrieve map results
      phaseTimings.mapResultsRetrieval = Date.now() - mapRetrievalStartTime;
      
      if (err) {
        console.error(`[MR-${jid}] Node ${nodeId}: Error retrieving map results: ${err.message}`);
        errors.count++;
        errors.details.push({phase: 'map_retrieval', error: err.message});
      }
      
      if (!mapResults || mapResults.length === 0) {
        console.log(`[MR-${jid}] Node ${nodeId}: No map results found for gid ${gid} and job ${jid}. Cannot shuffle.`);
        service.notify({
          phase: "SHUFFLE", 
          status: "COMPLETED", 
          gid: gid, 
          jid: jid,
          nodeId: nodeId,
          noKeysToProcess: true,
          shuffleStats: {
            entriesProcessed: 0,
            entriesSent: 0,
            targetsFound: 0,
            errors: errors
          }
        }, callback);
        return;
      }
      
      // Memory snapshot after retrieving map results
      const memAfterRetrieval = process.memoryUsage();
      memorySnapshots.afterMapRetrieval = memAfterRetrieval.heapUsed;
      
      console.log(`[MR-${jid}] Node ${nodeId}: Retrieved ${mapResults.length} map results in ${phaseTimings.mapResultsRetrieval}ms`);
      console.log(`[MR-${jid}] Node ${nodeId}: Memory after retrieval: heap=${Math.round(memAfterRetrieval.heapUsed/1024/1024)}MB (${Math.round((memAfterRetrieval.heapUsed - initialMemUsage.heapUsed)/1024/1024)}MB increase)`);
      
      // Sample and log the first few entries
      // const sampleSize = Math.min(2, mapResults.length);
      // if (sampleSize > 0) {
      //   console.log(`[MR-${jid}] Node ${nodeId}: Sample of map results (${sampleSize} entries):`);
      //   for (let i = 0; i < sampleSize; i++) {
      //     const keys = Object.keys(mapResults[i]);
      //     console.log(`  Entry ${i+1}: key=${keys[0]}, data size=${JSON.stringify(mapResults[i]).length} bytes`);
      //   }
      // }
      
      entriesProcessed = mapResults.length;
      
      // Start node targeting phase
      const nodeTargetingStartTime = Date.now();
      
      // Group results by target node to minimize network calls
      distribution.local.groups.get(gid, (err, groupNodes) => {
        if (err) {
          console.error(`[MR-${jid}] Node ${nodeId}: Error retrieving group nodes: ${err.message}`);
          errors.count++;
          errors.details.push({phase: 'group_retrieval', error: err.message});
          callback(err, null);
          return;
        }

        const groupNodeCount = Object.keys(groupNodes).length;
        console.log(`[MR-${jid}] Node ${nodeId}: Found ${groupNodeCount} group nodes for shuffling`);
        
        // Calculate target node for each key
        let nodeTargets = {};
        let keyCounts = {}; // For logging key distribution
        
        console.log(`[MR-${jid}] Node ${nodeId}: Starting to assign ${mapResults.length} entries to target nodes...`);
        const targetingBatchSize = 5000;
        const totalTargetingBatches = Math.ceil(mapResults.length / targetingBatchSize);
        
        // Process targeting in smaller batches to reduce GC pressure
        for (let batchIdx = 0; batchIdx < totalTargetingBatches; batchIdx++) {
          const batchStart = batchIdx * targetingBatchSize;
          const batchEnd = Math.min(batchStart + targetingBatchSize, mapResults.length);
          
          if (batchIdx > 0 && batchIdx % 5 === 0) {
            console.log(`[MR-${jid}] Node ${nodeId}: Processed ${batchIdx} targeting batches (${batchStart}/${mapResults.length} entries)`);
          }
          
          for (let i = batchStart; i < batchEnd; i++) {
            const entry = mapResults[i];
            const key = Object.keys(entry)[0];
            
            // Get node configs and IDs
            const nodeConfigs = Object.values(groupNodes);
            const nids = nodeConfigs.map((nc) => distribution.util.id.getNID(nc));
            
            // Hash the key to determine target node
            const kid = distribution.util.id.getID(key);
            const targetNID = distribution.util.id.consistentHash(kid, nids);
            const targetNode = nodeConfigs.find((nc) => distribution.util.id.getNID(nc) === targetNID);
            
            if (!targetNode) {
              console.error(`[MR-${jid}] Node ${nodeId}: No target node found for key ${key}`);
              continue;
            }
            
            const targetNodeId = distribution.util.id.getSID(targetNode);
            targetsFound++;
            
            // Track target statistics
            if (!targetStats[targetNodeId]) {
              targetStats[targetNodeId] = 0;
            }
            targetStats[targetNodeId]++;
            
            // Initialize arrays for this target
            if (!nodeTargets[targetNodeId]) {
              nodeTargets[targetNodeId] = [];
              keyCounts[targetNodeId] = 0;
            }
            
            // Add entry to target's batch
            nodeTargets[targetNodeId].push({
              key: key,
              entry: entry,
              jid: jid
            });
            
            keyCounts[targetNodeId]++;
          }
        }
        
        // Track node targeting time
        phaseTimings.nodeTargeting = Date.now() - nodeTargetingStartTime;
        
        // Memory snapshot after node targeting
        const memAfterTargeting = process.memoryUsage();
        memorySnapshots.afterNodeTargeting = memAfterTargeting.heapUsed;
        
        console.log(`[MR-${jid}] Node ${nodeId}: Completed node targeting in ${phaseTimings.nodeTargeting}ms`);
        console.log(`[MR-${jid}] Node ${nodeId}: Memory after targeting: heap=${Math.round(memAfterTargeting.heapUsed/1024/1024)}MB (${Math.round((memAfterTargeting.heapUsed - memAfterRetrieval.heapUsed)/1024/1024)}MB change)`);
        
        // Find the largest batch for logging
        Object.keys(nodeTargets).forEach(target => {
          const batchSize = nodeTargets[target].length;
          if (batchSize > largestBatchSize) {
            largestBatchSize = batchSize;
            largestBatchTarget = target;
          }
        });
        
        console.log(`[MR-${jid}] Node ${nodeId}: Largest batch: ${largestBatchSize} entries targeting node ${largestBatchTarget}`);
        console.log(`[MR-${jid}] Node ${nodeId}: Key distribution across nodes: ${JSON.stringify(keyCounts)}`);
        
        // Start batch sending phase
        const batchSendingStartTime = Date.now();
        
        // Process each target node's batch
        const targetNodeIds = Object.keys(nodeTargets);
        let nodesProcessed = 0;
        
        if (targetNodeIds.length === 0) {
          console.log(`[MR-${jid}] Node ${nodeId}: No target nodes found for shuffling. Completing phase.`);
          service.notify({
            phase: "SHUFFLE", 
            status: "COMPLETED", 
            gid: gid, 
            jid: jid,
            nodeId: nodeId,
            noKeysToProcess: true,
            shuffleStats: {
              entriesProcessed: entriesProcessed,
              entriesSent: entriesSent,
              targetsFound: targetsFound,
              targetStats: targetStats,
              errors: errors,
              phaseTimings: phaseTimings,
              memoryUsage: {
                initial: Math.round(initialMemUsage.heapUsed/1024/1024),
                afterMapRetrieval: Math.round(memorySnapshots.afterMapRetrieval/1024/1024),
                afterNodeTargeting: Math.round(memorySnapshots.afterNodeTargeting/1024/1024)
              }
            }
          }, callback);
          return;
        }
        
        // Track active transfers
        let activeTransfers = 0;
        const maxConcurrentTransfers = 2; // Limit concurrent transfers
        
        // Process nodes one by one to reduce memory pressure
        function processNextNode(nodeIndex) {
          if (nodeIndex >= targetNodeIds.length) {
            // All nodes processed, wait for remaining transfers
            if (activeTransfers === 0) {
              completeShufflePhase();
            }
            return;
          }
          
          const targetNodeId = targetNodeIds[nodeIndex];
          const entries = nodeTargets[targetNodeId];
          const targetNodeConfig = groupNodes[targetNodeId];
          
          console.log(`[MR-${jid}] Node ${nodeId}: Processing ${entries.length} entries for node ${targetNodeId}`);
          
          // Process entries in batches
          let batchesProcessed = 0;
          let entriesSentToThisNode = 0;
          const totalBatches = Math.ceil(entries.length / SHUFFLE_BATCH_SIZE);
          
          // Process batches sequentially
          function processNextBatch() {
            if (batchesProcessed >= totalBatches) {
              // All batches for this node processed
              console.log(`[MR-${jid}] Node ${nodeId}: Completed sending ${entriesSentToThisNode} entries to node ${targetNodeId}`);
              
              // Free memory by clearing processed entries
              nodeTargets[targetNodeId] = null;
              
              // Process next node
              processNextNode(nodeIndex + 1);
              return;
            }
            
            // Check if we should wait due to too many active transfers
            if (activeTransfers >= maxConcurrentTransfers) {
              setTimeout(processNextBatch, 100);
              return;
            }
            
            const batchStartTime = Date.now();
            const startIdx = batchesProcessed * SHUFFLE_BATCH_SIZE;
            const endIdx = Math.min(startIdx + SHUFFLE_BATCH_SIZE, entries.length);
            const batchEntries = entries.slice(startIdx, endIdx);
            
            console.log(`[MR-${jid}] Node ${nodeId}: Sending batch ${batchesProcessed+1}/${totalBatches} (${batchEntries.length} entries) to node ${targetNodeId}`);
            
            // Set timeout for this batch
            const sendTimeout = setTimeout(() => {
              console.error(`[MR-${jid}] Node ${nodeId}: TIMEOUT sending batch ${batchesProcessed+1} to ${targetNodeId}`);
              errors.count++;
              errors.details.push({
                phase: 'batch_send_timeout',
                targetNode: targetNodeId,
                batchNumber: batchesProcessed+1,
                entriesCount: batchEntries.length
              });
              
              // Reduce active transfers count
              activeTransfers--;
              
              // Continue with next batch despite timeout
              batchesProcessed++;
              processNextBatch();
            }, 60000); // 60-second timeout
            
            // Track active transfers
            activeTransfers++;
            
            // Send batch to target node
            const batchData = {
              entries: batchEntries,
              jid: jid,
              gid: gid
            };
            
            const config = {
              service: 'mem',
              method: 'bulk_append',
              node: targetNodeConfig
            };
            
            // Log memory before send
            if (batchesProcessed === 0) {
              const memBeforeSend = process.memoryUsage();
              console.log(`[MR-${jid}] Node ${nodeId}: Memory before first batch send: heap=${Math.round(memBeforeSend.heapUsed/1024/1024)}MB`);
            }
            
            distribution.local.comm.send([batchData], config, (err, result) => {
              // Clear timeout
              clearTimeout(sendTimeout);
              
              // Decrease active transfers
              activeTransfers--;
              
              // Handle errors
              if (err) {
                console.error(`[MR-${jid}] Node ${nodeId}: Error sending batch ${batchesProcessed+1} to node ${targetNodeId}: ${err.message}`);
                errors.count++;
                errors.details.push({
                  phase: 'batch_send_error',
                  targetNode: targetNodeId,
                  batchNumber: batchesProcessed+1,
                  error: err.message
                });
              } else {
                // Update statistics
                entriesSent += batchEntries.length;
                entriesSentToThisNode += batchEntries.length;
              }
              
              // Calculate batch duration
              const batchDuration = Date.now() - batchStartTime;
              batchesProcessed++;
              
              console.log(`[MR-${jid}] Node ${nodeId}: Batch ${batchesProcessed}/${totalBatches} for ${targetNodeId} completed in ${batchDuration}ms`);
              
              // Release references to help GC
              batchEntries.length = 0;
              
              // Process next batch
              processNextBatch();
            });
          }
          
          // Start processing first batch
          processNextBatch();
        }
        
        // Start processing first node
        processNextNode(0);
        
        // Function to complete the shuffle phase
        function completeShufflePhase() {
          phaseTimings.batchSending = Date.now() - batchSendingStartTime;
          phaseTimings.total = Date.now() - shuffleStartTime;
          
          // Memory snapshot after batch sending
          const memAfterSending = process.memoryUsage();
          memorySnapshots.afterBatchSending = memAfterSending.heapUsed;
          
          console.log(`[MR-${jid}] Node ${nodeId}: SHUFFLE COMPLETED in ${phaseTimings.total}ms`);
          console.log(`[MR-${jid}] Node ${nodeId}: Final memory: heap=${Math.round(memAfterSending.heapUsed/1024/1024)}MB (${Math.round((memAfterSending.heapUsed - initialMemUsage.heapUsed)/1024/1024)}MB total change)`);
          
          // Clear mapped results to release memory
          storageService.del({gid: gid, key: mapResultName}, (err, val) => {
            if (err) {
              console.error(`[MR-${jid}] Node ${nodeId}: Error deleting map results: ${err.message}`);
              errors.count++;
              errors.details.push({phase: 'cleanup', error: err.message});
            } else {
              console.log(`[MR-${jid}] Node ${nodeId}: Successfully deleted map results after shuffling`);
            }
            
            // Force garbage collection if available
            if (global.gc) {
              global.gc();
              const memAfterGC = process.memoryUsage();
              console.log(`[MR-${jid}] Node ${nodeId}: Memory after forced GC: heap=${Math.round(memAfterGC.heapUsed/1024/1024)}MB (${Math.round((memAfterGC.heapUsed - memAfterSending.heapUsed)/1024/1024)}MB change)`);
            }
            
            // Notify completion
            service.notify({
              phase: "SHUFFLE", 
              status: "COMPLETED", 
              gid: gid, 
              jid: jid,
              nodeId: nodeId,
              noKeysToProcess: false,
              shuffleStats: {
                entriesProcessed: entriesProcessed,
                entriesSent: entriesSent,
                targetsFound: targetsFound,
                errors: errors,
                phaseTimings: phaseTimings,
                memoryUsage: {
                  initial: Math.round(initialMemUsage.heapUsed/1024/1024),
                  afterMapRetrieval: Math.round(memorySnapshots.afterMapRetrieval/1024/1024),
                  afterNodeTargeting: Math.round(memorySnapshots.afterNodeTargeting/1024/1024),
                  afterBatchSending: Math.round(memorySnapshots.afterBatchSending/1024/1024)
                },
                targetStats: targetStats
              }
            }, callback);
          });
        }
      });
    });
  });
};

    /**
     * Reduce function with enhanced tracking
     */
    const reduce = (config, callback) => {
      const gid = config.gid;
      const job_id = config.jid;
      const nodeId = global.nodeConfig.port;
      
      const reduceStartTime = Date.now();
      const initialMemUsage = process.memoryUsage();
      console.log(`[MR-${job_id}] Node ${nodeId}: Starting reduce phase. Memory: heap=${Math.round(initialMemUsage.heapUsed/1024/1024)}MB`);
      
      // Tracking variables
      let keysProcessed = 0;
      let totalValues = 0;
      let slowKeysCount = 0;
      
      distribution.local.routes.get(job_id, (err, service) => {
        if (err) {
          console.error(`[MR-${job_id}] Node ${nodeId}: Error getting service: ${err.message}`);
          callback(err, null);
          return;
        }
        
        const reducer = service.reducer;
        const shuffleResultName = "reduce@" + job_id;
        const storageService = distribution.local.mem;
        
        storageService.get({gid: gid, key: shuffleResultName}, (err, shuffleResults) => {
          if (err || !shuffleResults || Object.keys(shuffleResults).length === 0) {
            console.error(`[MR-${job_id}] Node ${nodeId}: Error retrieving shuffle results: ${err ? err.message : "no results found"}`);
            service.notify({
              phase: "REDUCE", 
              status: "COMPLETED", 
              results: [], 
              gid: gid, 
              jid: job_id,
              nodeId: nodeId,
              port: nodeId,
              noKeysToProcess: true,
              reduceStats: {
                keysProcessed: 0,
                totalValues: 0,
                slowKeys: 0
              }
            }, callback);
            return;
          }
          
          const reduceKeys = Object.keys(shuffleResults);
          keysProcessed = reduceKeys.length;
          
          console.log(`[MR-${job_id}] Node ${nodeId}: Reducing ${reduceKeys.length} keys`);
          
          // Process keys in batches for better memory management
          const REDUCE_BATCH_SIZE = Math.min(50, reduceKeys.length);
          let currentBatch = 0;
          const totalBatches = Math.ceil(reduceKeys.length / REDUCE_BATCH_SIZE);
          let reduceResults = [];
          
          const processBatch = () => {
            const batchStartTime = Date.now();
            const startIdx = currentBatch * REDUCE_BATCH_SIZE;
            const endIdx = Math.min(startIdx + REDUCE_BATCH_SIZE, reduceKeys.length);
            const batchKeys = reduceKeys.slice(startIdx, endIdx);
            
            // console.log(`[MR-${job_id}] Node ${nodeId}: Processing reduce batch ${currentBatch+1}/${totalBatches} with ${batchKeys.length} keys`);
            
            let batchResults = [];
            let keysProcessed = 0;
            let batchTotalValues = 0;
            let batchSlowKeys = 0;
            
            batchKeys.forEach(key => {
              let values = shuffleResults[key];
              
              if (!Array.isArray(values)) {
                values = [values];
              }
              
              batchTotalValues += values.length;
              totalValues += values.length;
              
              try {
                const reduceStartMs = Date.now();
                const result = reducer(key, values);
                const reduceDurationMs = Date.now() - reduceStartMs;
                
                if (reduceDurationMs > 500) { // Log slow reducer operations
                  console.log(`[MR-${job_id}] Node ${nodeId}: Slow reducer for key ${key}: ${reduceDurationMs}ms with ${values.length} values`);
                  batchSlowKeys++;
                  slowKeysCount++;
                }
                
                batchResults.push(result);
              } catch (reduceError) {
                console.error(`[MR-${job_id}] Node ${nodeId}: Error reducing key ${key}: ${reduceError.message}`);
              }
              
              keysProcessed++;
              
              if (keysProcessed === batchKeys.length) {
                // Add batch results to total results
                // const batchDuration = Date.now() - batchStartTime;
                // console.log(`[MR-${job_id}] Node ${nodeId}: Reduce batch ${currentBatch+1}/${totalBatches} completed in ${batchDuration}ms. Processed ${keysProcessed} keys with ${batchTotalValues} total values. Slow keys: ${batchSlowKeys}`);
                
                reduceResults = reduceResults.concat(batchResults);
                currentBatch++;
                
                if (currentBatch < totalBatches) {
                  // Process next batch
                  processBatch();
                } else {
                  // All batches processed, notify completion
                  storageService.del({gid: gid, key: shuffleResultName}, (err, val) => {
                    if (err) {
                      console.error(`[MR-${job_id}] Node ${nodeId}: Error deleting shuffle results: ${err.message}`);
                    } else {
                      console.log(`[MR-${job_id}] Node ${nodeId}: Deleted shuffle results after reducing`);
                    }
                    
                    const reduceDuration = Date.now() - reduceStartTime;
                    const finalMemUsage = process.memoryUsage();
                    console.log(`[MR-${job_id}] Node ${nodeId}: Reduce phase completed in ${reduceDuration}ms. Results: ${reduceResults.length}, Values processed: ${totalValues}, Memory: heap=${Math.round(finalMemUsage.heapUsed/1024/1024)}MB`);
                    
                    service.notify({
                      phase: "REDUCE", 
                      status: "COMPLETED", 
                      results: reduceResults,
                      gid: gid, 
                      jid: job_id,
                      nodeId: nodeId,
                      port: nodeId, // Include port for debugging
                      noKeysToProcess: false,
                      reduceStats: {
                        keysProcessed: keysProcessed,
                        totalValues: totalValues,
                        slowKeys: slowKeysCount
                      }
                    }, callback);
                  });
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
    console.log(`[MR-${mrId}] EXEC STARTS on node ${global.nodeConfig.port}, with ${keys.length} keys`);
    
    distribution[context.gid].routes.put(mrServiceObject, mrServiceName, (err, res) => {
      if (err) {
        console.error(`[MR-${mrId}] Error registering service: ${err.message}`);
        cb(err, null);
        return;
      }

      console.log(`[MR-${mrId}] Service registered successfully, starting map phase for batch 0/${num_batches}`);
      checkpointManager.load((err, checkpoint) => {
        if (!err && checkpoint) {
          console.log(`[MR-${mrId}] Resuming from checkpoint at batch ${checkpoint.state.batch_num}`);
          
          // Restore saved state
          state_dict = checkpoint.state;
          keyTrackingMap = checkpoint.keyTrackingMap;
          
          // Restore collected results
          if (checkpoint.partialResults && checkpoint.partialResults.length > 0) {
            console.log(`[MR-${mrId}] Restored ${checkpoint.partialResults.length} partial results`);
            results = checkpoint.partialResults;
          }
          
          // Continue from the current batch
          console.log(`[MR-${mrId}] Continuing MapReduce from batch ${state_dict.batch_num}/${state_dict.num_batches}`);
          
          // Start from the current phase
          const setupConfig = {
            gid: context.gid,
            jid: mrServiceName,
            keys: keys,
            batch_num: state_dict.batch_num,
            batch_size: BATCH_SIZE
          };
          
          distribution[context.gid].comm.send([setupConfig], {gid: 'local', service: mrServiceName, method: state_dict.phase.toLowerCase()}, (e, v) => {
            if (e) {
              console.error(`[MR-${mrId}] Error resuming job: ${e.message}`);
            }
          });
        } else {
          // No checkpoint found, start fresh
          console.log(`[MR-${mrId}] Starting new MapReduce job with ${keys.length} keys`);
          
          const setupConfig = {
            gid: context.gid,
            jid: mrServiceName,
            keys: keys,
            batch_num: state_dict.batch_num,
            batch_size: BATCH_SIZE
          };
            
          distribution[context.gid].comm.send([setupConfig], {gid: 'local', service: mrServiceName, method: 'map'}, (e, v) => {
            if (e) {
              console.error(`[MR-${mrId}] Error starting map phase: ${e.message}`);
            }
          });
        }
      });
      
      
    });
  }

  return { exec };
}

module.exports = mr;