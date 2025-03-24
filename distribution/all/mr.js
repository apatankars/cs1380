/** @typedef {import("../types").Callback} Callback */
// const { log } = require("console");
// const distribution = require("../../config");

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
    const mrId = require("crypto").randomUUID().substring;
    const mrServiceName = `mr@${mrId}`; // mr@<uuid>
    const user_reduce = distribution.util.serialization.serialize(configuration.reduce)
    const user_map = distribution.util.serialization.serialize(configuration.map)

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
     *    phase: string of "MAP", "REDUCE", "SHUFLLE"
     *    status: string of "COMPLETED", "ERROR"
     *    gid: string of the group ID
     *    jid: string of the job ID (mr@<uuid>)
     *  
     * @param {*} cb 
     */
    const notify = (config, cb)  => {
      const phase_map = {
        MAP: "SHUFFLE",
        SHUFFLE: "REDUCE",
        REDUCE: "DONE",
      };
      if (config.status === "ERROR") {
            cb(Error(res.error), null);
      } 
      if (config.phase === "SETUP") {
        const remote = {
          service: config.jid,
          method: 'map',
        }
        const setupConfig = {
          mapper: user_map,
          gid: config.gid,
          jid: config.jid
        }
        const message = [setupConfig];
        state_dict.phase = "MAP";
        //
        distribution[context.gid].comm.send(message, remote, cb);
      }
      
      distribution.local.groups.get(config.gid, (err, group) => {
            if (err) {
              cb(err, null);
              return;
            }
            let groupNodeCount = Object.keys(group).length;

            // log(`Starting MapReduce job ${mrServiceName} for group ${config.gid} with ${groupNodeCount} nodes`);
              state_dict.phase_count = state_dict.phase_count + 1;

              if (config.phase !== state_dict.phase) {
                cb(
                  Error(
                    `Error: Phase mismatch. Expected ${state_dict.phase}, got ${config.phase}`
                  ),
                  null
                );
                return;
              }

              if (state_dict.phase === "REDUCE") {
                if (config.results) {
                  results.push(config.results);
                }
              }

              // When we update the state dictionary, we check if the phase_count === workerCount to know if we are done
              if (state_dict.phase_count === groupNodeCount) {
                // we move onto the next phase
                if (state_dict.phase === "REDUCE") {
                  cb(null, results);
                  return;
                }
                // otherwise we update the phase and phase count to the next phase
                let new_phase = phase_map[state_dict.phase];
                state_dict.phase = new_phase;
                state_dict.phase_count = 0;

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
                if (state_dict.phase === "REDUCE"){
                  phaseConfig.reducer = user_reduce;
                }
                const message = [phaseConfig];
                distribution[context.gid].comm.send(message, remote, cb);
              }
          })
        };

    /**
     * 
     * @param {*} config
     *    mapper: this is the serialized version of the user provided mapper
     *    gid: this is the groupID 
     *    jid: this is the jobID (mr@<uuid>)
     * @param {*} cb 
     */
    const map = (config, cb) => {
      // Config object should contain the serialized user map function
      const ser_mapper = config.mapper;
      const gid = config.gid;
      const job_id = config.jid;
      const mapper = distribution.util.deserialize(ser_mapper);

      // Get the service for this job
      distribution.local.routes.get({gid: gid, service: job_id}, (err, service) => {
        // get the keys for the group
        distribution.local.store.getGroupKeys(gid, (err, keys) => {
          if (err) {
            cb(err, null);
            return;
          }

          if (!keys || keys.length === 0) {
            // No keys to process on this node, but still notify completion
            const mapResultName = "map@" + job_id;
            distribution.local.store.put([], {key: mapResultName, gid: gid}, (err) => {
              if (err) {
                cb(err, null);
                return;
              }
              // Notify that the map phase is completed
              service.notify({phase: "MAP", status: "COMPLETED"}, (err, res) => {
                if (err) {
                  cb(err, null);
                }
                cb(null, "DONE");
              });
            });
            return;
          }

          // Array to hold the results of the map operation
          let mapResults = [];
          // Counter for pending operations
          let pendingOperations = keys.length;
          // Flag to track if an error has occurred
          let hasError = false;

          // Process each key
          keys.forEach((key) => {
            // Get the value for this key
            distribution.local.store.get({ key: key, gid: gid }, (err, value) => {
              // If we already encountered an error, don't continue processing
              if (hasError) return;

              if (err) {
                hasError = true;
                cb(err, null);
                return;
              }

              try {
                // Apply the mapper function
                let res = mapper(key, value);
                
                // Make sure result is an array
                if (!Array.isArray(res)) {
                  res = [res];
                }
                
                // Add results to our collection
                mapResults = mapResults.concat(res);
                
                // Decrement the counter of pending operations
                pendingOperations--;
                
                // If all operations are done, store results and notify completion
                if (pendingOperations === 0) {
                  const mapResultName = "map@" + job_id;
                  distribution.local.store.put(mapResults, {key: mapResultName, gid: gid}, (err) => {
                    if (err) {
                      cb(err, null);
                      return;
                    }
                    service.notify({phase: "MAP", status: "COMPLETED"}, (err, res) => {
                      if (err) {
                        cb(err, null);
                        return;
                      }
                      cb(null, "DONE");
                      return;
                    });
                  });
                }
              } catch (mapError) {
                if (!hasError) {
                  hasError = true;
                  cb(mapError, null);
                }
              }
            });
          });
        });
      });
    } // This is the end of the map method

    /**
     * For each node it should send the results of the map phase to the designated node 
     * using the given hash function provided by the user
     * @param {*} config 
     *    gid: this is the groupID
     *    job_id: this is the jobID (mr@<uuid>)
     * @param {*} cb 
     */
    const shuffle = (config, cb) => {

      const gid = config.gid;
      const jid = config.jid;

      // Get the service for this job
      distribution.local.routes.get({gid: gid, serivce: jid}, (err, service) => {
        // Get the map results from the local store
        distribution.local.store.get({key: "map@" + jid, gid: config.gid}, (err, mapResults) => {
          // mapResults contains the unserialized map results
          if (err) {
            service.notify({phase: "SHUFFLE", status: "ERROR"}, (err, res) => {
              cb(err, null);
              return;
            });
          }

          // Now we have the map results, we need to distribute them to the correct nodes
          const entrySize = mapResults.length;

          let entriesProcessed = 0;
          
          Object.entries(mapResults).forEach(([key, value]) => {
            
            distribution[gid].store.append(value, "reduce@" + jid, (err, res) => {
              if (err) {
                cb(err, null);
                return;
              }
              if (++entriesProcessed === entrySize) {
                service.notify({phase: "SHUFFLE", status: "COMPLETED"}, (err, res) => {
                  if (err) {
                    cb(err, null);
                    return;
                  }
                  cb(null, "DONE");
                  return;
                });
              }
            });
          })
        });
      });
    }

    /**
     * The reduce function should pull all of the local information and then call the user provided reduce function
     * @param {*} config 
     *    reducer: user provided reducer
     *    gid: this is the groupID
     *    job_id: this is the jobID (mr@<uuid>)
     * @param {*} cb 
     */
    const reduce = (config, cb) => {
      // Config object should contain the serialized user reduce function
      const ser_reducer = config.reducer;
      const gid = config.gid;
      const job_id = config.jid;
      const reducer = distribution.util.deserialize(ser_reducer);

      // Get the service for this job
      distribution.local.routes.get({gid: gid, service: job_id}, (err, service) => {
        // get the reduce job stored object for the group
        const shuffleResultName = "reduce@" + job_id;
        distribution.local.store.get({key: shuffleResultName, gid: gid}, (err, shuffleResults) => {
          if (err) {
            cb(err, null);
            return;
          }

          if (!shuffleResults || Object.keys(shuffleResults).length === 0) {
            // No keys to process on this node, but still notify completion
            // Notify that the map phase is completed
            service.notify({phase: "REDUCE", status: "COMPLETED", results: []}, (err, res) => {
              if (err) {
                cb(err, null);
              }
              cb(null, "DONE");
            });
            return;
          }

          // Array to hold the results of the map operation
          let reduceResults = [];
          // Counter for pending operations
          let pendingOperations = Object.keys(shuffleResults).length;
          // Flag to track if an error has occurred
          let hasError = false;

          // Process each key
          Object.keys(shuffleResults).forEach((key) => {
            // Get the value for this key
            let value = shuffleResults[key];
            
            // If we already encountered an error, don't continue processing
            if (hasError) return;

            try {
              // Apply the mapper function
              let res = reducer(key, value);
              
              // Add results to our collection
              reduceResults = reduceResults.push(res);
              
              // Decrement the counter of pending operations
              pendingOperations--;
              
              // If all operations are done, store results and notify completion
              if (pendingOperations === 0) {
                service.notify({phase: "REDUCE", status: "COMPLETED", results: reduceResults}, (err, res) => {
                  if (err) {
                    cb(err, null);
                    return;
                  }
                  cb(null, "DONE");
                  return;
                });
              }
            } catch (mapError) {
              if (!hasError) {
                hasError = true;
                cb(mapError, null);
              }
            }

          });
        });
      });
    } // This is the end of the map method

    
    let notifyRPC = distribution.util.wire.createRPC(distribution.util.wire.toAsync(notify));

    let mrServiceObject = {
      notify: notifyRPC,
      map: map,
      shuffle: shuffle,
      reduce: reduce
    };
    
    distribution[context.gid].routes.put(mrServiceObject, mrServiceName, (err, res) => {
      if (err) {
        cb(err, null);
        return;
      }
    });
        
      
  }

  return { exec };
}

module.exports = mr;
