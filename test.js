/** @typedef {import("../types").Callback} Callback */
const { log } = require("console");
const distribution = require("../../config");

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
    const mrId = require("crypto").randomUUID().substring(0, 8);
    const mrServiceName = `mr@${mrId}`; // mr@<uuid>

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
      // TODO: Does notify need like a "START" so that exec can call it and then notify actually handles
      // TODO: coordinating all of the nodes together

      const phase_map = {
        MAP: "SHUFFLE",
        SHUFFLE: "REDUCE",
        REDUCE: "DONE",
      };
      if (config.status === "ERROR") {
            cb(Error(res.error), null);
      }
      // TODO: Consolidate this so that the nodeGroupSize is a variable that can just be referenced instead
      // TODO: of having to make a redunant call to groups everytime that notify is called
      
      distribution.local.groups.get(config.gid, (err, group) => {
            if (err) {
              callback(err, null);
              return;
            }
            let groupNodeCount = Object.keys(group).length;

            // log(`Starting MapReduce job ${mrServiceName} for group ${config.gid} with ${groupNodeCount} nodes`);
              state_dict.phase_count = state_dict.phase_count + 1;

              log(`Found state dictionary ${JSON.stringify(state_dict)}`);

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
                  // TODO: When reduce is finished, we want to collect all of the results and demolish all of our eph
                  cb(null, "DONE");
                  return;
                }
                // otherwise we update the phase and phase count to the next phase
                let new_phase = phase_map[state_dict.phase];
                state_dict.phase = new_phase;
                state_dict.phase_count = 0;
                // TODO: Now we send a message to all nodes to start their next phase
                const endPoint = mrServiceName;
                const method = state_dict.phase.toLowerCase();

                // const remote = {service: endPoint, method: method};

                // TODO: The message will vary depending on the endPoint
                // make each worker node call the notify method for the next phase
                
                // const message = [{}] 
                // distribution[context.gid].send(
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
     *    node_list : this is the list of nodes to send the data to
     *    hash: this is the hash function provided by the user
     * @param {*} cb 
     */
    const shuffle = (config, cb) => {

      const gid = config.gid;
      const jid = config.jid;
      const node_list = config.node_list;
      const prov_hash = config.hash;

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
     * @param {*} cb 
     */
    const reduce = (config, cb) => {

      const ser_reduce = config.reducer;
      const gid = config.gid;
      const jid = config.jid;
      const reducer = distribution.util.deserialize(ser_reduce);
      let reduceResults = {};

      // Get the service for this job
      distribution.local.routes.get({gid: gid, service: jid}, (err, service) => {
        // Get the shuffle results from the local store
        distribution.local.store.get({key: "reduce@" + jid, gid: gid}, (err, reduceResults) => {
          // So now we have the shuffled results
          if (err) {
            cb(err, null);
            return;
          }

          let index = {};

          Object.entries(reduceResults).forEach(([key, value]) => {
            if (!index[key]) {
              index[key] = [];
            }
            index[key].push(value);
          });

          Object.entries(index).forEach(([key, value]) => {
            let res = reducer(key, value);
            if (!Array.isArray(res)) {
              res = [res];
            }
            reduceResults = {...reduceResults, ...res};
          });
        });
      });
    };

    // This is handling the setup of the MapReduce job
    // First we create the servive object for the job
    let notifyRPC = distribution.util.wire.createRPC(distribution.util.wire.toAsync(notify));

    let mrServiceObject = {};
    mrServiceObject[notify] = notifyRPC;
    mrServiceObject[map] = map;
    mrServiceObject[shuffle] = shuffle;

    // TODO: Put the new serviceObject on all of the worker nodes 
    // TODO: Then, call `notify` to start the first phase of the MR job



        
    const setupConfig = {
          service: mrServiceName,
          method: "map"
        }
    const setupMessage = [configuration.mapper, config.gid, config.jid];
    distribution[config.gid].comm.send(setupMessage, setupConfig, (err, res) => {
      if (err) {
        cb(err, null);
        return;
      }
      cb(null, "DONE");
      return;
    });

    // Now we publicly expose the route so other nodes can call this notify service
    // distribution[context.gid].routes.put(); // 

    // TODO: ISSUE: how do we get other nodes to be able to know to this MR job's notify service (mr-<uuid>)
    // TODO: need to figure out a way to get other nodes to call this coordinator's node notify method
    // TODO: My understanding is that we need to create a notify service on the coordinator node. Then we create 
    // TODO: A map, shuffle, and reduce endpoint services at <phase>@mr-<uuid> and then we create this route on all nodes
    // TODO: The issue is, how do these map, shuffle, and reduce services know to call the notify service on the coordinator node when they are done
    // TODO: I think I am confused on what notify is really doing, and how it is doing it 
        
      
  }

  return { exec };
}

module.exports = mr;
