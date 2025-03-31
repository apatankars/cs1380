const distribution = require("@brown-ds/distribution");

    const map = (config, callback) => {
      // Config object should contain the serialized user map function
      const keys = config.keys;
      const gid = config.gid;
      const job_id = config.jid;
      let pendingOperations = keys.length;

      // First we get the serivce object for this worker node
      distribution.local.routes.get({gid: gid, service: job_id}, (err, service) => {
        if (err) {
          console.log("ERROR ERROR ERROR ", err);
          callback(err, null);
          return;
        }

        // We placed the service method mapper from the user provided function on each worker
        const mapper = service.mapper;

        let mapResults = [];

        console.log("Mapping for node: ", global.nodeConfig, 'starting!');

        keys.forEach((key) => {
          distribution.local.store.get({key: key, gid: gid}, (err, val => {
            if (val) {
              try {
                let res = mapper(key, val);

                if (!Array.isArray(res)) {
                  res = [res];
                }

                mapResults = mapResults.concat(res);

                // Decrement the counter of pending operations
                pendingOperations--;

                if (pendingOperations === 0) {
                  const mapResultName = "map@" + job_id;
                  distribution.local.store.put(mapResults, {key: mapResultName, gid: gid}, (err) => {
                    if (err) {
                      callback(err, null);
                      return;
                    }
                    console.log("Mapping for node: ", global.nodeConfig, ' finished!');
                    service.notify({phase: "MAP", status: "COMPLETED", gid: gid, jid: job_id}, callback);
                  });
                }
              } catch (mapError) {
                if (!hasError) {
                  hasError = true;
                  callback(mapError, null);
                }
              }
            }
          }))
        })

        // let node_sid = util.id.getSID(global.nodeConfig);
        
        // Addded in this function so the local node can just grab the keys for the group
        // distribution.local.store.getGroupKeys(gid, (err, keys) => {
          // if (err) {
          //   console.log("ERROR ERROR ERROR: ", err)
          //   callback(err, null);
          //   return;
          // }
          // console.log("Found for node: ", global.nodeConfig);
          

          // Array to hold the results of the map operation
          
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
                callback(err, null);
                return;
              }

              try {
                // Apply the mapper function
                let res = mapper(key, value);

                console.log(`Node ${node_sid}: mapped original key: ${key} and val: ${value} to ${res}`);
                
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
                      callback(err, null);
                      return;
                    }
                    console.log("Mapping for node: ", global.nodeConfig, ' finished!');
                    service.notify({phase: "MAP", status: "COMPLETED", gid: gid, jid: job_id}, callback);
                  });
                }
              } catch (mapError) {
                if (!hasError) {
                  hasError = true;
                  callback(mapError, null);
                }
              }
            });
          });
        });
        // });
      // });
    }; // This is the end of the map method





    keys.forEach((key) => {
          // console.log("on key ", key)
          distribution.local.store.get({key: key, gid: gid}, (err, val2 => {
            if (err) {
              console.log(global.nodeConfig, err)
            }
            if (!(val2 instanceof Error)) {
              try {
                console.log(global.nodeConfig, key, val2, err)
                let res = mapper(key, val2);

                console.log("Mapper output ", res)

                if (!Array.isArray(res)) {
                  res = [res];
                }

                mapResults = mapResults.concat(res);

                // Decrement the counter of pending operations
                pendingOperations--;

                if (pendingOperations === 0) {
                  const mapResultName = "map@" + job_id;
                  distribution.local.store.put(mapResults, {key: mapResultName, gid: gid}, (err) => {
                    if (err) {
                      callback(err, null);
                      return;
                    }
                    console.log("Mapping for node: ", global.nodeConfig, ' finished!');
                    service.notify({phase: "MAP", status: "COMPLETED", gid: gid, jid: job_id}, callback);
                  });
                }
              } catch (mapError) {
                if (!hasError) {
                  hasError = true;
                  console.log("BIG LOSER")
                  callback(mapError, null);
                }
              }
            }
          }))
        })


        const mapResultName = "map@" + job_id;
                  // distribution.local.store.put(mapResults, {key: mapResultName, gid: gid}, (err, val) => {
                  //   if (err) {
                  //     callback(err, null);
                  //     return;
                  //   }
                  //   // console.log(service.notify)
                  //   // service.notify({phase: "MAP", status: "COMPLETED", gid: gid, jid: job_id}, callback);
                  // })