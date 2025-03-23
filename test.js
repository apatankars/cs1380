function mr(config) {
  const context = {
    gid: config.gid || "all",
  };

  const util = require("../util/util");
  const id = util.id;

  /**
   * @param {MRConfig} configuration
   * @param {Callback} cb
   */
  function exec(configuration, cb) {
    // Generate a unique ID for this MapReduce task
    const crypto = require("crypto");
    const mrId = crypto.randomUUID().substring(0, 8);
    const mrServiceName = `mr-${mrId}`;

    // Get the user-provided map and reduce functions
    const mapFn = configuration.map;
    const reduceFn = configuration.reduce;
    const keys = configuration.keys || [];

    // Track phase completions
    let mapComplete = 0;
    let shuffleComplete = 0;
    let reduceComplete = 0;
    let finalResults = [];

    // Get total number of nodes in the group
    distribution.local.groups.get(context.gid, (err, group) => {
      if (err) return cb(err, null);
      const totalNodes = Object.keys(group).length;

      // Create the notification service
      const notifyService = {
        notify: (data, callback) => {
          const phase = data.phase;

          if (phase === "map") {
            mapComplete++;
            if (mapComplete === totalNodes) {
              startShufflePhase();
            }
          } else if (phase === "shuffle") {
            shuffleComplete++;
            if (shuffleComplete === totalNodes) {
              startReducePhase();
            }
          } else if (phase === "reduce") {
            if (data.result) {
              finalResults = finalResults.concat(data.result);
            }
            reduceComplete++;
            if (reduceComplete === totalNodes) {
              // Clean up and return results
              distribution.local.routes.rem(mrServiceName, () => {
                cb(null, finalResults);
              });
            }
          }
          callback(null, "Notification received");
        },
      };

      // Register the notification service
      distribution.local.routes.put(notifyService, mrServiceName, (err) => {
        if (err) return cb(err, null);

        // Register map handler
        const mapHandler = {
          execute: (data, callback) => {
            const coordinator = data.coordinator;
            const mapFunction = data.mapFunction;
            const keysToProcess = data.keys || [];

            // Store map results locally
            const mapResults = [];
            let processed = 0;

            keysToProcess.forEach((key) => {
              distribution.local.store.get(key, (err, value) => {
                if (!err && value) {
                  try {
                    const result = mapFunction(key, value);
                    if (result) {
                      // Store each key-value pair for shuffling
                      const tempKey = `map-${mrId}-${key}`;
                      distribution.local.store.put(result, tempKey, () => {
                        mapResults.push(tempKey);
                        processed++;
                        if (processed === keysToProcess.length) {
                          // Store map keys for shuffle phase
                          distribution.local.store.put(
                            mapResults,
                            `map-keys-${mrId}`,
                            () => {
                              notifyCoordinator("map", coordinator);
                            }
                          );
                        }
                      });
                    } else {
                      processed++;
                    }
                  } catch (e) {
                    processed++;
                  }
                } else {
                  processed++;
                }

                // Check if all keys processed (handles empty case too)
                if (
                  processed === keysToProcess.length &&
                  processed > 0 &&
                  mapResults.length === 0
                ) {
                  notifyCoordinator("map", coordinator);
                }
              });
            });

            // Handle empty keys array
            if (keysToProcess.length === 0) {
              notifyCoordinator("map", coordinator);
            }

            callback(null, "Map processing started");
          },
        };

        // Register shuffle handler
        const shuffleHandler = {
          execute: (data, callback) => {
            const coordinator = data.coordinator;

            // Get map results
            distribution.local.store.get(`map-keys-${mrId}`, (err, mapKeys) => {
              if (err || !mapKeys || mapKeys.length === 0) {
                return notifyCoordinator("shuffle", coordinator);
              }

              let shuffledKeys = [];
              let processed = 0;

              // Process each map result
              mapKeys.forEach((mapKey) => {
                distribution.local.store.get(mapKey, (err, mapResult) => {
                  if (!err && mapResult) {
                    // For each key-value pair in the map result
                    Object.keys(mapResult).forEach((key) => {
                      // Determine target node for this key
                      distribution.local.groups.get(
                        context.gid,
                        (err, group) => {
                          if (err) return;

                          const nodes = Object.values(group);
                          const nodeIDs = nodes.map((n) => id.getNID(n));
                          const keyID = id.getID(key);
                          const targetNodeID = id.naiveHash(keyID, nodeIDs);
                          const targetNode = nodes.find(
                            (n) => id.getNID(n) === targetNodeID
                          );

                          // If this is the target node, store locally
                          if (
                            id.getNID(targetNode) ===
                            id.getNID(global.nodeConfig)
                          ) {
                            const shuffleKey = `shuffle-${mrId}-${key}`;

                            // Get existing values or create new array
                            distribution.local.store.get(
                              shuffleKey,
                              (err, values) => {
                                const allValues = values || [];
                                allValues.push(mapResult[key]);

                                // Store updated values
                                distribution.local.store.put(
                                  allValues,
                                  shuffleKey,
                                  () => {
                                    if (!shuffledKeys.includes(shuffleKey)) {
                                      shuffledKeys.push(shuffleKey);
                                    }
                                  }
                                );
                              }
                            );
                          } else {
                            // Send to target node
                            const msg = { key, value: mapResult[key], mrId };
                            const remote = {
                              service: `shuffle-receive-${mrId}`,
                              method: "receive",
                              node: targetNode,
                            };
                            distribution.local.comm.send(msg, remote, () => {});
                          }
                        }
                      );
                    });
                  }

                  processed++;
                  if (processed === mapKeys.length) {
                    // Store shuffle keys for reduce phase
                    setTimeout(() => {
                      distribution.local.store.put(
                        shuffledKeys,
                        `reduce-keys-${mrId}`,
                        () => {
                          notifyCoordinator("shuffle", coordinator);
                        }
                      );
                    }, 500); // Small delay to ensure all shuffle operations complete
                  }
                });
              });
            });

            callback(null, "Shuffle processing started");
          },
        };

        // Register shuffle receive handler
        const shuffleReceiveHandler = {
          receive: (data, callback) => {
            const key = data.key;
            const value = data.value;
            const taskId = data.mrId;

            const shuffleKey = `shuffle-${taskId}-${key}`;

            // Get existing values or create new array
            distribution.local.store.get(shuffleKey, (err, values) => {
              const allValues = values || [];
              allValues.push(value);

              // Store updated values
              distribution.local.store.put(allValues, shuffleKey, callback);
            });
          },
        };

        // Register reduce handler
        const reduceHandler = {
          execute: (data, callback) => {
            const coordinator = data.coordinator;
            const reduceFnStr = data.reduceFnStr;
            const reduceFunction = eval(`(${reduceFnStr})`);

            // Get reduce keys
            distribution.local.store.get(
              `reduce-keys-${mrId}`,
              (err, reduceKeys) => {
                if (err || !reduceKeys || reduceKeys.length === 0) {
                  return notifyCoordinator("reduce", coordinator, []);
                }

                const results = [];
                let processed = 0;

                // Process each reduce key
                reduceKeys.forEach((shuffleKey) => {
                  // Extract original key from shuffle key
                  const originalKey = shuffleKey.substring(
                    `shuffle-${mrId}-`.length
                  );

                  // Get values for this key
                  distribution.local.store.get(shuffleKey, (err, values) => {
                    if (!err && values) {
                      try {
                        // Apply reduce function
                        const result = reduceFunction(originalKey, values);
                        if (result) {
                          results.push(result);
                        }
                      } catch (e) {
                        // Continue even if reduce fails
                      }
                    }

                    processed++;
                    if (processed === reduceKeys.length) {
                      notifyCoordinator("reduce", coordinator, results);
                    }
                  });
                });
              }
            );

            callback(null, "Reduce processing started");
          },
        };

        // Helper to notify coordinator
        function notifyCoordinator(phase, coordinator, result) {
          const remote = {
            service: mrServiceName,
            method: "notify",
            node: coordinator,
          };

          const message = { phase };
          if (result) {
            message.result = result;
          }

          distribution.local.comm.send(message, remote, () => {});
        }

        // Function to start shuffle phase
        function startShufflePhase() {
          distribution[context.gid].routes.put(
            shuffleHandler,
            `shuffle-${mrId}`,
            () => {
              distribution[context.gid].routes.put(
                shuffleReceiveHandler,
                `shuffle-receive-${mrId}`,
                () => {
                  const remote = {
                    service: `shuffle-${mrId}`,
                    method: "execute",
                  };

                  const message = {
                    coordinator: global.nodeConfig,
                    mrId: mrId,
                  };

                  distribution[context.gid].comm.send(
                    message,
                    remote,
                    () => {}
                  );
                }
              );
            }
          );
        }

        // Function to start reduce phase
        function startReducePhase() {
          distribution[context.gid].routes.put(
            reduceHandler,
            `reduce-${mrId}`,
            () => {
              const remote = {
                service: `reduce-${mrId}`,
                method: "execute",
              };

              const message = {
                coordinator: global.nodeConfig,
                reduceFnStr: reduceFn.toString(),
                mrId: mrId,
              };

              distribution[context.gid].comm.send(message, remote, () => {});
            }
          );
        }

        // Start the map phase
        distribution[context.gid].routes.put(mapHandler, `map-${mrId}`, () => {
          const remote = {
            service: `map-${mrId}`,
            method: "execute",
          };

          const message = {
            coordinator: global.nodeConfig,
            mapFunction: mapFn,
            keys: keys,
            mrId: mrId,
          };

          distribution[context.gid].comm.send(message, remote, () => {});
        });
      });
    });
  }

  return { exec };
}

module.exports = mr;
