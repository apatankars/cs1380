const local = distribution.local;
const util = require("../util/util");
const id = util.id;

function indexer(config) {
  const context = {};
  context.gid = config.gid || 'all';

  const cb = (error, value) => {
    if (error) {
      console.error(error);
    } else {
      console.log(value);
    }
  };

  return {
    /**
     * Process a document and distribute its terms to appropriate index nodes
     * @param {Object} configuration - Document to index
     * @param {Function} callback - Callback function
     */
    index: (configuration, callback) => {
      callback = callback || cb;
      if (configuration === undefined || configuration === null) {
        callback(new Error('Configuration is required'), null);
        return;
      }

      // Find a node to process the indexing
      distribution.local.groups.get(context.gid, (err, group) => {
        if (err) {
          return callback(new Error(`Failed to get group: ${err.message}`), null);
        }

        // Get a list of nodes in the group
        const nodes = Object.values(group);
        if (nodes.length === 0) {
          return callback(new Error('No nodes available in the group'), null);
        }

        // Choose a node based on document ID or randomly if no ID
        let nodeIndex = 0;
        if (configuration.url) {
          // Use document URL to consistently route to the same node
          const docId = configuration.url;
          const kid = id.getID(docId);
          const nids = nodes.map(node => id.getNID(node));
          const chosenNID = id.naiveHash(kid, nids);
          nodeIndex = nids.findIndex(nid => nid === chosenNID);
          if (nodeIndex === -1) nodeIndex = 0; // Fallback
        } else {
          // Choose randomly
          nodeIndex = Math.floor(Math.random() * nodes.length);
        }

        const targetNode = nodes[nodeIndex];
        console.log(`Routing indexing request for ${configuration.url || 'document'} to node ${targetNode.ip}:${targetNode.port}`);

        // Send to the local indexer on the chosen node
        // No changes needed here - just pass the configuration as-is
        const remoteConfig = {
          service: 'indexer',
          method: 'index',
          node: targetNode
        };

        local.comm.send([configuration], remoteConfig, (err, val) => {
          if (err) {
            callback(err, null);
            return;
          }
          callback(null, val);
        });
      });
    },
    get_stats: (callback) => {
      callback = callback || cb;
      
      // Get stats from all nodes in the group
      const remoteConfig = {
        service: 'indexer',
        method: 'get_stats'
      };
      
      // Send the request to all nodes in the group
      distribution[context.gid].comm.send([], remoteConfig, (errMap, statsMap) => {
        // Just return the raw node stats - aggregation will be done by the caller
        callback(errMap, statsMap);
      });
    }
  };
}

module.exports = indexer;