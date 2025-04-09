// distribution/all/crawler.js
const local = global.distribution.local;
const util = require("../util/util");
const id = util.id;

function crawler(config) {
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
     * Initialize the crawler service across all nodes
     */
    initialize: (callback) => {
      callback = callback || cb;
      
      const remoteConfig = {
        service: 'crawler',
        method: 'initialize'
      };
      
      global.distribution[context.gid].comm.send([], remoteConfig, (errMap, resMap) => {
        if (Object.keys(errMap).length > 0) {
          callback(errMap, null);
        } else {
          callback(null, resMap);
        }
      });
    },

    /**
     * Add a link to the crawl queue on the appropriate node
     */
    add_link_to_crawl: (link, callback) => {
      callback = callback || cb;
      
      global.distribution.local.groups.get(context.gid, (err, group) => {
        if (err) {
          return callback(new Error(`Failed to get group: ${err.message}`), null);
        }
        
        // Get a list of nodes
        const nodes = Object.values(group);
        
        // Determine which node should handle this link
        let targetNodeIndex = 0;
        const linkId = link || '';
        const kid = id.getID(linkId);
        const nids = nodes.map(node => id.getNID(node));
        const chosenNID = id.naiveHash(kid, nids);
        targetNodeIndex = nids.findIndex(nid => nid === chosenNID);
        if (targetNodeIndex === -1) targetNodeIndex = 0;
        
        const targetNode = nodes[targetNodeIndex];
        
        // Send to the target node
        const remoteConfig = {
          service: 'crawler',
          method: 'add_link_to_crawl',
          node: targetNode
        };
        
        local.comm.send([link], remoteConfig, (err, val) => {
          if (err) {
            callback(err, null);
            return;
          }
          callback(null, val);
        });
      });
    },

    /**
     * Crawl one page from each node in the group
     */
    crawl_one: (callback) => {
      callback = callback || cb;
      
      const remoteConfig = {
        service: 'crawler',
        method: 'crawl_one'
      };
      
      global.distribution[context.gid].comm.send([], remoteConfig, (errMap, resMap) => {
        callback(errMap, resMap);
      });
    },

    /**
     * Get statistics from all nodes
     */
    get_stats: (callback) => {
      callback = callback || cb;
      
      const remoteConfig = {
        service: 'crawler',
        method: 'get_stats'
      };
      
      global.distribution[context.gid].comm.send([], remoteConfig, (errMap, resMap) => {
        callback(errMap, resMap);
      });
    },

    /**
     * Save maps to disk on all nodes
     */
    save_maps_to_disk: (callback) => {
      callback = callback || cb;
      
      const remoteConfig = {
        service: 'crawler',
        method: 'save_maps_to_disk'
      };
      
      global.distribution[context.gid].comm.send([], remoteConfig, (errMap, resMap) => {
        callback(errMap, resMap);
      });
    },

    /**
     * Clean up resources on all nodes
     */
    cleanup: (callback) => {
      callback = callback || cb;
      
      const remoteConfig = {
        service: 'crawler',
        method: 'cleanup'
      };
      
      global.distribution[context.gid].comm.send([], remoteConfig, (errMap, resMap) => {
        callback(errMap, resMap);
      });
    }
  };
}

module.exports = crawler;