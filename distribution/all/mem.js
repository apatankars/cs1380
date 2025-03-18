
const local = distribution.local;
const util = require("../util/util");
const id = util.id;

function mem(config) {
  const context = {};
  context.gid = config.gid || 'all';
  context.hash = config.hash || global.distribution.util.id.naiveHash;

  const cb = (error, value) => {
    if (error) {
      console.error(error);
    } else {
      console.log(value);
    }
  };

  function getChosenNode(configuration, callback) {
    distribution.local.groups.get(context.gid, (err, group) => {
        if (err) {
          return null;
        }

      const nodeConfigs = Object.values(group); // an array of {ip, port} objects
      const nids = nodeConfigs.map((nc) => id.getNID(nc));

      const kid = id.getID(configuration);
      const chosenNID = context.hash(kid, nids);

      let chosenNode = nodeConfigs.find((nc) => id.getNID(nc) === chosenNID);
      callback(null, chosenNode);
    });
  }

  /* For the distributed mem service, the configuration will
          always be a string */
  return {
    get: (configuration, callback) => {
      callback = callback || cb;
      if (configuration === undefined || configuration === null ){
        callback(new Error('Configuration is required'), null);
        return;
      }

      getChosenNode(configuration, (err, chosenNode) => {
        if (err) return callback(new Error('Could not find a node'), null);

        const config = {
          service: 'mem',
          method: 'get',
          node: chosenNode
        };

        const messageConfig = {
          key: configuration,
          gid: context.gid
        }

        const message = [messageConfig];

        local.comm.send(message, config, (err, val) => {
          if (err) {
            callback(err, null);
            return;
          }
          callback(null, val);
        });
      });
    },

    put: (state, configuration, callback) => {
      callback = callback || cb;
      if (state === undefined || state === null ){
        callback(new Error('State is required'), null);
        return;
      }

      if (configuration === null) {
        configuration = id.getID(state);
      }

      // 3) Get the correct node
      getChosenNode(configuration , (err, chosenNode) => {
        if (err) return callback(new Error('Could not find a node'), null);
        // 6) Send the key to the chosen node
        const config = {
          service: 'mem',
          method: 'put',
          node: chosenNode
        };

        let messageConfig = {
          key: configuration,
          gid: context.gid
        }

        const message = [state, messageConfig];

        local.comm.send(message, config, (err, val) => {
          if (err) {
            callback(err, null);
            return;
          }
          callback(null, val);
        });
      });
    },

    del: (configuration, callback) => {
      callback = callback || cb;
      if (configuration === undefined || configuration === null ){
        callback(new Error('Configuration is required'), null);
        return;
      }
      getChosenNode(configuration, (err, chosenNode) => {
        if (err) return callback(new Error('Could not find a node'), null);

        const config = {
          service: 'mem',
          method: 'del',
          node: chosenNode
        };

        const messageConfig = {
          key: configuration,
          gid: context.gid
        }

        const message = [messageConfig];

        local.comm.send(message, config, (err, val) => {
          if (err) {
            callback(err, null);
            return;
          }
          callback(null, val);
        });
      });
    },

    reconf: (configuration, callback) => {
    },
  };
};

module.exports = mem;
