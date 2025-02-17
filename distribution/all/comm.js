/** @typedef {import("../types").Callback} Callback */

const local = distribution.local;

/**
 * NOTE: This Target is slightly different from local.all.Target
 * @typdef {Object} Target
 * @property {string} service
 * @property {string} method
 */

/**
 * @param {object} config
 * @return {object}
 */
function comm(config) {
  const context = {};
  context.gid = config.gid || 'all';

  /**
   * @param {Array} message
   * @param {object} configuration
   * @param {Callback} callback
   */
  function send(message, configuration, callback) {
    if (message === undefined || message === null) {
      // If no message is provided, we assume the default message is a node id
      message = ['nid'];
    }
    if (configuration === undefined || configuration === null || !configuration.service || !configuration.method
    ) {
      callback(new Error('Remote configuration is required'), null);
      return;
    }
    let service = configuration.service;
    let method = configuration.method;

    let errorMap = {};
    let responseMap = {};
    let responseCount = 0;

    distribution.local.groups.get(context.gid, (err, group) => {
      if (err) {
        callback(err, null);
        return;
      }

      responseCount = Object.keys(group).length;

      Object.entries(group).forEach(([sid, node]) => {
        let config = {
          service: service,
          method: method,
          node: node,
        }
        local.comm.send(message, config, (error, response) => {
          if (error) {
            errorMap[sid] = error;
          } else {
            responseMap[sid] = response;
          }
          responseCount--;
          if (responseCount === 0) {
            // // All responses have been received
            // // Now, if there are any special aggregation logic needed, we can do it here
            // // For example, if the service is status and the method is get and the argument is 'heapTotal' or 'heapUsed'
            // // we can aggregate the results and return the total
            // if (service === 'status' && method === 'get' && (message[0] === 'heapTotal' || message[0] === 'heapUsed')) {
            //   let total = 0;
            //   Object.values(responseMap).forEach((response) => {
            //     total += response;
            //   });
            //   responseMap = {total: total};
            // }
            callback(errorMap, responseMap);
          }
        })
      })
    });

  }

  return {send};
};

module.exports = comm;
