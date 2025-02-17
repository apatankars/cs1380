const status = function(config) {
  const context = {};
  context.gid = config.gid || 'all';

  cb = (error, value) => {
    if (error) {
      console.error(error);
    } else {
      console.log(value);
    }
  }

  return {
    get: (configuration, callback) => {
      callback = callback || cb;
      if (configuration === undefined || configuration === null ){
        callback(new Error('Configuration is required'), null);
        return;
      }
      const remoteConfig = {
        service: 'status',
        method: 'get',
      }
      distribution[context.gid].comm.send([configuration], remoteConfig, (errMap, resMap) => {
        if (Object.keys(errMap).length > 0) {
          callback(errMap, null);
          return;
        }
        if (configuration === 'heapTotal' || configuration === 'heapUsed') {
          const total = 0;
          for (const key in resMap) {
            total += resMap[key];
          }
        }
        callback(null, resMap);
      });
    },

    spawn: (configuration, callback) => {
      callback = callback || cb;
      if (configuration === undefined || configuration === null ){
        callback(new Error('Configuration is required'), null);
        return;
      }
      if (configuration.ip === undefined || configuration.port === undefined) {
        callback(new Error('Invalid configuration provided'), null);
        return;
      }
      distribution.local.status.spawn(configuration, (err, val) => {
        if (err) {
          callback(err, null);
          return;
        }
        const remoteConfig = {
          service: 'groups',
          method: 'add',
        }
        const message = [context.gid, configuration];
        distribution[context.gid].comm.send(message, remoteConfig, (errMap, resMap) => {
          if (Object.keys(errMap).length > 0) {
            callback(errMap, null);
            return;
          }
          callback(null, resMap);
        });
      })
    },

    stop: (callback) => {
      callback = callback || cb;
      const remoteConfig = {
        service: 'status',
        method: 'stop',
      }
      distribution[context.gid].comm.send([], remoteConfig, (errMap, resMap) => {
        if (Object.keys(errMap).length > 0) {
          callback(errMap, null);
          return;
        }
        distribution.local.comm.stop((err, val) => {
          if (err) { 
            callback(err, null);
            return;
          }
          callback(null, val);
        });
      });
    },
  };
};

module.exports = status;
