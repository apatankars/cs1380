const distribution = require("../../config");

const groups = function(config) {
  const context = {};
  context.gid = config.gid || 'all';

  const cb = (error, value) => {
    if (error) {
      console.error(error);
    } else {
      console.log(value);
    }
  }

  return {
    put: (config, group, callback) => {
      callback = callback || cb;
      if (typeof config === 'object') {
        if (config.gid) {
          config = config.gid;
        } 
      } else if (typeof config !== 'string') {
        return callback(new Error('Invalid group name'));
      }
      if (typeof group !== 'object') {
        return callback(new Error('Invalid group object'));
      }
      const remoteConfig = {
        service: 'groups',
        method: 'put',
      }
      const message = [config, group];
      distribution[context.gid].comm.send(message, remoteConfig, (errMap, resMap) => {
        if (Object.keys(errMap).length > 0) {
          callback(errMap, null);
          return;
        }
        callback(null, `Added group ${config} to all nodes within the ${context.gid} group`);
      });
    },

    del: (name, callback) => {
      callback = callback || cb;
      if (typeof name === 'object') {
        if (name.gid) {
          name = name.gid;
        } 
      } else if (typeof name !== 'string') {
        return callback(new Error('Invalid group name'));
      }
      const remoteConfig = {
        service: 'groups',
        method: 'del',
      }
      const message = [name];
      distribution[context.gid].comm.send(message, remoteConfig, (errMap, resMap) => {
        if (Object.keys(errMap).length > 0) {
          callback(errMap, null);
          return;
        }
        callback(null, `Removed group ${name} from all nodes within the ${context.gid} group`);
      });
    },

    get: (name, callback) => {
      callback = callback || cb;
      if (typeof name === 'object') {
        if (name.gid) {
          name = name.gid;
        } 
      } else if (typeof name !== 'string') {
        return callback(new Error('Invalid group name'));
      }
      const remoteConfig = {
        service: 'groups',
        method: 'get',
      }
      const message = [name];
      distribution[context.gid].comm.send(message, remoteConfig, (errMap, resMap) => {
        if (Object.keys(errMap).length > 0) {
          callback(errMap, null);
          return;
        }
        callback(null, resMap);
      });
    },

    add: (name, node, callback) => {
      callback = callback || cb;
      if (typeof name === 'object') {
        if (name.gid) {
          name = name.gid;
        } 
      } else if (typeof name !== 'string') {
        return callback(new Error('Invalid group name'));
      }
      if (typeof node !== 'object' || node.ip === undefined || node.port === undefined) {
        return callback(new Error('Invalid node object'));
      }
      const remoteConfig = {
        service: 'groups',
        method: 'add',
      }
      const message = [name, node];
      distribution[context.gid].comm.send(message, remoteConfig, (errMap, resMap) => {
        if (Object.keys(errMap).length > 0) {
          callback(errMap, null);
          return;
        }
        callback(null, `Added node ${node} to group ${name}`);
      });
    },

    rem: (name, node, callback) => {
      callback = callback || cb;
      if (typeof name === 'object') {
        if (name.gid) {
          name = name.gid;
        } 
      } else if (typeof name !== 'string') {
        return callback(new Error('Invalid group name'));
      }
      if (typeof node !== 'object' || node.ip === undefined || node.port === undefined) {
        return callback(new Error('Invalid node object'));
      }
      const remoteConfig = {
        service: 'groups',
        method: 'rem',
      }
      const message = [name, node];
      distribution[context.gid].comm.send(message, remoteConfig, (errMap, resMap) => {
        if (Object.keys(errMap).length > 0) {
          callback(errMap, null);
          return;
        }
        callback(null, `Removed node ${node} from group ${name}`);
      });
    },
  };
};

module.exports = groups;
