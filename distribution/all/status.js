const distribution = require("@brown-ds/distribution");

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
      
    },

    spawn: (configuration, callback) => {
    },

    stop: (callback) => {
    },
  };
};

module.exports = status;
