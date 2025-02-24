
function store(config) {
  const context = {};
  context.gid = config.gid || 'all';
  context.hash = config.hash || global.distribution.util.id.naiveHash;

  /* For the distributed store service, the configuration will
          always be a string */
  return {
    get: (configuration, callback) => {
    },

    put: (state, configuration, callback) => {
    },

    del: (configuration, callback) => {
    },

    reconf: (configuration, callback) => {
    },
  };
};

module.exports = store;
