/** @typedef {import("../types").Callback} Callback */

// This table will map from a service name to its configuration
// This will now be a mapping from gid -> service name -> configuration
// This is a global variable that will be used to store the routes
// The type nesting is string -> string -> object
global.routesTable = {};

const cb = (e, v) => {
  if (e) {
    console.error(e);
  } else {
    console.log(v);
  }
};

/**
 * @param {string} configuration
 * @param {Callback} callback
 * @return {void}
 */
function get(configuration, callback) {
  callback = callback || cb;
  let gid = 'local';
  let service = 'status';
  if (configuration === undefined || configuration === null) {
    callback(null, global.routesTable[gid][configuration]);
  }
  if (typeof configuration === "object") {
    if (configuration.gid) {
      gid = configuration.gid;
    }
    if (configuration.service) {
      service = configuration.service;
    }
  } else if (typeof configuration === "string") {
    service = configuration;
  }
  if (
    !global.routesTable || 
    !global.routesTable[gid] || 
    global.routesTable[gid][service] === undefined || 
    global.routesTable[gid][service] === null
  ) {
    callback(new Error(`Service ${service} in the ${gid} group does not exist`), null);
    return;
  } else {
    callback(null, global.routesTable[gid][service]);
  }
}

/**
 * @param {object} service
 * @param {string} configuration
 * @param {Callback} callback
 * @return {void}
 */
function put(service, configuration, callback) {
  callback = callback || cb;
  let gid = "local";
  if (service === undefined || service === null) {
    if (configuration && configuration.service) {
      if (configuration.gid) {
        gid = configuration.gid;
      }
      service = configuration.service;
    } else {
      return callback(new Error("Service cannot be null or undefined"), null);
    }
  } else if (typeof service !== "object") {
    callback(new Error("Service must be an object"), null);
    return;
  }
  if (configuration === undefined || configuration === null) {
    callback(new Error("Configuration cannot be null or undefined"), null);
    return;
  } else if (typeof configuration === "object") {
    if (configuration.gid) {
      gid = configuration.gid;
    }
    if (configuration.service) {
      configuration = configuration.service;
    }
  } else if (typeof configuration === "string") {
    configuration = configuration;
  } else {
    callback(new Error("Configuration must be provided"), null);
    return;
  }
  if (!global.routesTable[gid]) {
    global.routesTable[gid] = {};
  }
  global.routesTable[gid][configuration] = service;
  callback(null, `Successfully added service ${configuration} to the ${gid} group`);
}

/**
 * @param {string} configuration
 * @param {Callback} callback
 */
function rem(configuration, callback) {
  callback = callback || cb;
  let gid = "local";
  if (!configuration) {
    callback(null, "");
  }
  if (typeof configuration === "object") {
    if (configuration.gid) {
      gid = configuration.gid;
    }
    if (configuration.service) {
      configuration = configuration.service;
    }
  } else if (typeof configuration === "string") {
    configuration = configuration;
  }
  if (
    !global.routesTable || 
    !global.routesTable[gid] || 
    global.routesTable[gid][configuration] === undefined || 
    global.routesTable[gid][configuration] === null
  ) {
    callback(new Error(`Service ${configuration} for the ${gid} group does not exist`), null);
    return;
  } else {
    delete global.routesTable[gid][configuration];
    callback(null, `Successfully removed service ${configuration}`);
  }
}

module.exports = { get, put, rem };
