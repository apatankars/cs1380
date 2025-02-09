/** @typedef {import("../types").Callback} Callback */

// This table will map from a service name to its configuration
// {string: {string: function}}
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
    if (configuration === undefined || configuration === null) {
        configuration = 'status';
    }
    if (global.routesTable[configuration] === undefined) {
        callback(new Error(`Service ${configuration} not found`), null);
        return;
    } else {
        callback(null, global.routesTable[configuration]);
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
    if (service === undefined || service === null) {
        if (configuration && configuration.service) {
            service = configuration.service;
        } else {
        callback(new Error('Service cannot be null or undefined'), null);
        return;
        }
    }
    if (configuration === undefined || configuration === null) {
        configuration = "unknown";
    }
    global.routesTable[configuration] = service;
    callback(null, "");
}

/**
 * @param {string} configuration
 * @param {Callback} callback
 */
function rem(configuration, callback) {
    if (!configuration) {
        callback(null, "");
    }
    if (global.routesTable[configuration] === undefined) {
        callback(new Error(`Service ${configuration} does not exist`), null);
        return;
    } else {
        delete global.routesTable[configuration];
        callback(null, `Successfully removed service ${configuration}`);
    }
};

module.exports = {get, put, rem};
