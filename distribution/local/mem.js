
const { all } = require('@brown-ds/distribution');
let util = require('../util/util');
// This will be a memory mapping from 'gid' to 'key' to 'value'
memory = {
    local: {},
    all: {}
};

function put(state, configuration, callback) {
    let key;
    let gid = 'local';
    let value = util.serialize(state);
    if (typeof configuration === 'object') {
        if (configuration.key) {
            key = configuration.key;
        }
        if (configuration.gid) {
            gid = configuration.gid;
        }
    } else if (typeof configuration === 'string') {
        key = configuration;
    } else if (!configuration) {
        key = util.id.getID(value);
    }
    if (!state) {
        callback(new Error('No state to put'));
        return;
    }
    memory[gid][key] = value;
    callback(null, state);
};

function get(configuration, callback) {
    let key;
    let gid = 'local';
    if (typeof configuration === 'object') {
        if (configuration.key) {
            key = configuration.key;
        } if (configuration.gid) {
            gid = configuration.gid;
        }
    } else if (typeof configuration === 'string') {
        key = configuration;
    } else if (configuration === null) {
        // Return all of the keys in the local memory
        console.log('Getting all keys');
        let keys = Object.keys(memory[gid]);
        callback(null, keys);
        return;
    }
    if (!memory[gid][key]) {
        callback(new Error('No value found for key: ' + key));
        return;
    }
    let value = util.deserialize(memory[gid][key]);
    callback(null, value);
}

function del(configuration, callback) {
    let key;
    let gid = 'local';
    if (typeof configuration === 'object') {
        if (configuration.key) {
            key = configuration.key;
        }
        if (configuration.gid) {
            gid = configuration.gid;
        }
    }
    if (typeof configuration === 'string') {
        key = configuration;
    }
    if (!configuration) {
        callback(new Error('No key to delete'));
        return;
    }
    if (!memory[gid][key]) {
        callback(new Error('No value found for key: ' + key));
        return;
    }
    let value = util.deserialize(memory[gid][key]); 
    delete memory[gid][key];
    callback(null, value);
};

module.exports = {put, get, del};
