const id = require('../util/id');
const log = require('../util/log');

const status = {
};

global.moreStatus = {
  sid: id.getSID(global.nodeConfig),
  nid: id.getNID(global.nodeConfig),
  counts: 0,
};

const cb = (e, v) => {
  if (e) {
    console.error(e);
  } else {
    log(v);
  }
};

status.get = function(configuration, callback) {
  callback = callback || cb;
  if (!configuration) {
    if (typeof callback !== 'function') {
      configuration = callback;
      callback = cb;
    } else {
      callback(null, "No configuration specified");
      return;
    }
  }
  switch(configuration ) {
    case 'sid':
      callback(null, global.moreStatus.sid);
      break;
    case 'nid':
      callback(null, global.moreStatus.nid);
      break;
    case 'counts':
      callback(null, global.moreStatus.counts);
      break;
    case 'ip':
      callback(null, global.nodeConfig.ip);
      break;
    case 'port':
      callback(null, global.nodeConfig.port);
      break;
    case 'heapTotal':
      callback(null, process.memoryUsage().heapTotal);
      break;
    case 'heapUsed':
      callback(null, process.memoryUsage().heapUsed);
      break;
    default:
      callback(new Error('Status key not found'), null);
  }
};

status.spawn = function(configuration, callback) {
};

status.stop = function(callback) {
};

module.exports = status;
