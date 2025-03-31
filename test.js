function append(state, configuration, callback) {
  let nodeConfig = global.nodeConfig;
  let nodeID = util.id.getNID(nodeConfig);
  let key;
  let gid = 'local';
  
  // If no state is given, error out
  if (!state) {
    return callback(new Error('No state to append'), null);
  }
  
  // Parse configuration to get key and gid
  if (configuration === null || configuration === undefined) {
    key = util.id.getID(state);
  } else if (typeof configuration === 'object') {
    if (configuration.key) {
      key = configuration.key;
    }
    if (configuration.gid) {
      gid = configuration.gid;
    }
    if (!key) {
      key = util.id.getID(state);
    }
  } else if (typeof configuration === 'string') {
    key = configuration;
  } else {
    key = util.id.getID(state);
  }

  console.log(`${nodeConfig.port}: Appending to key ${key} in group ${gid}`);
  
  // Build directory path
  const groupDir = path.join('store', nodeID, gid);
  fs.mkdirSync(groupDir, { recursive: true });
  
  const filePath = path.join(groupDir, sanitizeKey(key) + '.json');
  
  // Check if file exists
  if (fs.existsSync(filePath)) {
    // Read existing data
    fs.readFile(filePath, 'utf8', (err, data) => {
      if (err) return callback(err, null);
      
      let existingData;
      try {
        const parsedData = JSON.parse(data);
        existingData = util.deserialize(parsedData);
        
        // Ensure existing data is an object we can work with
        if (typeof existingData !== 'object' || existingData === null) {
          existingData = { value: existingData };
        }
      } catch (e) {
        return callback(new Error('Error parsing existing data: ' + e.message), null);
      }
      
      // Ensure state is an object
      const stateObj = typeof state === 'object' && state !== null ? state : { value: state };
      
      // Create a deep copy to avoid modifying the original
      let result = JSON.parse(JSON.stringify(existingData));
      
      // More careful merging logic
      Object.keys(stateObj).forEach(key => {
        if (!result.hasOwnProperty(key)) {
          // Key doesn't exist yet, add it
          result[key] = [stateObj[key]];
        } else if (!Array.isArray(result[key])) {
          // Value exists but isn't an array, convert to array with both values
          result[key] = [result[key], stateObj[key]];
        } else {
          // Already an array, append the new value
          result[key].push(stateObj[key]);
        }
      });
      
      // Serialize and save the updated result
      const serialized = util.serialize(result);
      const value = JSON.stringify(serialized);
      
      fs.writeFile(filePath, value, (err) => {
        if (err) return callback(err, null);
        return callback(null, result);
      });
    });
  } else {
    // If file doesn't exist, create a new one
    put(state, configuration, callback);
  }
}

module.exports = {put, get, getGroupKeys, del, append};
