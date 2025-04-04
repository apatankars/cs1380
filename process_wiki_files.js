const fs = require('fs');
const path = require('path');
const LZString = require('lz-string');
const crypto = require('crypto');

// Try to import the serialization module first
let externalSerialize;
try {
  const serializationModule = require('@brown-ds/distribution/distribution/util/serialization');
  externalSerialize = serializationModule.serialize;
  console.log('Using external serialization module.');
} catch (error) {
  console.log('External serialization module not found. Using internal serialization function.');
  externalSerialize = null;
}

// Internal serialization function as fallback
function internalSerialize(object, seen = new Map()) {
  function getSerialized(type, value) {
    return JSON.stringify({type: type, value: value});
  }
  
  const native_func = {'error': "console.error", 'warn': "console.warn", 'readFile': "fs.readFile", 'getOSType': "os.type"};
  
  if (seen.has(object)) {
    return getSerialized('register', seen.get(object));
  }
  
  if (typeof object === 'string') {
    return getSerialized('string', object);
  }
  
  if (typeof object === 'number') {
    return getSerialized('number', object.toString());
  }
  
  if (typeof object === 'boolean') {
    return getSerialized('boolean', object.toString());
  }
  
  if (object === null) {
    return getSerialized('null', '');
  }
  
  if (object === undefined) {
    return getSerialized('undefined', '');
  }
  
  if (typeof object === 'function') {
    if (object.name === 'log' && object.toString().includes("[native code")) {
      return getSerialized('native', "console.log");
    } else if (native_func[object.name]) {
      return getSerialized('native', native_func[object.name]);
    }
    return getSerialized('function', object.toString());
  }
  
  if (typeof object === 'object') {
    const id = crypto.randomUUID();
    seen.set(object, id);
    let serialized = {id: id, type: null, value: {}};
    
    if (Array.isArray(object)) {
      serialized.type = "array";
      for (let i = 0; i < object.length; i++) {
        serialized.value[i] = internalSerialize(object[i], seen);
      }
      return JSON.stringify(serialized);
    } else if (object instanceof Date) {
      serialized.type = "date";
      serialized.value = object.toISOString();
      return JSON.stringify(serialized);
    } else if (object instanceof Error) {
      serialized.type = "error";
      serialized.value = {
        type: "object",
        value: {
          name: getSerialized('string', object.name),
          message: getSerialized('string', object.message),
          cause: object.cause ? getSerialized('string', object.cause) : getSerialized('undefined', ''),
        }
      };
      return JSON.stringify(serialized);
    } else {
      serialized.type = "object";
      for (let key in object) {
        serialized.value[key] = internalSerialize(object[key], seen);
      }
      return JSON.stringify(serialized);
    }
  }
  
  throw new Error(`Unknown type: ${typeof object}`);
}

// Choose the appropriate serialization function
const serialize = externalSerialize || internalSerialize;

// Function to parse article data from decompressed string
function parseArticleData(rawString) {
  try {
    // Find where the actual JSON begins (after "string ")
    const jsonStartIndex = rawString.indexOf('{"hierarchy"');
    if (jsonStartIndex === -1) {
      console.error("Could not find JSON data in the string");
      return null;
    }

    // Extract just the JSON part
    const jsonString = rawString.substring(jsonStartIndex);

    // Parse it into an object
    const articleObject = JSON.parse(jsonString);
    return articleObject;
  } catch (error) {
    console.error("Error parsing JSON:", error);
    console.log("First 100 chars:", rawString.substring(0, 100));
    return null;
  }
}

// Function to process a single file
async function processFile(filePath) {
  try {
    // Read the file content
    const fileContent = fs.readFileSync(filePath, 'utf8');
    
    // Parse the JSON
    const parsedData = JSON.parse(fileContent);
    
    // Ensure the data is in the expected format
    if (!parsedData || typeof parsedData !== 'object' || parsedData.type !== 'string' || !parsedData.value) {
      console.error(`File ${filePath} does not have the expected format`);
      return false;
    }
    
    // Decompress the value
    const decompressedValue = LZString.decompressFromBase64(parsedData.value);
    
    // Parse the decompressed data
    const articleObject = parseArticleData(decompressedValue);
    
    if (!articleObject) {
      console.error(`Failed to parse article data for ${filePath}`);
      return false;
    }
    
    // Serialize the object using the chosen serialization function
    const serializedData = serialize(articleObject);
    
    // Save to a new file with .json extension
    const newFilePath = filePath + '.json';
    fs.writeFileSync(newFilePath, serializedData);
    
    console.log(`Successfully processed ${filePath} -> ${newFilePath}`);
    return true;
  } catch (error) {
    console.error(`Error processing file ${filePath}:`, error);
    return false;
  }
}

// Function to process a directory recursively
async function processDirectory(directoryPath) {
  try {
    // Get all items in the directory
    const items = fs.readdirSync(directoryPath);
    
    for (const item of items) {
      const itemPath = path.join(directoryPath, item);
      const stats = fs.statSync(itemPath);
      
      if (stats.isDirectory()) {
        // If it's a directory, process it recursively
        await processDirectory(itemPath);
      } else if (stats.isFile() && path.basename(itemPath).startsWith('-wiki')) {
        // If it's a file starting with '-wiki', process it
        const success = await processFile(itemPath);
        
        // Delete the old file if processing was successful
        if (success) {
          fs.unlinkSync(itemPath);
          console.log(`Deleted original file: ${itemPath}`);
        }
      }
    }
  } catch (error) {
    console.error(`Error processing directory ${directoryPath}:`, error);
  }
}

// Main function to start processing
async function main() {
  const storeDir = '/usr/src/app/store';
  
  try {
    // Get all directories in the store
    const items = fs.readdirSync(storeDir);
    
    for (const item of items) {
      const itemPath = path.join(storeDir, item);
      const stats = fs.statSync(itemPath);
      
      // Skip non-directories and the shell script
      if (!stats.isDirectory() || item === 'move_to_index.sh') {
        continue;
      }
      
      // Check if this directory has an 'index' subdirectory
      const indexDir = path.join(itemPath, 'index');
      if (fs.existsSync(indexDir) && fs.statSync(indexDir).isDirectory()) {
        console.log(`Processing index directory: ${indexDir}`);
        await processDirectory(indexDir);
      }
    }
    
    console.log('Processing completed successfully!');
  } catch (error) {
    console.error('Error in main function:', error);
  }
}

// Start the script
main();