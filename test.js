const distribution = require('./config.js');
const LZ = require('lz-string');
const fs = require('fs');

// console.log(distribution.util.serialize({ a: {c: {d: { e: { f: 2}}}}, b: 2 }));
const storeDir = '/usr/src/app/store';
const path = require('path');

let filePath = path.join(storeDir, "0e5f323d277680570b1ec139dd2122191f0c5dc289f0ba06917e07b4ac4441e5",
  "index", 
  "-wiki--C3-97-Aegilotriticum-erebunii.json"
);

// const fileContent = fs.readFileSync(filePath, 'utf8');

// console.log("Processing file:", filePath);

// let deser = distribution.util.deserialize(fileContent);

// console.log("Deserialized content:", deser.url);

console.log(distribution.util.id.getNID({ ip: "127.0.0.1", port: 7111 }))
console.log(distribution.util.id.getNID({ ip: "127.0.0.1", port: 7112 }))
console.log(distribution.util.id.getNID({ ip: "127.0.0.1", port: 7113 }))


