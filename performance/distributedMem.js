#!/usr/bin/env node

const distribution = require('../config.js');
const id = distribution.util.id;

// Slice off the first two elements (node and script path)
const args = process.argv.slice(2);

// Check if an argument was provided
if (args.length === 0) {
  console.error('Please provide an index argument.');
  process.exit(1);
}

const index = args[0];

const n1 = {ip: '127.0.0.1', port: 9001};
const n2 = {ip: '127.0.0.1', port: 9002};
const n3 = {ip: '127.0.0.1', port: 9003};
const n4 = {ip: '127.0.0.1', port: 9004};
const n5 = {ip: '127.0.0.1', port: 9005};
const n6 = {ip: '127.0.0.1', port: 9006};

let spawnNode = null;
distribution.node.start((server) => {
    switch (index) {
        case '1':
            console.log('Running code for index 1');
            spawnNode = n1;
            break;
        case '2':
            console.log('Running code for index 2');
            spawnNode = n2;
            break;
        case '3':
            console.log('Running code for index 3');
            spawnNode = n3;
            break;
        default:
            console.error(`No code associated with index: ${index}`);
            process.exit(1);
        }
        distribution.local.status.spawn(spawnNode, (e, v) => {
            if (e) {
                console.error(e);
                process.exit(1);
            }
            console.log('Spawned node: ', spawnNode);
            
            const groupConfig = {gid: 'mygroup', hash: id.consistentHash};
            setTimeout(() => {
                console.log("Waited 10 seconds");
            }, 10000);
            if (index === '1') {
                const groupObj = {};
                groupObj[id.getSID(n1)] = n1;
                groupObj[id.getSID(n2)] = n2;
                groupObj[id.getSID(n3)] = n3;

                distribution.local.groups.put(groupConfig, groupObj, (e, v) => {
                    if (e) {
                        console.error(e);
                        process.exit(1);
                    }
                    console.log('Group created: ', groupObj);
                    distribution.mygroup.groups.put(groupConfig, groupObj, (e, v) => {
                        if (e) {
                            console.error(e);
                            process.exit(1);
                        }
                        console.log('Group created: ', groupObj);
                    });
                });
            }
            setTimeout(() => {
                console.log("Waited 10 seconds");
            }, 10000);
            distribution.local.groups.get('mygroup', (e, v) => {
                if (e) {
                    console.error(e);
                    process.exit(1);
                }
                console.log('Group retrieved: ', v);
            });
        });
});


