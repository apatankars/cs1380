// test-indexer.js
// Test script for the distributed indexer service

const distribution = require('./config.js');
const id = distribution.util.id;
const path = require('path');
const fs = require('fs');

// Define test nodes (we'll use 3 nodes to test distribution)
const nodes = [
  { ip: '127.0.0.1', port: 8001 },
  { ip: '127.0.0.1', port: 8002 },
  { ip: '127.0.0.1', port: 8003 }
];

function isEmptyObject(obj) {
  return obj && typeof obj === 'object' && Object.keys(obj).length === 0;
}

// Sample test document - a plant species with taxonomy
const mockDocumentData = {
  hierarchy: [
    ['kingdom', 'plantae'],
    ['phylum', 'tracheophyta'],
    ['class', 'magnoliopsida'],
    ['order', 'rosales'],
    ['family', 'rosaceae'],
    ['genus', 'rosa'],
    ['species', 'rosa gallica']
  ],
  binomial_name: 'rosa gallica',
  url: '/wiki/Rosa_gallica',
  article_words: [
    'rose', 'rosa', 'gallica', 'plant', 'flower', 'red', 'petals', 'fragrant',
    'garden', 'species', 'cultivated', 'ancient', 'medicine', 'perfume', 'history',
    'europe', 'native', 'taxonomy', 'kingdom', 'plantae', 'phylum', 'tracheophyta',
    'shrub', 'thorns', 'deciduous', 'bloom', 'summer', 'spring', 'cultivation',
    'soil', 'sun', 'water', 'prune', 'disease', 'pest', 'hybrid', 'variety',
    'cultivar', 'fragrance', 'color', 'size', 'height', 'width', 'leaf', 'stem',
    'root', 'fruit', 'seed', 'pollination', 'bees', 'butterflies', 'insects',
    'officinalis', 'medicinal', 'properties', 'essential', 'oil', 'extract'
  ]
};

// Helper function to spawn a node
const spawnNode = (nodeConfig) => {
  return new Promise((resolve, reject) => {
    distribution.local.status.spawn(nodeConfig, (err, val) => {
      if (err) {
        console.error(`Error spawning node ${nodeConfig.ip}:${nodeConfig.port}:`, err);
        reject(err);
      } else {
        console.log(`Node spawned at ${nodeConfig.ip}:${nodeConfig.port}`);
        resolve(val);
      }
    });
  });
};

// Helper function to stop a node
const stopNode = (nodeConfig) => {
  return new Promise((resolve, reject) => {
    distribution.local.comm.send([], { 
      service: 'status', 
      method: 'stop', 
      node: nodeConfig 
    }, (err, val) => {
      if (err) {
        console.error(`Error stopping node ${nodeConfig.ip}:${nodeConfig.port}:`, err);
        reject(err);
      } else {
        console.log(`Node stopped at ${nodeConfig.ip}:${nodeConfig.port}`);
        resolve(val);
      }
    });
  });
};

// Main test function
distribution.node.start(async (server) => {
  console.log('Starting indexer test with', nodes.length, 'nodes');
  
  try {
    // Step 1: Spawn all test nodes
    console.log('Spawning test nodes...');
    for (const node of nodes) {
      await spawnNode(node);
    }
    
    // Step 2: Create the index group
    console.log('Creating index group...');
    const indexGroupConfig = { gid: 'index' };
    const indexGroup = {};
    
    // Add all nodes to the group
    nodes.forEach(node => {
      indexGroup[id.getSID(node)] = node;
    });
    
    // Setup the index group
    await new Promise((resolve, reject) => {
      distribution.local.groups.put(indexGroupConfig, indexGroup, (err, val) => {
        if (err) {
          console.error('Error creating index group:', err);
          reject(err);
        } else {
          console.log('Index group created successfully with', Object.keys(indexGroup).length, 'nodes');
          resolve(val);
        }
      });
    });

    await new Promise((resolve, reject) => {
        distribution.index.groups.put(indexGroupConfig, indexGroup, (err, val) => {
        if (err && !isEmptyObject(err)) {
            // console.log(err)
          console.error('Error creating index service group for index group:', err);
          reject(err);
        } else {
          console.log('Index service group for all indexer nodes created successfully with', Object.keys(indexGroup).length, 'nodes');
          resolve(val);
        }
      });
    })
    
    // Step 3: Register the indexer service
    console.log('Registering indexer service...');
    
    // Create a reference to the local indexer
    const indexerService = require('./distribution/local/indexer');
    
    // Register it with the distributed system
    await new Promise((resolve, reject) => {
      distribution.index.routes.put(indexerService, 'indexer', (err, val) => {
        if (err) {
          console.error('Error registering indexer service:', err);
          reject(err);
        } else {
          console.log('Indexer service registered successfully');
          resolve(val);
        }
      });
    });
    
    // Step 4: Process the test document
    console.log('Processing test document:', mockDocumentData.url);

    // First make sure local indexer is registered
    await new Promise((resolve, reject) => {
    distribution.local.routes.put(indexerService, 'indexer', (err, val) => {
        if (err) {
        console.error('Error registering local indexer service:', err);
        reject(err);
        } else {
        console.log('Local indexer service registered successfully');
        resolve(val);
        }
    });
    });

    // Make sure the group is properly initialized with the indexer service
    await new Promise((resolve, reject) => {
    // Check if distribution.index exists
    if (!distribution.index) {
        console.error('Error: index group not initialized properly');
        reject(new Error('Index group not initialized'));
        return;
    }
    
    // Check if indexer service is available
    if (!distribution.index.indexer) {
        console.error('Error: indexer service not available in index group');
        console.log('Available services:', Object.keys(distribution.index));
        reject(new Error('Indexer service not available'));
        return;
    }
    
    resolve();
    });

    // Now process the document
    await new Promise((resolve, reject) => {
    // Call the indexer with our test document
        distribution.index.indexer.index(mockDocumentData, (err, result) => {
            if (err) {
            console.error('Error processing document:', err);
            reject(err);
            } else {
            console.log('Document processed successfully!');
            console.log('Result:', JSON.stringify(result, null, 2));
            resolve(result);
            }
        });
    });
    
    // Step 5: Verify prefix files were created
    console.log('Verifying prefix files...');
    
    // Check each node for prefix files
    for (const node of nodes) {
      await new Promise((resolve) => {
        // First establish a connection to the node
        distribution.local.comm.send([], {
          service: 'status',
          method: 'get',
          node: node
        }, async (err, val) => {
          if (err) {
            console.error(`Could not connect to node ${node.ip}:${node.port}:`, err);
          } else {
            console.log(`\nChecking prefix files on node ${node.ip}:${node.port}...`);
            
            // Get the node ID
            const nodeID = id.getNID(node);
            const groupDir = path.join('store', nodeID, 'index');
            
            try {
              if (fs.existsSync(groupDir)) {
                const files = fs.readdirSync(groupDir);
                const prefixFiles = files.filter(f => f.startsWith('prefix-'));
                
                console.log(`Found ${prefixFiles.length} prefix files:`);
                prefixFiles.forEach(file => {
                  // Extract prefix from filename (prefix-ro.json -> ro)
                  const prefix = file.replace('prefix-', '').replace('.json', '');
                  console.log(`- ${prefix}`);
                });
                
                // If files found, check content of the first one
                if (prefixFiles.length > 0) {
                  const sampleFile = prefixFiles[0];
                  const prefix = sampleFile.replace('prefix-', '').replace('.json', '');
                  
                  // Get the content for this prefix
                  await new Promise((resolvePrefix) => {
                    distribution.index.store.get(`prefix-${prefix}`, (err, val) => {
                      if (err) {
                        console.log(`Error reading prefix data for ${prefix}:`, err);
                      } else {
                        console.log(`\nSample data for prefix '${prefix}':`);
                        // Show term count and first term only for brevity
                        const termCount = Object.keys(val).length;
                        console.log(`- Contains ${termCount} terms`);
                        
                        if (termCount > 0) {
                          const firstTerm = Object.keys(val)[0];
                          const docCount = val[firstTerm].df;
                          console.log(`- Example term: "${firstTerm}" appears in ${docCount} document(s)`);
                          
                          // Show a sample posting
                          const postings = val[firstTerm].postings;
                          const sampleDocId = Object.keys(postings)[0];
                          if (sampleDocId) {
                            console.log(`- Sample posting: ${sampleDocId}`);
                            // Display a few ranking metrics
                            const ranking = postings[sampleDocId].ranking;
                            if (ranking) {
                              console.log(`  - Term frequency: ${ranking.tf.toFixed(4)}`);
                              console.log(`  - Final score: ${ranking.score.toFixed(4)}`);
                            }
                          }
                        }
                      }
                      resolvePrefix();
                    });
                  });
                }
              } else {
                console.log(`No prefix directory found at ${groupDir}`);
              }
            } catch (error) {
              console.error('Error checking prefix files:', error);
            }
          }
          resolve();
        });
      });
    }
    
    console.log('\nTest completed successfully!');
    
  } catch (error) {
    console.error('Test failed:', error);
  } finally {
    // Clean up resources
    console.log('\nCleaning up resources...');
    
    try {
      // Stop all nodes
      for (const node of nodes) {
        await stopNode(node);
      }
    } catch (cleanupError) {
      console.error('Error during cleanup:', cleanupError);
    }
    
    // Close server
    server.close(() => {
      console.log('Server closed');
      process.exit(0);
    });
  }
});