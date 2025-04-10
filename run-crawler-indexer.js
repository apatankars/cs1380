// test-crawler-indexer.js
// Test script for the distributed crawler and indexer

const distribution = require('./config.js');
const id = distribution.util.id;
const path = require('path');
const fs = require('fs');

// Define test nodes - adjust IP/ports as needed for your environment
const crawl_nodes = [
  { ip: '127.0.0.1', port: 8001 },
  { ip: '127.0.0.1', port: 8002 },
  { ip: '127.0.0.1', port: 8003 }
];

const index_nodes = [
    { ip: '127.0.0.1', port: 8004 },
  { ip: '127.0.0.1', port: 8005 },
  { ip: '127.0.0.1', port: 8006 },
  { ip: '127.0.0.1', port: 8007 },
  { ip: '127.0.0.1', port: 8008 }
];
const nodes = [
  ...crawl_nodes,
  ...index_nodes
]

// Helper function to check if an object is empty
function isEmptyObject(obj) {
  return obj && typeof obj === 'object' && Object.keys(obj).length === 0;
}

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

// Helper function to create a group
const createGroup = async (groupName, nodes) => {
  const groupConfig = { gid: groupName };
  const group = {};
  
  nodes.forEach(node => {
    group[id.getSID(node)] = node;
  });
  
  // Create the group locally
  await new Promise((resolve, reject) => {
    distribution.local.groups.put(groupConfig, group, (err, val) => {
      if (err) {
        console.error(`Error creating local ${groupName} group:`, err);
        reject(err);
      } else {
        console.log(`Local ${groupName} group created successfully`);
        resolve(val);
      }
    });
  });
  
  // Create the group in the distributed system
  await new Promise((resolve, reject) => {
    // Check if the group service exists
    if (!distribution[groupName]) {
      console.error(`Error: ${groupName} group not initialized properly`);
      reject(new Error(`${groupName} group not initialized`));
      return;
    }
    
    distribution[groupName].groups.put(groupConfig, group, (err, val) => {
      if (err && !isEmptyObject(err)) {
        console.error(`Error creating distributed ${groupName} group:`, err);
        reject(err);
      } else {
        console.log(`Distributed ${groupName} group created successfully`);
        resolve(val);
      }
    });
  });
  
  return { groupConfig, group };
};

const createGroupOnGroup = async (originGroupName, groupName, nodes) => {
    const groupConfig = { gid: groupName };
  const group = {};
  
  nodes.forEach(node => {
    group[id.getSID(node)] = node;
  });

  // Create the group in the distributed system
  await new Promise((resolve, reject) => {
    // Check if the group service exists
    if (!distribution[originGroupName]) {
      console.error(`Error: ${originGroupName} group not initialized properly`);
      reject(new Error(`${originGroupName} group not initialized`));
      return;
    }
    
    distribution[originGroupName].groups.put(groupConfig, group, (err, val) => {
      if (err && !isEmptyObject(err)) {
        console.error(`Error creating distributed ${groupName} group:`, err);
        reject(err);
      } else {
        console.log(`Distributed ${groupName} group created successfully`);
        resolve(val);
      }
    });
  });
}

// Helper to get crawlerStats from the crawler
const getCrawlerStats = () => {
  return new Promise((resolve, reject) => {
    distribution.taxonomy.crawler.get_stats((err, crawlerStats) => {
      if (err && !isEmptyObject(err)) {
        console.error("Error getting crawler crawlerStats:", err);
        reject(err);
      } else {
        resolve(crawlerStats);
      }
    });
  });
};

const getIndexerStats = () => {
    return new Promise((resolve, reject) => {
        distribution.index.indexer.get_stats((err, indexerStats) => {
            if (err && !isEmptyObject(err)) {
                console.error("Error getting indexer indexerStats:", err);
                reject(err);
            } else {
                resolve(indexerStats);
            }
        })
    })
}

// Helper to get aggregated metrics from crawler and indexer crawlerStats
const aggregateMetrics = (crawlerStats, indexerStats) => {
  const aggregated = {
    crawling: {
      totalPagesProcessed: 0,
      totalBytesDownloaded: 0,
      avgProcessingTime: 0,
      totalTermsExtracted: 0,
      totalBytesTransferred: 0,
    },
    indexing: {
      totalDocumentsIndexed: 0,
      avgIndexTime: 0
    },
    links: {
      totalToCrawl: 0,
      totalCrawled: 0,
      totalTargetsFound: 0
    }
  };
  
  let totalCrawlTime = 0;
  let totalIndexTime = 0;
  
  // Process node crawlerStats
  for (const nodeId in crawlerStats) {
    if (!crawlerStats[nodeId]) continue;
    
    // Links crawlerStats
    aggregated.links.totalToCrawl += crawlerStats[nodeId].links_to_crawl || 0;
    aggregated.links.totalCrawled += crawlerStats[nodeId].crawled_links || 0;
    
    // Process metrics
    if (crawlerStats[nodeId].metrics) {
      const crawlerMetrics = crawlerStats[nodeId].metrics;
      
      // Crawling metrics
      if (crawlerMetrics.crawling) {
        aggregated.crawling.totalPagesProcessed += crawlerMetrics.crawling.pagesProcessed || 0;
        aggregated.crawling.totalBytesDownloaded += crawlerMetrics.crawling.bytesDownloaded || 0;
        aggregated.links.totalTargetsFound += crawlerMetrics.crawling.targetsHit || 0;
        totalCrawlTime += crawlerMetrics.crawling.totalCrawlTime || 0;
        aggregated.crawling.totalBytesTransferred += crawlerMetrics.crawling.bytesTransferred || 0;
        aggregated.crawling.totalTermsExtracted += crawlerMetrics.crawling.termsExtracted || 0;
      }
    }
  }

  for (const nodeId in indexerStats) {
    if (!indexerStats[nodeId]) continue;

    if (indexerStats[nodeId].metrics) {
        const indexerMetrics = indexerStats[nodeId].metrics;

        if (indexerMetrics) {
            aggregated.indexing.totalDocumentsIndexed += indexerMetrics.documentsIndexed || 0;
            totalIndexTime += indexerMetrics.totalIndexTime || 0;
        }
    }



  }
  
  // Calculate averages
  if (aggregated.crawling.totalPagesProcessed > 0) {
    aggregated.crawling.avgProcessingTime = totalCrawlTime / aggregated.crawling.totalPagesProcessed;
  }
  
  if (aggregated.indexing.totalDocumentsIndexed > 0) {
    aggregated.indexing.avgIndexTime = totalIndexTime / aggregated.indexing.totalDocumentsIndexed;
  }
  
  return aggregated;
};

// Main test function
distribution.node.start(async (server) => {
  console.log("Starting crawler-indexer test with", nodes.length, "nodes");
  let metricsInterval = null;
  
  try {
    // Step 1: Spawn all test nodes
    console.log("Spawning test nodes...");
    for (const node of nodes) {
      await spawnNode(node);
    }
    
    // Step 2: Create the taxonomy group
    console.log("Creating taxonomy group...");
    const { groupConfig: taxonomyConfig, group: taxonomyGroup } = await createGroup('taxonomy', crawl_nodes);
    
    // Step 3: Create the index group with the same nodes
    console.log("Creating index group...");
    const { groupConfig: indexConfig, group: indexGroup } = await createGroup('index', index_nodes);

    console.log("Creating index group on taxonomy group...");
    await createGroupOnGroup('taxonomy', 'index', index_nodes);

    console.log("Creating taxonomy group on index group...");
    await createGroupOnGroup('index', 'taxonomy', crawl_nodes);
    
    // Step 4: Setup global info for all nodes
    // console.log("Setting up global info...");
    // await new Promise((resolve, reject) => {
    //   const globalInfo = {
    //     nodes: nodes,
    //     num_nodes: nodes.length
    //   };
      
    //   distribution.taxonomy.mem.put(globalInfo, 'global_info', (err, val) => {
    //     if (err) {
    //       console.error("Error setting global info:", err);
    //       reject(err);
    //     } else {
    //       console.log("Global info set successfully");
    //       resolve(val);
    //     }
    //   });
    // });
    
    // Step 5: Initialize the crawler service
    console.log("Initializing crawler service...");
    await new Promise((resolve, reject) => {
      distribution.taxonomy.crawler.initialize((err, result) => {
        if (err) {
          console.error("Error initializing crawler:", err);
          reject(err);
        } else {
          console.log("Crawler initialized successfully");
          resolve(result);
        }
      });
    });
    
    // Step 6: Add seed links to start crawling
    console.log("Adding seed links for crawling...");
    const seedLinks = [
      '/wiki/Cnidaria',    // Sea life
      '/wiki/Plantae',     // Plants
      '/wiki/Fungi',       // Fungi
      '/wiki/Lepidoptera'  // Butterflies
    ];
    
    for (const link of seedLinks) {
      await new Promise((resolve) => {
        distribution.taxonomy.crawler.add_link_to_crawl(link, (err, result) => {
          if (err) {
            console.error(`Error adding seed link ${link}:`, err);
          } else {
            console.log(`Seed link ${link} added successfully`);
          }
          resolve();
        });
      });
    }
    
    // Step 7: Start periodic metrics reporting
    console.log("\n=== Starting Crawler-Indexer System ===");
    console.log("Press Ctrl+C to stop the test");
    
    // Setup periodic crawling
    const crawlInterval = setInterval(() => {
      distribution.taxonomy.crawler.crawl_one((err, result) => {
        if (err && !isEmptyObject(err)) {
          console.error("Error during crawl iteration:", err);
        } else if (result && Object.keys(result).length > 0) {
          // At least one node performed a successful crawl
          const successCount = Object.values(result).filter(r => 
            r && r.status === 'success').length;
          
          if (successCount > 0) {
            console.log(`Crawl iteration completed: ${successCount} pages processed`);
          }
        }
      });
    }, 2000); // Crawl every 2 seconds
    
    // Save maps every 30 seconds
    const saveInterval = setInterval(() => {
      distribution.taxonomy.crawler.save_maps_to_disk((err, result) => {
        if (err && !isEmptyObject(err)) {
          console.error("Error saving crawler data:", err);
        } else {
          console.log("Crawler data saved to disk");
        }
      });
    }, 30000);
    
    // Report metrics every 10 seconds
    metricsInterval = setInterval(async () => {
      try {
        const crawlerStats = await getCrawlerStats();
        const indexerStats = await getIndexerStats();
        const metrics = aggregateMetrics(crawlerStats, indexerStats);
        
        console.log("\n=== System Metrics ===");
        console.log(`Time: ${new Date().toISOString()}`);
        console.log(`Links in queue: ${metrics.links.totalToCrawl}`);
        console.log(`Links crawled: ${metrics.links.totalCrawled}`);
        console.log(`Target pages found: ${metrics.links.totalTargetsFound}`);
        console.log(`Pages processed: ${metrics.crawling.totalPagesProcessed}`);
        console.log(`Documents indexed: ${metrics.indexing.totalDocumentsIndexed}`);
        console.log(`Terms extracted: ${metrics.crawling.totalTermsExtracted}`);
        console.log(`Data downloaded: ${(metrics.crawling.totalBytesDownloaded/1024/1024).toFixed(2)}MB`);
        console.log(`Avg processing time: ${metrics.crawling.avgProcessingTime.toFixed(2)}ms`);
        console.log(`Avg indexing time: ${metrics.indexing.avgIndexTime.toFixed(2)}ms`);
        console.log("======================\n");
      } catch (err) {
        console.error("Error getting metrics:", err);
      }
    }, 10000);
    
    // Handle graceful shutdown on SIGINT (Ctrl+C)
    process.on('SIGINT', async () => {
      console.log("\nShutting down test...");
      clearInterval(crawlInterval);
      clearInterval(saveInterval);
      clearInterval(metricsInterval);
      
      // Clean up crawler resources
      await new Promise((resolve) => {
        distribution.taxonomy.crawler.cleanup((err, result) => {
          if (err) {
            console.error("Error cleaning up crawler:", err);
          } else {
            console.log("Crawler cleaned up successfully");
          }
          resolve();
        });
      });
      
      // Get final metrics
      try {
        const crawlerStats = await getCrawlerStats();
        const indexerStats = await getIndexerStats();
        const metrics = aggregateMetrics(crawlerStats, indexerStats);
        
        console.log("\n=== Final Metrics ===");
        console.log(`Links crawled: ${metrics.links.totalCrawled}`);
        console.log(`Target pages found: ${metrics.links.totalTargetsFound}`);
        console.log(`Pages processed: ${metrics.crawling.totalPagesProcessed}`);
        console.log(`Documents indexed: ${metrics.indexing.totalDocumentsIndexed}`);
        console.log(`Terms extracted: ${metrics.crawling.totalTermsExtracted}`);
        console.log(`Data downloaded: ${(metrics.crawling.totalBytesDownloaded/1024/1024).toFixed(2)}MB`);
        console.log(`Avg processing time: ${metrics.crawling.avgProcessingTime.toFixed(2)}ms`);
        console.log(`Avg indexing time: ${metrics.indexing.avgIndexTime.toFixed(2)}ms`);
        console.log("======================\n");
      } catch (err) {
        console.error("Error getting final metrics:", err);
      }
      
      // Stop all nodes
      for (const node of nodes) {
        await stopNode(node);
      }
      
      // Close server
      server.close(() => {
        console.log("Server closed");
        process.exit(0);
      });
    });
    
  } catch (error) {
    console.error("Test failed:", error);
    
    // Clean up on error
    clearInterval(metricsInterval);
    
    for (const node of nodes) {
      try {
        await stopNode(node);
      } catch (err) {
        console.error(`Error stopping node ${node.ip}:${node.port}:`, err);
      }
    }
    
    server.close(() => {
      console.log("Server closed due to error");
      process.exit(1);
    });
  }
});