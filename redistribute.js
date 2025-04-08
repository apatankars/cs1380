const distribution = require("./config.js");
const util = distribution.util;
const id = util.id;
const fs = require('fs');
const path = require('path');

// Configuration - customize these values
const SOURCE_GROUP_ID = "index";       // Group ID to read from
const DESTINATION_GROUP_ID = "tfidf";  // Group ID to write to
const BATCH_SIZE = 500;                // Number of keys to process in each batch
const CONCURRENT_KEYS = 50;            // Number of keys to process concurrently
const METRICS_INTERVAL = 30000;        // Report metrics every 30 seconds (in milliseconds)
const MAX_MEMORY_USAGE_PERCENT = 80;   // Maximum memory usage before triggering GC (percent)

// Set up nodes for distributed processing
const inNodes = [
  { ip: "127.0.0.1", port: 7112 },
  { ip: "127.0.0.1", port: 7113 },
  { ip: "127.0.0.1", port: 7114 },
  { ip: "127.0.0.1", port: 7115 },
];

const outNodes = [
  { ip: "127.0.0.1", port: 8110 },
  { ip: "127.0.0.1", port: 8111 },
  { ip: "127.0.0.1", port: 8112 },
  { ip: "127.0.0.1", port: 8113 },
  { ip: "127.0.0.1", port: 8114 },
  { ip: "127.0.0.1", port: 8115 },
  { ip: "127.0.0.1", port: 8116 },
  { ip: "127.0.0.1", port: 8117 },
];

// Create node groups
const sourceGroup = {};
const destGroup = {};

// Performance tracking
const perfStats = {
  startTime: Date.now(),
  filesProcessed: 0,
  filesFound: 0,
  currentNodeIndex: 0,
  nodesProcessed: 0,
  batchesProcessed: 0,
  errors: 0,
  lastReportTime: Date.now(),
  // Pause configuration
  pauseInterval: 2000,      // Pause after every 2000 files
  pauseDuration: 5000,      // Pause for 5 seconds
  // Metrics timer
  metricsTimer: null,
  // Is the process currently paused
  isPaused: false,
  // Track spawned nodes for teardown
  spawnedNodes: []
};

// Helper function for checking memory usage
function checkMemoryUsage() {
  const memUsage = process.memoryUsage();
  const heapUsedPercent = (memUsage.heapUsed / memUsage.heapTotal) * 100;
  
  if (heapUsedPercent > MAX_MEMORY_USAGE_PERCENT) {
    console.log(`\nHigh memory usage detected (${heapUsedPercent.toFixed(2)}%). Attempting to free memory...`);
    // Force V8 garbage collection if possible
    try {
      if (global.gc) {
        global.gc();
        console.log("Garbage collection completed.\n");
      } else {
        console.log("Suggestion: Run with --expose-gc flag for better memory management.\n");
      }
    } catch (e) {
      console.log("Unable to force garbage collection.\n");
    }
    
    // These operations can help trigger garbage collection
    const forceGC = () => {
      let arr = [];
      for (let i = 0; i < 1000; i++) {
        arr.push(new ArrayBuffer(1024 * 100));
      }
      arr = null;
    };
    
    try {
      forceGC();
    } catch (e) {
      // Ignore errors
    }
  }
}

// Main function
distribution.node.start((server) => {
  console.log(`STARTING DATA REDISTRIBUTION FROM '${SOURCE_GROUP_ID}' TO '${DESTINATION_GROUP_ID}'...`);
  
  // Setup and populate groups with node information
  for (const nodeConfig of inNodes) {
    const sid = id.getSID(nodeConfig);
    const nid = id.getNID(nodeConfig);
    sourceGroup[sid] = nodeConfig;
    console.log(`Adding node ${nid} (${nodeConfig.ip}:${nodeConfig.port}) to source group`);
  }

  for (const nodeConfig of outNodes) {
    const sid = id.getSID(nodeConfig);
    const nid = id.getNID(nodeConfig);
    destGroup[sid] = nodeConfig;
    console.log(`Adding node ${nid} (${nodeConfig.ip}:${nodeConfig.port}) to destination group`);
  }

  // Helper function to spawn a node - returns a proper Promise
  const spawn_node = (node) => {
    // Create a proper node identifier for logging
    const nodeIdentifier = `${node.ip}:${node.port} (${id.getNID(node)})`;
    
    return new Promise((resolve, reject) => {
      console.log(`Attempting to spawn node at ${nodeIdentifier}...`);
      
      distribution.local.status.spawn(node, (e, v) => {
        if (e) {
          console.error(`Failed to spawn node at ${nodeIdentifier}:`, e);
          reject(e);
        } else {
          console.log(`Spawned node at ${nodeIdentifier} successfully`);
          // Track this node for teardown
          perfStats.spawnedNodes.push(node);
          resolve(v);
        }
      });
    });
  };

  // Helper function to stop a node
  const stop_node = (node) => {
    // Create a proper node identifier for logging
    const nodeIdentifier = `${node.ip}:${node.port} (${id.getNID(node)})`;
    
    return new Promise((resolve, reject) => {
      console.log(`Stopping node at ${nodeIdentifier}...`);
      
      distribution.local.comm.send(
        [],
        { service: "status", method: "stop", node: node },
        (e, v) => {
          if (e) {
            console.error(`Error stopping node at ${nodeIdentifier}:`, e);
            reject(e);
          } else {
            console.log(`Node at ${nodeIdentifier} stopped successfully`);
            resolve(v);
          }
        }
      );
    });
  };

  // Group configurations
  const sourceGroupConfig = { gid: SOURCE_GROUP_ID };
  const destGroupConfig = { gid: DESTINATION_GROUP_ID };
  
  // Function to report current metrics
  function reportMetrics() {
    const now = Date.now();
    const elapsedMs = now - perfStats.startTime;
    const elapsedSeconds = elapsedMs / 1000;
    const filesPerSecond = perfStats.filesProcessed / elapsedSeconds;
    
    // Calculate overall progress
    const totalNodesCount = inNodes.length;
    const progressPercentage = ((perfStats.nodesProcessed + (perfStats.currentNodeIndex > 0 ? 1 : 0)) / totalNodesCount) * 100;
    let remainingEstimate = "";
    
    if (perfStats.filesProcessed > 0) {
      const averageFilesPerNode = perfStats.filesFound / Math.max(1, perfStats.nodesProcessed + (perfStats.currentNodeIndex > 0 ? 1 : 0));
      const estimatedTotalFiles = averageFilesPerNode * totalNodesCount;
      const estimatedRemainingFiles = estimatedTotalFiles - perfStats.filesProcessed;
      const estimatedRemainingSeconds = filesPerSecond > 0 ? estimatedRemainingFiles / filesPerSecond : 0;
      
      if (estimatedRemainingSeconds > 3600) {
        remainingEstimate = `~${Math.floor(estimatedRemainingSeconds / 3600)}h ${Math.floor((estimatedRemainingSeconds % 3600) / 60)}m`;
      } else if (estimatedRemainingSeconds > 60) {
        remainingEstimate = `~${Math.floor(estimatedRemainingSeconds / 60)}m ${Math.floor(estimatedRemainingSeconds % 60)}s`;
      } else {
        remainingEstimate = `~${Math.floor(estimatedRemainingSeconds)}s`;
      }
    }
    
    console.log("\n===== METRICS REPORT =====");
    console.log(`Time: ${new Date().toISOString()}`);
    console.log(`Nodes: ${perfStats.nodesProcessed + (perfStats.currentNodeIndex > 0 ? 1 : 0)}/${totalNodesCount} (${progressPercentage.toFixed(2)}%)`);
    console.log(`Files Processed: ${perfStats.filesProcessed}/${perfStats.filesFound} found so far`);
    console.log(`Speed: ${filesPerSecond.toFixed(2)} files/sec`);
    console.log(`Elapsed time: ${(elapsedSeconds / 60).toFixed(2)} minutes`);
    
    if (remainingEstimate) {
      console.log(`Estimated time remaining: ${remainingEstimate}`);
    }
    
    console.log(`Errors: ${perfStats.errors}`);
    
    // Memory usage
    const memUsage = process.memoryUsage();
    console.log(`Memory: ${(memUsage.heapUsed / 1024 / 1024).toFixed(2)} MB used of ${(memUsage.heapTotal / 1024 / 1024).toFixed(2)} MB total`);
    
    console.log("=========================\n");
    
    // Update last report time
    perfStats.lastReportTime = now;
    
    // Check memory usage and trigger GC if needed
    checkMemoryUsage();
  }

  function isEmptyObject(obj) {
    return obj && typeof obj === 'object' && Object.keys(obj).length === 0;
  }

  // Helper function to set up groups
  function setupGroups(callback) {
    console.log("Setting up groups...");
    
    // Set up source group
    distribution.local.groups.put(sourceGroupConfig, sourceGroup, (err, result) => {
      if (err && !isEmptyObject(err)) {
        console.error("Error setting up source group:", err);
        finish();
        return;
      }
      
      console.log(`Source group (${SOURCE_GROUP_ID}) set up successfully.`);
      
      // Set up destination group
      distribution.local.groups.put(destGroupConfig, destGroup, (err, result) => {
        if (err && !isEmptyObject(err)) {
          console.error("Error setting up destination group:", err);
          finish();
          return;
        }
        
        console.log(`Destination group (${DESTINATION_GROUP_ID}) set up successfully.`);
        
        // Set up destination group in service
        distribution[DESTINATION_GROUP_ID].groups.put(destGroupConfig, destGroup, (err, result) => {
          if (err && !isEmptyObject(err)) {
            console.error("Error setting up destination group in service:", err);
            finish();
            return;
          }
          
          console.log(`Destination group (${DESTINATION_GROUP_ID}) set up in service successfully.`);
          callback();
        });
      });
    });
  }


  // Process a single file using the distributed store service
  function processFile(fileInfo, callback) {
    const filePath = path.join(fileInfo.nodePath, fileInfo.fileName);
    
    try {
      // Read the file
      const fileData = fs.readFileSync(filePath, 'utf8');
      
      try {
        // Parse the JSON content
        const parsedJson = JSON.parse(fileData);
        const deserializedData = util.deserialize(parsedJson);
        
        // Use the distributed store service which automatically handles node selection
        distribution[DESTINATION_GROUP_ID].store.put(deserializedData, fileInfo.key, (err, result) => {
          if (err) {
            console.error(`Error storing key ${fileInfo.key} in destination group:`, err);
            perfStats.errors++;
            callback();
            return;
          }
          
          perfStats.filesProcessed++;
          
          // Log only occasionally to avoid flooding console
          if (perfStats.filesProcessed % 500 === 0) {
            console.log(`Processed ${perfStats.filesProcessed} files so far`);
          }
          
          // Check if we need to pause after this file
          if (perfStats.filesProcessed % perfStats.pauseInterval === 0) {
            perfStats.isPaused = true;
            console.log(`\n----- PAUSING FOR ${perfStats.pauseDuration/1000} SECONDS AFTER ${perfStats.filesProcessed} FILES -----`);
            
            // Report metrics during pause
            reportMetrics();
            
            console.log(`----- PAUSE STARTED AT ${new Date().toISOString()} -----\n`);
            
            // Pause for the specified duration
            setTimeout(() => {
              console.log(`\n----- RESUMING AFTER PAUSE AT ${new Date().toISOString()} -----\n`);
              perfStats.isPaused = false;
              callback();
            }, perfStats.pauseDuration);
          } else {
            callback();
          }
        });
      } catch (err) {
        console.error(`Error parsing JSON from file ${filePath}:`, err);
        perfStats.errors++;
        callback();
      }
    } catch (err) {
      console.error(`Error reading file ${filePath}:`, err);
      perfStats.errors++;
      callback();
    }
  }
  
  // Process a batch of files
  function processBatch(batchFiles, callback) {
    let processedCount = 0;
    let currentIndex = 0;
    
    // Process files in chunks to control concurrency
    function processNextChunk() {
      if (currentIndex >= batchFiles.length) {
        callback();
        return;
      }
      
      // Process multiple files concurrently up to CONCURRENT_KEYS
      const concurrentLimit = Math.min(CONCURRENT_KEYS, batchFiles.length - currentIndex);
      let concurrentCompleted = 0;
      
      for (let i = 0; i < concurrentLimit; i++) {
        const fileIndex = currentIndex + i;
        if (fileIndex < batchFiles.length) {
          processFile(batchFiles[fileIndex], () => {
            concurrentCompleted++;
            processedCount++;
            
            // When all concurrent files are processed, move to next chunk
            if (concurrentCompleted === concurrentLimit) {
              currentIndex += concurrentLimit;
              setImmediate(processNextChunk); // Use setImmediate instead of nextTick for better performance
            }
          });
        }
      }
    }
    
    // Start processing the first chunk
    processNextChunk();
  }
  
  // Process files from a node directory in batches
  function processNodeFiles(nodeConfig, callback) {
    const nid = id.getNID(nodeConfig);
    const dirPath = path.join('store', nid, SOURCE_GROUP_ID);
    
    // Check if directory exists
    if (!fs.existsSync(dirPath)) {
      console.log(`Directory does not exist: ${dirPath}`);
      callback();
      return;
    }
    
    try {
      // Get all the files in the directory
      console.log(`Reading directory for node ${nid}...`);
      const allFiles = fs.readdirSync(dirPath)
        .filter(file => file.endsWith('.json'));
      
      const fileCount = allFiles.length;
      console.log(`Processing ${fileCount} files from node ${nid}`);
      
      // Update total file count
      perfStats.filesFound += fileCount;
      
      // Process files in smaller segments to avoid memory issues
      const processSegment = (startIndex, segmentSize) => {
        if (startIndex >= fileCount) {
          // This node is complete
          perfStats.nodesProcessed++;
          callback();
          return;
        }
        
        // Create a segment of files to process
        const endIndex = Math.min(startIndex + segmentSize, fileCount);
        const segmentFiles = allFiles.slice(startIndex, endIndex).map(file => ({
          nodePath: dirPath,
          nodeId: nid,
          fileName: file,
          key: file.replace(/\.json$/, '')
        }));
        
        console.log(`Processing segment ${startIndex}-${endIndex} of ${fileCount} from node ${nid}`);
        
        // Process in batches to avoid memory issues
        let batchIndex = 0;
        const totalBatches = Math.ceil(segmentFiles.length / BATCH_SIZE);
        
        function processNextBatch() {
          if (batchIndex >= totalBatches) {
            // This segment is complete, move to next segment
            // Use nextTick to avoid stack overflow
            process.nextTick(() => processSegment(endIndex, segmentSize));
            return;
          }
          
          const batchStart = batchIndex * BATCH_SIZE;
          const batchEnd = Math.min(batchStart + BATCH_SIZE, segmentFiles.length);
          const batchFiles = segmentFiles.slice(batchStart, batchEnd);
          
          console.log(`Processing batch ${batchIndex + 1}/${totalBatches} from segment ${startIndex}-${endIndex} (${batchFiles.length} files)`);
          
          processBatch(batchFiles, () => {
            batchIndex++;
            perfStats.batchesProcessed++;
            
            // Periodically trigger GC to keep memory usage in check
            checkMemoryUsage();
            
            // Continue to next batch
            process.nextTick(processNextBatch);
          });
        }
        
        // Start processing batches
        processNextBatch();
      };
      
      // Start processing with the first segment (10000 files per segment)
      processSegment(0, 10000);
      
    } catch (err) {
      console.error(`Error reading directory ${dirPath}:`, err);
      perfStats.errors++;
      callback();
    }
  }
  
  // Process all nodes one by one
  function processAllNodes() {
    // Start metrics reporting timer if not already started
    if (!perfStats.metricsTimer) {
      console.log(`Starting metrics reporter (will report every ${METRICS_INTERVAL/1000} seconds)`);
      perfStats.metricsTimer = setInterval(() => {
        // Only report if not paused (to avoid duplicate reports)
        if (!perfStats.isPaused) {
          reportMetrics();
        }
      }, METRICS_INTERVAL);
    }
    
    // Process each node one at a time
    function processNextNode() {
      if (perfStats.currentNodeIndex >= inNodes.length) {
        // All nodes processed
        console.log("All nodes processed. Redistribution complete!");
        displayFinalMetrics();
        finish();
        return;
      }
      
      const nodeConfig = inNodes[perfStats.currentNodeIndex];
      processNodeFiles(nodeConfig, () => {
        perfStats.currentNodeIndex++;
        process.nextTick(processNextNode);
      });
    }
    
    // Start with the first node
    processNextNode();
  }
  
  // Display final metrics at the end of processing
  function displayFinalMetrics() {
    clearInterval(perfStats.metricsTimer);
    
    const elapsedSeconds = (Date.now() - perfStats.startTime) / 1000;
    
    console.log("\n===== FINAL METRICS =====");
    console.log(`Total files processed: ${perfStats.filesProcessed}`);
    console.log(`Total errors: ${perfStats.errors}`);
    console.log(`Total time: ${elapsedSeconds.toFixed(2)} seconds (${(elapsedSeconds / 60).toFixed(2)} minutes)`);
    console.log(`Average speed: ${(perfStats.filesProcessed / elapsedSeconds).toFixed(2)} files/sec`);
    console.log("================================\n");
  }
  
  // Sequential node spawning with proper error handling
  async function spawnAllNodes() {
    console.log("Spawning all nodes...");
    
    // All nodes (combine source and destination)
    const allNodes = [...inNodes, ...outNodes];
    
    try {
      // Spawn nodes sequentially to avoid potential race conditions
      for (const node of allNodes) {
        await spawn_node(node);
      }
      
      console.log(`All ${allNodes.length} nodes spawned successfully!`);
      
      // Now set up the groups
      setupGroups(() => {
        console.log("All groups set up. Starting processing...");
        processAllNodes();
      });
    } catch (error) {
      console.error("Error during node spawning process:", error);
      console.log(`Successfully spawned ${perfStats.spawnedNodes.length} out of ${allNodes.length} nodes before error occurred.`);
      finish();
    }
  }
  
  // Properly stop all spawned nodes
  async function stopAllNodes() {
    console.log(`Stopping all spawned nodes (${perfStats.spawnedNodes.length} nodes)...`);
    
    try {
      // Track successfully stopped nodes
      let stoppedCount = 0;
      let errorCount = 0;
      
      // Stop each node sequentially to avoid overwhelming the system
      for (const node of perfStats.spawnedNodes) {
        try {
          await stop_node(node);
          stoppedCount++;
        } catch (nodeError) {
          errorCount++;
          console.error(`Failed to stop node at ${node.ip}:${node.port}: ${nodeError.message}`);
          // Continue with other nodes even if one fails
        }
      }
      
      console.log(`Node stopping complete: ${stoppedCount} stopped successfully, ${errorCount} failed.`);
    } catch (error) {
      console.error("Error in stopAllNodes function:", error);
    }
  }
  
  // Helper function to clean up
  async function finish() {
    console.log("SHUTTING DOWN...");
    
    // Clear metrics timer if it's still running
    if (perfStats.metricsTimer) {
      clearInterval(perfStats.metricsTimer);
      perfStats.metricsTimer = null;
    }
    
    // Stop all spawned nodes
    await stopAllNodes();
    
    // Close the server
    server.close();
    console.log("Server closed. Script complete.");
    
    // Exit with appropriate code after a brief delay to allow for final log messages
    setTimeout(() => {
      process.exit(perfStats.errors > 0 ? 1 : 0);
    }, 1000);
  }
  
  // Handle unexpected errors and termination
  process.on('uncaughtException', async (error) => {
    console.error('Uncaught exception:', error);
    await finish();
  });
  
  process.on('SIGINT', async () => {
    console.log('Received SIGINT signal. Shutting down gracefully...');
    await finish();
  });
  
  process.on('SIGTERM', async () => {
    console.log('Received SIGTERM signal. Shutting down gracefully...');
    await finish();
  });
  
  // Start the process by spawning all nodes
  spawnAllNodes();
});