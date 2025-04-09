#!/usr/bin/env node
/**
 * TF-IDF Aggregation and Reduction
 * 
 * Memory-efficient script to process batch files produced by mapreduce and
 * store the aggregated results in a distributed key-value store.
 */

const fs = require('fs');
const path = require('path');
const util = require('util');
const readline = require('readline');
const os = require('os');
const { performance } = require('perf_hooks');

// Import the same distribution module used in the original script
const distribution = require("../config.js");
const id = distribution.util.id;

// Configuration
const CONFIG = {
  // Processing config
  BATCH_SIZE: 50,                   // Number of items per batch for storage
  MAX_RETRIES: 3,                   // Maximum retries for failed operations
  RETRY_DELAY: 1000,                // Delay in ms between retries
  PROCESS_CHUNK_SIZE: 500,          // Chunk size for in-memory processing
  
  // Index structure config
  USE_DISTRIBUTED_STORE: true,      // Whether to use the distributed key-value store
  
  // Group configuration
  TERM_GROUP: "index",              // Group for storing terms
  DOC_GROUP: "tfidf",               // Group for storing documents
  
  // Key prefixes
  INDEX_PREFIX: "term:",            // Prefix for index keys in the distributed store
  DOC_PREFIX: "doc:",               // Prefix for document keys in the distributed store
  
  // Directories
  RESULTS_DIR: "./tfidf-results",   // Directory for results
  BATCHES_DIR: "./tfidf-results/batches",
  
  // Index limits
  MAX_DOCS_PER_TERM: 500,           // Maximum documents per term
  MAX_TERMS_PER_DOC: 50,            // Maximum terms per document
  
  // Logging and monitoring
  MEMORY_MONITORING: true,          // Track memory usage
  LOGGING_LEVEL: 'info',            // Logging level: debug, info, warn, error
};

// Performance tracking
const METRICS = {
  startTime: 0,
  endTime: 0,
  phases: {},
  memorySnapshots: [],
  errors: 0,
  throughput: {
    terms: [],
    docs: [],
    batches: []
  }
};

// Logging utility
const LOGGER = {
  debug: (message) => {
    if (CONFIG.LOGGING_LEVEL === 'debug') {
      console.log(`[DEBUG] ${new Date().toISOString()} - ${message}`);
    }
  },
  info: (message) => {
    if (['debug', 'info'].includes(CONFIG.LOGGING_LEVEL)) {
      console.log(`[INFO] ${new Date().toISOString()} - ${message}`);
    }
  },
  warn: (message) => {
    if (['debug', 'info', 'warn'].includes(CONFIG.LOGGING_LEVEL)) {
      console.warn(`[WARN] ${new Date().toISOString()} - ${message}`);
    }
  },
  error: (message) => {
    if (['debug', 'info', 'warn', 'error'].includes(CONFIG.LOGGING_LEVEL)) {
      console.error(`[ERROR] ${new Date().toISOString()} - ${message}`);
    }
  }
};

// Helper function to format time
function formatTime(ms) {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`;
  if (ms < 3600000) return `${(ms / 60000).toFixed(2)}m`;
  return `${(ms / 3600000).toFixed(2)}h`;
}

// Helper function to format bytes
function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1048576) return `${(bytes / 1024).toFixed(2)}KB`;
  if (bytes < 1073741824) return `${(bytes / 1048576).toFixed(2)}MB`;
  return `${(bytes / 1073741824).toFixed(2)}GB`;
}

// Take a memory snapshot
function takeMemorySnapshot(label) {
  if (!CONFIG.MEMORY_MONITORING) return;
  
  const memoryUsage = process.memoryUsage();
  METRICS.memorySnapshots.push({
    timestamp: Date.now(),
    label,
    rss: memoryUsage.rss,
    heapTotal: memoryUsage.heapTotal,
    heapUsed: memoryUsage.heapUsed,
    external: memoryUsage.external
  });
  
  LOGGER.debug(`Memory [${label}]: RSS: ${formatBytes(memoryUsage.rss)}, Heap: ${formatBytes(memoryUsage.heapUsed)}/${formatBytes(memoryUsage.heapTotal)}`);
}

// Phase timing functions
function startPhase(phaseName) {
  METRICS.phases[phaseName] = {
    startTime: performance.now(),
    endTime: 0,
    duration: 0,
    subTasks: {}
  };
  
  LOGGER.info(`===== STARTING PHASE: ${phaseName} =====`);
  takeMemorySnapshot(`phase_${phaseName}_start`);
}

function endPhase(phaseName) {
  if (!METRICS.phases[phaseName]) {
    LOGGER.warn(`Cannot end phase ${phaseName} - not started`);
    return;
  }
  
  METRICS.phases[phaseName].endTime = performance.now();
  METRICS.phases[phaseName].duration = METRICS.phases[phaseName].endTime - METRICS.phases[phaseName].startTime;
  
  LOGGER.info(`===== COMPLETED PHASE: ${phaseName} in ${formatTime(METRICS.phases[phaseName].duration)} =====`);
  takeMemorySnapshot(`phase_${phaseName}_end`);
}

// Subtask timing functions
function startSubTask(phaseName, taskName) {
  if (!METRICS.phases[phaseName]) {
    LOGGER.warn(`Cannot start subtask ${taskName} - phase ${phaseName} not started`);
    return;
  }
  
  METRICS.phases[phaseName].subTasks[taskName] = {
    startTime: performance.now(),
    endTime: 0,
    duration: 0
  };
  
  LOGGER.debug(`Starting subtask [${phaseName}:${taskName}]`);
}

function endSubTask(phaseName, taskName) {
  if (!METRICS.phases[phaseName] || !METRICS.phases[phaseName].subTasks[taskName]) {
    LOGGER.warn(`Cannot end subtask ${taskName} - not started`);
    return;
  }
  
  METRICS.phases[phaseName].subTasks[taskName].endTime = performance.now();
  METRICS.phases[phaseName].subTasks[taskName].duration = 
    METRICS.phases[phaseName].subTasks[taskName].endTime - 
    METRICS.phases[phaseName].subTasks[taskName].startTime;
  
  LOGGER.debug(`Completed subtask [${phaseName}:${taskName}] in ${formatTime(METRICS.phases[phaseName].subTasks[taskName].duration)}`);
}

// Save metrics to file
function saveMetrics(outputDir) {
  const metricsDir = path.join(outputDir, 'metrics');
  if (!fs.existsSync(metricsDir)) {
    fs.mkdirSync(metricsDir, { recursive: true });
  }
  
  const metricsFilePath = path.join(metricsDir, `aggregation_metrics_${new Date().toISOString().replace(/:/g, '-')}.json`);
  
  METRICS.endTime = performance.now();
  METRICS.totalDuration = METRICS.endTime - METRICS.startTime;
  
  fs.writeFileSync(
    metricsFilePath,
    JSON.stringify(METRICS, null, 2)
  );
  
  LOGGER.info(`Metrics saved to ${metricsFilePath}`);
}

// Check if a value is an empty object
function isEmptyObject(obj) {
  return obj && typeof obj === "object" && Object.keys(obj).length === 0;
}

// Function to put a value in the distributed store
function putInDistributedStore(gid, key, value) {
  return new Promise((resolve, reject) => {
    if (typeof key !== 'string') {
      key = String(key);
    }
    
    let retries = 0;
    const maxRetries = CONFIG.MAX_RETRIES;
    
    const storeWithRetry = () => {
      distribution[gid].store.put(value, key, (err, result) => {
        if (err && !isEmptyObject(err)) {
          if (retries < maxRetries) {
            retries++;
            LOGGER.warn(`Retrying store operation for ${key} (${retries}/${maxRetries})...`);
            setTimeout(storeWithRetry, CONFIG.RETRY_DELAY);
            return;
          }
          LOGGER.error(`Error storing ${key} after ${maxRetries} retries: ${JSON.stringify(err)}`);
          reject(err);
        } else {
          resolve(result);
        }
      });
    };
    
    storeWithRetry();
  });
}

// Run garbage collection
function runGarbageCollection(label) {
  if (global.gc) {
    LOGGER.debug(`Running garbage collection: ${label || ''}`);
    global.gc();
  } else {
    LOGGER.debug(`Explicit garbage collection not available`);
    // Create memory pressure to encourage garbage collection
    const pressure = [];
    pressure.length = 1000000;
    pressure.length = 0;
  }
}

// Create alphabet-based shards for distributed workload
function createShards() {
  const shards = {};
  // Use a combination of letters for better distribution
  const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
  for (let i = 0; i < alphabet.length; i++) {
    for (let j = 0; j < alphabet.length; j++) {
      const prefix = alphabet[i] + alphabet[j];
      shards[prefix] = { termCount: 0, docIds: new Set() };
    }
  }
  
  // Add a catch-all "other" shard
  shards["other"] = { termCount: 0, docIds: new Set() };
  return shards;
}

// Function to determine which shard a term belongs to
function getShardKey(term, shards) {
  if (!term || term.length < 2) return "other";
  const prefix = term.substring(0, 2).toLowerCase();
  if (shards[prefix]) return prefix;
  return "other";
}

// Estimate time to complete
function estimateTimeToComplete(phaseStartTime, itemsProcessed, totalItems) {
  if (itemsProcessed === 0) return "Calculating...";
  
  const elapsedMs = performance.now() - phaseStartTime;
  const msPerItem = elapsedMs / itemsProcessed;
  const remainingItems = totalItems - itemsProcessed;
  const estimatedRemainingMs = msPerItem * remainingItems;
  
  return formatTime(estimatedRemainingMs);
}

// Read a batch file using streaming
async function readBatchFile(filePath) {
  LOGGER.debug(`Reading batch file: ${filePath}`);
  
  try {
    // Use streaming to avoid loading entire file at once
    const fileStream = fs.createReadStream(filePath, { encoding: 'utf8' });
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity
    });
    
    let jsonContent = '';
    
    // Read the file line by line to build the JSON content
    for await (const line of rl) {
      jsonContent += line;
    }
    
    // Parse JSON content
    const batchData = JSON.parse(jsonContent);
    LOGGER.debug(`Successfully read ${batchData.length} items from ${filePath}`);
    
    return batchData;
  } catch (err) {
    LOGGER.error(`Error reading batch file ${filePath}: ${err.message}`);
    throw err;
  }
}

// Get all available batch files
async function getBatchFiles(batchesDir, resultsDir) {
  LOGGER.info(`Looking for batch files...`);
  
  try {
    let batchFiles = [];
    
    // First try to get batch files from the successful-batches.json metadata
    if (fs.existsSync(`${resultsDir}/successful-batches.json`)) {
      const successfulBatches = JSON.parse(
        fs.readFileSync(`${resultsDir}/successful-batches.json`, 'utf8')
      );
      LOGGER.info(`Found ${successfulBatches.length} successful batches from metadata`);
      batchFiles = successfulBatches.map(batch => batch.filePath).filter(path => fs.existsSync(path));
    } 
    
    // If no batch files found from metadata, scan the directory
    if (batchFiles.length === 0) {
      batchFiles = fs.readdirSync(batchesDir)
        .filter(file => file.startsWith('batch-') && file.endsWith('.json'))
        .map(file => `${batchesDir}/${file}`);
      LOGGER.info(`Found ${batchFiles.length} batch files in directory`);
    }
    
    return batchFiles;
  } catch (err) {
    LOGGER.error(`Error getting batch files: ${err.message}`);
    throw err;
  }
}

// Process terms in a shard
async function processShardTerms(shardKey, shardDir, batchFiles, actualTotalDocuments, globalShards) {
  const termStats = new Map();
  
  LOGGER.info(`Processing terms for shard ${shardKey}`);
  startSubTask('phase2', `shard_${shardKey}`);
  
  // Process each batch file to collect term statistics for this shard
  for (let fileIndex = 0; fileIndex < batchFiles.length; fileIndex++) {
    const batchFile = batchFiles[fileIndex];
    
    // Provide occasional progress updates
    if ((fileIndex + 1) % 50 === 0 || fileIndex === 0 || fileIndex === batchFiles.length - 1) {
      LOGGER.info(`[Shard ${shardKey}] Processed ${fileIndex+1}/${batchFiles.length} batch files`);
      LOGGER.info(`[Shard ${shardKey}] Estimated time to complete: ${
        estimateTimeToComplete(
          METRICS.phases.phase2.subTasks[`shard_${shardKey}`].startTime,
          fileIndex + 1,
          batchFiles.length
        )
      }`);
    }
    
    try {
      const batchData = await readBatchFile(batchFile);
      
      // Process only the relevant terms for this shard
      for (const result of batchData) {
        if (!result || !result.word) continue;
        
        const word = result.word;
        if (getShardKey(word, globalShards) !== shardKey) continue;
        
        // Initialize term entry if not exists
        if (!termStats.has(word)) {
          termStats.set(word, {
            word,
            documentFrequency: 0,
            documents: new Map(),
            uniqueDocIds: new Set()
          });
        }
        
        const entry = termStats.get(word);
        
        // Process document scores
        if (Array.isArray(result.scores)) {
          result.scores.forEach(score => {
            if (!score || !score.docId) return;
            
            const docId = score.docId;
            entry.uniqueDocIds.add(docId);
            
            // Store document score information
            entry.documents.set(docId, {
              tf: score.tf || 0,
              count: score.count || 1,
              totalWords: score.totalWords || 0
            });
          });
        }
      }
      
      // Force garbage collection after processing each batch
      if ((fileIndex + 1) % 25 === 0) {
        runGarbageCollection(`shard_${shardKey}_batch_${fileIndex}`);
      }
      
    } catch (err) {
      LOGGER.error(`Error processing batch file ${batchFile} for shard ${shardKey}: ${err.message}`);
      METRICS.errors++;
    }
  }
  
  // Calculate document frequency and store results
  const termsFile = path.join(shardDir, 'terms.json');
  const termData = {};
  
  // Convert term Map to a plain object and calculate document frequency
  for (const [word, entry] of termStats.entries()) {
    entry.documentFrequency = entry.uniqueDocIds.size;
    
    // Calculate IDF
    const idf = Math.log(actualTotalDocuments / Math.max(entry.uniqueDocIds.size, 1));
    
    // Create document scores array
    const docScores = [];
    for (const [docId, scoreData] of entry.documents.entries()) {
      const tf = scoreData.tf;
      const tfidf = tf * idf;
      
      docScores.push({
        docId,
        score: tfidf,
        count: scoreData.count || 1
      });
    }
    
    // Sort by score (descending) and limit to max docs per term
    docScores.sort((a, b) => b.score - a.score);
    const limitedDocScores = docScores.slice(0, CONFIG.MAX_DOCS_PER_TERM);
    
    // Store term data in compact format
    termData[word] = {
      df: entry.uniqueDocIds.size,
      idf,
      // Use compact posting list format [docId, score, count]
      postings: limitedDocScores.map(p => [
        p.docId, 
        parseFloat(p.score.toFixed(6)), 
        p.count
      ])
    };
    
    // Calculate term importance
    termData[word].importance = limitedDocScores.reduce((sum, doc) => sum + doc.score, 0);
  }
  
  // Save term data to disk
  fs.writeFileSync(termsFile, JSON.stringify(termData, null, 0));
  
  endSubTask('phase2', `shard_${shardKey}`);
  LOGGER.info(`Completed processing shard ${shardKey}: saved ${Object.keys(termData).length} terms to ${termsFile}`);
  
  return {
    termCount: Object.keys(termData).length,
    filePath: termsFile
  };
}

// Store documents in batches
async function storeDocumentsInBatches(documentMap, docGid, resultsDir) {
  LOGGER.info(`Storing ${documentMap.size} documents in distributed store...`);
  startSubTask('phase3', 'store_documents');
  
  // Configuration
  const BATCH_SIZE = CONFIG.BATCH_SIZE;
  const DOC_PREFIX = CONFIG.DOC_PREFIX;
  
  // Create document output directory if it doesn't exist
  const docsDir = path.join(resultsDir, "docs");
  if (!fs.existsSync(docsDir)) {
    fs.mkdirSync(docsDir, { recursive: true });
  }
  
  // Get all document IDs
  const docIds = Array.from(documentMap.keys());
  
  // Process documents in batches
  let processedDocs = 0;
  let errorCount = 0;
  
  const batchStartTime = performance.now();
  
  for (let i = 0; i < docIds.length; i += BATCH_SIZE) {
    const batchDocIds = docIds.slice(i, i + BATCH_SIZE);
    
    // Log progress with time estimate
    if ((i + BATCH_SIZE) % (BATCH_SIZE * 5) === 0 || i === 0 || i + BATCH_SIZE >= docIds.length) {
      const percentComplete = ((i + BATCH_SIZE) / docIds.length * 100).toFixed(2);
      LOGGER.info(`Processing documents ${i+1} to ${Math.min(i + BATCH_SIZE, docIds.length)} of ${docIds.length} (${percentComplete}%)...`);
      
      // Estimate time remaining
      if (i > 0) {
        LOGGER.info(`Estimated time to complete: ${
          estimateTimeToComplete(batchStartTime, i, docIds.length)
        }`);
      }
    }
    
    const batchPromises = [];
    
    for (const docId of batchDocIds) {
      const docData = documentMap.get(docId);
      
      if (!docData || !docData.terms || !docData.terms.length) {
        continue;
      }
      
      try {
        // Sort terms by TF-IDF score (descending)
        docData.terms.sort((a, b) => b.tfidf - a.tfidf);
        
        // Limit to top N terms per document
        const topTermsCount = Math.min(docData.terms.length, CONFIG.MAX_TERMS_PER_DOC);
        const topTerms = docData.terms.slice(0, topTermsCount);
        
        // Calculate document vector norm for similarity calculations
        let normSum = 0;
        for (let i = 0; i < topTerms.length; i++) {
          normSum += Math.pow(topTerms[i].tfidf, 2);
        }
        const docNorm = Math.sqrt(normSum);
        
        // Create optimized document object
        const docObject = {
          id: docId,
          totalWords: docData.totalWords,
          docNorm: parseFloat(docNorm.toFixed(6)),
          // Store terms in compact format [term, tfidf, count]
          terms: topTerms.map(t => [
            t.term, 
            parseFloat(t.tfidf.toFixed(6)), 
            t.count
          ])
        };
        
        // Store the document object
        if (CONFIG.USE_DISTRIBUTED_STORE) {
          // Store in distributed key-value store
          const storeKey = `${DOC_PREFIX}${docId}`;
          batchPromises.push(
            putInDistributedStore(docGid, storeKey, docObject)
              .then(() => {
                processedDocs++;
                return null;
              })
              .catch(err => {
                LOGGER.error(`Failed to store document ${docId}: ${err}`);
                errorCount++;
                return null;
              })
          );
        } else {
          // Store to local file system
          const docPath = path.join(docsDir, `${encodeURIComponent(docId)}.json`);
          fs.writeFileSync(docPath, JSON.stringify(docObject));
          processedDocs++;
        }
        
      } catch (err) {
        LOGGER.error(`Error processing document ${docId}: ${err.message}`);
        errorCount++;
      }
    }
    
    // Wait for all store operations to complete for this batch
    if (batchPromises.length > 0) {
      await Promise.allSettled(batchPromises);
    }
    
    // Force garbage collection
    if ((i + BATCH_SIZE) % (BATCH_SIZE * 5) === 0) {
      runGarbageCollection(`docs_batch_${i}`);
    }
    
    // Record throughput
    const batchTime = performance.now() - batchStartTime;
    const docsPerSecond = processedDocs / (batchTime / 1000);
    METRICS.throughput.docs.push(docsPerSecond);
    
    // Log progress
    if ((i + BATCH_SIZE) % (BATCH_SIZE * 10) === 0 || i + BATCH_SIZE >= docIds.length) {
      LOGGER.info(`Processed ${processedDocs} documents with ${errorCount} errors (${docsPerSecond.toFixed(2)} docs/sec)...`);
    }
  }
  
  endSubTask('phase3', 'store_documents');
  LOGGER.info(`Document storage complete. Processed ${processedDocs} documents with ${errorCount} errors.`);
  
  return { processedDocs, errorCount };
}

// Store terms in batches
async function storeTermsInBatches(termsData, termGid, resultsDir) {
  const termCount = Object.keys(termsData).length;
  LOGGER.info(`Storing ${termCount} terms in distributed store...`);
  
  // Configuration
  const BATCH_SIZE = CONFIG.BATCH_SIZE;
  const INDEX_PREFIX = CONFIG.INDEX_PREFIX;
  
  // Create terms output directory for backup
  const termsDir = path.join(resultsDir, "terms");
  if (!fs.existsSync(termsDir)) {
    fs.mkdirSync(termsDir, { recursive: true });
  }
  
  // Get all terms
  const allTerms = Object.keys(termsData);
  
  // Track progress
  let completedTerms = 0;
  let errors = 0;
  
  const batchStartTime = performance.now();
  
  // Process terms in batches
  for (let i = 0; i < allTerms.length; i += BATCH_SIZE) {
    const termBatch = allTerms.slice(i, i + BATCH_SIZE);
    
    // Log progress with time estimate
    if ((i + BATCH_SIZE) % (BATCH_SIZE * 5) === 0 || i === 0 || i + BATCH_SIZE >= allTerms.length) {
      const percentComplete = ((i + BATCH_SIZE) / allTerms.length * 100).toFixed(2);
      LOGGER.info(`Processing terms ${i+1} to ${Math.min(i + BATCH_SIZE, allTerms.length)} of ${allTerms.length} (${percentComplete}%)...`);
      
      // Estimate time remaining
      if (i > 0) {
        LOGGER.info(`Estimated time to complete: ${
          estimateTimeToComplete(batchStartTime, i, allTerms.length)
        }`);
      }
    }
    
    // Use Promise.allSettled to continue even if some promises reject
    const promises = termBatch.map(term => {
      const storeKey = `${INDEX_PREFIX}${term}`;
      const termObject = termsData[term];
      
      // Also save a backup to file system
      try {
        // Use the first two characters as a directory prefix
        const prefix = term.substring(0, 2).toLowerCase();
        const prefixDir = path.join(termsDir, prefix);
        
        if (!fs.existsSync(prefixDir)) {
          fs.mkdirSync(prefixDir, { recursive: true });
        }
        
        const termPath = path.join(prefixDir, `${encodeURIComponent(term)}.json`);
        fs.writeFileSync(termPath, JSON.stringify(termObject));
      } catch (fileErr) {
        LOGGER.error(`Error saving backup of term ${term}: ${fileErr}`);
      }
      
      return putInDistributedStore(termGid, storeKey, termObject)
        .then(() => true)
        .catch(err => {
          errors++;
          return false;
        });
    });
    
    // Wait for all promises to settle
    const results = await Promise.allSettled(promises);
    const successCount = results.filter(r => r.status === 'fulfilled' && r.value === true).length;
    
    // Update progress
    completedTerms += successCount;
    
    // Record throughput
    const batchTime = performance.now() - batchStartTime;
    const termsPerSecond = completedTerms / (batchTime / 1000);
    METRICS.throughput.terms.push(termsPerSecond);
    
    // Log progress
    if ((i + BATCH_SIZE) % (BATCH_SIZE * 20) === 0 || i + BATCH_SIZE >= allTerms.length) {
      LOGGER.info(`Stored ${completedTerms} of ${allTerms.length} terms with ${errors} errors (${termsPerSecond.toFixed(2)} terms/sec)...`);
    }
    
    // Force garbage collection
    if ((i + BATCH_SIZE) % (BATCH_SIZE * 10) === 0) {
      runGarbageCollection(`terms_batch_${i}`);
    }
  }
  
  LOGGER.info(`Completed storing all terms in distributed store.`);
  return { completedTerms, errors };
}

/**
 * Main function to run the TF-IDF aggregation and reduction process
 */
async function runAggregation() {
  METRICS.startTime = performance.now();
  
  LOGGER.info("===== STARTING TF-IDF AGGREGATION AND REDUCTION =====");
  LOGGER.info(`Node.js Version: ${process.version}`);
  LOGGER.info(`Platform: ${os.platform()} ${os.arch()}`);
  LOGGER.info(`CPUs: ${os.cpus().length}`);
  LOGGER.info(`Total Memory: ${formatBytes(os.totalmem())}`);
  LOGGER.info(`Free Memory: ${formatBytes(os.freemem())}`);
  
  takeMemorySnapshot('initial');
  
  // Set up required directories
  const resultsDir = CONFIG.RESULTS_DIR;
  const batchesDir = CONFIG.BATCHES_DIR;
  const tmpDir = path.join(resultsDir, "tmp");
  const statsDir = path.join(resultsDir, "stats");
  const shardsDir = path.join(tmpDir, "shards");
  
  [resultsDir, tmpDir, statsDir, shardsDir].forEach(dir => {
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
  });
  
  // Get list of batch files
  const batchFiles = await getBatchFiles(batchesDir, resultsDir);
  
  if (batchFiles.length === 0) {
    LOGGER.error("No batch files found for aggregation");
    return;
  }
  
  // ===== PHASE 1: Term Discovery and Sharding =====
  startPhase('phase1');
  LOGGER.info("PHASE 1: Term Discovery and Sharding");
  
  // Create shards for distributing the workload
  const shards = createShards();
  
  // Track unique documents and total term count
  const allDocuments = new Set();
  let totalTermsProcessed = 0;
  
  // Get size of first batch file for estimation
  const firstBatchSize = fs.statSync(batchFiles[0]).size;
  const totalBatchSize = batchFiles.reduce((total, file) => total + fs.statSync(file).size, 0);
  const estimatedProcessingTimePhase1 = (totalBatchSize / firstBatchSize) * 2000; // 2 seconds per batch estimated
  
  LOGGER.info(`Estimated processing time for Phase 1: ${formatTime(estimatedProcessingTimePhase1)}`);
  
  // Process each batch file to identify and distribute terms
  for (let fileIndex = 0; fileIndex < batchFiles.length; fileIndex++) {
    const batchFile = batchFiles[fileIndex];
    
    // Log progress with estimate
    if ((fileIndex + 1) % 10 === 0 || fileIndex === 0 || fileIndex === batchFiles.length - 1) {
      const percentComplete = ((fileIndex + 1) / batchFiles.length * 100).toFixed(2);
      LOGGER.info(`[Phase 1] Processing batch file ${fileIndex+1}/${batchFiles.length} (${percentComplete}%): ${path.basename(batchFile)}`);
      
      // Estimate time remaining
      if (fileIndex > 0) {
        LOGGER.info(`Estimated time to complete Phase 1: ${
          estimateTimeToComplete(METRICS.phases.phase1.startTime, fileIndex + 1, batchFiles.length)
        }`);
      }
    }
    
    try {
      // Use streaming JSON parsing to avoid loading entire file at once
      const batchData = await readBatchFile(batchFile);
      
      // Process in small chunks to avoid memory issues
      const CHUNK_SIZE = CONFIG.PROCESS_CHUNK_SIZE;
      for (let i = 0; i < batchData.length; i += CHUNK_SIZE) {
        const chunk = batchData.slice(i, i + CHUNK_SIZE);
        
        // Process each result in the chunk
        for (const result of chunk) {
          if (!result || !result.word) continue;
          
          const word = result.word;
          const shardKey = getShardKey(word, shards);
          
          // Increment term count for this shard
          shards[shardKey].termCount++;
          totalTermsProcessed++;
          
          // Add document IDs to tracking
          if (Array.isArray(result.scores)) {
            result.scores.forEach(score => {
              if (score && score.docId) {
                allDocuments.add(score.docId);
                shards[shardKey].docIds.add(score.docId);
              }
            });
          }
        }
      }
      
      // Record batch throughput
      const batchTime = performance.now() - METRICS.phases.phase1.startTime;
      const batchesPerSecond = (fileIndex + 1) / (batchTime / 1000);
      METRICS.throughput.batches.push(batchesPerSecond);
      
      // Provide progress update
      if ((fileIndex + 1) % 50 === 0) {
        LOGGER.info(`[Phase 1] Processed ${fileIndex+1}/${batchFiles.length} batch files, identified ${totalTermsProcessed} term occurrences and ${allDocuments.size} unique documents`);
        LOGGER.info(`[Phase 1] Current throughput: ${batchesPerSecond.toFixed(2)} batches/sec`);
        
        // Force garbage collection
        runGarbageCollection(`phase1_batch_${fileIndex}`);
        takeMemorySnapshot(`phase1_batch_${fileIndex}`);
      }
      
    } catch (err) {
      LOGGER.error(`Error processing batch file ${batchFile}: ${err.message}`);
      METRICS.errors++;
    }
  }
  
  // Save progress information from Phase 1
  const actualTotalDocuments = allDocuments.size;
  LOGGER.info(`[Phase 1] Completed. Found ${actualTotalDocuments} unique documents and ${totalTermsProcessed} term occurrences distributed across ${Object.keys(shards).length} shards`);
  
  // Save metadata for future reference
  const indexMetadata = {
    totalTerms: totalTermsProcessed,
    totalDocuments: actualTotalDocuments,
    indexCreationDate: new Date().toISOString(),
    shards: Object.keys(shards).length,
    maxDocsPerTerm: CONFIG.MAX_DOCS_PER_TERM,
    maxTermsPerDoc: CONFIG.MAX_TERMS_PER_DOC
  };
  
  fs.writeFileSync(
    `${resultsDir}/index-metadata.json`,
    JSON.stringify(indexMetadata, null, 2)
  );
  
  // Save shard information
  const shardMetadata = {};
  for (const shardKey in shards) {
    shardMetadata[shardKey] = {
      termCount: shards[shardKey].termCount,
      docCount: shards[shardKey].docIds.size
    };
    
    // Convert docIds Set to array and save to disk for later phases
    if (shards[shardKey].termCount > 0) {
      const shardDir = path.join(shardsDir, shardKey);
      if (!fs.existsSync(shardDir)) {
        fs.mkdirSync(shardDir, { recursive: true });
      }
      
      fs.writeFileSync(
        path.join(shardDir, 'metadata.json'),
        JSON.stringify({
          termCount: shards[shardKey].termCount,
          docCount: shards[shardKey].docIds.size,
          docIds: Array.from(shards[shardKey].docIds)
        })
      );
    }
  }
  
  fs.writeFileSync(
    `${resultsDir}/shard-metadata.json`,
    JSON.stringify(shardMetadata, null, 2)
  );
  
  // Clean up memory after phase 1
  for (const key in shards) {
    shards[key].docIds.clear();
    shards[key].docIds = null;
  }
  allDocuments.clear();
  
  // Force garbage collection
  runGarbageCollection('phase1_complete');
  endPhase('phase1');
  
  // ===== PHASE 2: Shard-by-Shard Term Statistics Generation =====
  startPhase('phase2');
  LOGGER.info("\nPHASE 2: Shard-by-Shard Term Statistics Generation");
  
  // Get all shard keys that have terms
  const activeShardKeys = Object.keys(shards).filter(key => shards[key].termCount > 0);
  LOGGER.info(`Processing ${activeShardKeys.length} active shards`);
  
  // Estimate processing time for Phase 2
  const estimatedTimePerShard = 30000; // 30 seconds per shard estimated
  const estimatedProcessingTimePhase2 = activeShardKeys.length * estimatedTimePerShard;
  LOGGER.info(`Estimated processing time for Phase 2: ${formatTime(estimatedProcessingTimePhase2)}`);
  
  // Process each shard sequentially to avoid memory issues
  const shardResults = [];
  
  for (let shardIndex = 0; shardIndex < activeShardKeys.length; shardIndex++) {
    const shardKey = activeShardKeys[shardIndex];
    const shardTermCount = shards[shardKey].termCount;
    
    const percentComplete = ((shardIndex + 1) / activeShardKeys.length * 100).toFixed(2);
    LOGGER.info(`[Phase 2] Processing shard ${shardIndex+1}/${activeShardKeys.length} (${percentComplete}%): ${shardKey} (${shardTermCount} terms)`);
    
    // Skip empty shards
    if (shardTermCount === 0) continue;
    
    const shardDir = path.join(shardsDir, shardKey);
    if (!fs.existsSync(shardDir)) {
      fs.mkdirSync(shardDir, { recursive: true });
    }
    
    try {
      const result = await processShardTerms(shardKey, shardDir, batchFiles, actualTotalDocuments, shards);
      shardResults.push(result);
      
      // Estimate time remaining
      if (shardIndex > 0) {
        LOGGER.info(`Estimated time to complete Phase 2: ${
          estimateTimeToComplete(METRICS.phases.phase2.startTime, shardIndex + 1, activeShardKeys.length)
        }`);
      }
      
      // Force garbage collection after each shard
      runGarbageCollection(`phase2_shard_${shardKey}`);
      takeMemorySnapshot(`phase2_shard_${shardKey}`);
      
    } catch (err) {
      LOGGER.error(`Error processing shard ${shardKey}: ${err.message}`);
      METRICS.errors++;
    }
  }
  
  LOGGER.info(`[Phase 2] Completed processing ${shardResults.length} shards with ${METRICS.errors} errors`);
  
  // Save shard processing results
  fs.writeFileSync(
    path.join(statsDir, 'shard_processing_results.json'),
    JSON.stringify(shardResults, null, 2)
  );
  
  endPhase('phase2');
  
  // ===== PHASE 3: Document Index Creation =====
  startPhase('phase3');
  LOGGER.info("\nPHASE 3: Document Index Creation");
  
  // Estimate processing time for Phase 3
  const estimatedDocsPerSecond = 5; // Estimated docs processed per second
  const estimatedProcessingTimePhase3 = actualTotalDocuments / estimatedDocsPerSecond * 1000;
  LOGGER.info(`Estimated processing time for Phase 3: ${formatTime(estimatedProcessingTimePhase3)}`);
  
  // Initialize document map to track terms per document
  const documentMap = new Map();
  
  // Process each shard to collect document-term information
  for (let shardIndex = 0; shardIndex < activeShardKeys.length; shardIndex++) {
    const shardKey = activeShardKeys[shardIndex];
    const shardDir = path.join(shardsDir, shardKey);
    const termsFile = path.join(shardDir, 'terms.json');
    
    if (!fs.existsSync(termsFile)) {
      LOGGER.info(`[Phase 3] Skipping shard ${shardKey}: no terms file found`);
      continue;
    }
    
    const percentComplete = ((shardIndex + 1) / activeShardKeys.length * 100).toFixed(2);
    LOGGER.info(`[Phase 3] Processing shard ${shardIndex+1}/${activeShardKeys.length} (${percentComplete}%): ${shardKey}`);
    
    try {
      // Read terms data for this shard
      const termsData = JSON.parse(fs.readFileSync(termsFile, 'utf8'));
      
      // Process terms in chunks to avoid memory issues
      const termKeys = Object.keys(termsData);
      const CHUNK_SIZE = CONFIG.PROCESS_CHUNK_SIZE;
      
      for (let i = 0; i < termKeys.length; i += CHUNK_SIZE) {
        const termChunk = termKeys.slice(i, i + CHUNK_SIZE);
        
        for (const term of termChunk) {
          const termObj = termsData[term];
          
          // Add this term's data to each document it appears in
          if (Array.isArray(termObj.postings)) {
            for (const posting of termObj.postings) {
              const [docId, tfidf, count] = posting;
              
              // Initialize document entry if not exists
              if (!documentMap.has(docId)) {
                documentMap.set(docId, {
                  terms: [],
                  totalWords: 0 // Will be populated later
                });
              }
              
              // Add term to document
              documentMap.get(docId).terms.push({
                term,
                tfidf,
                count
              });
            }
          }
        }
        
        // Force garbage collection after each chunk
        if ((i + CHUNK_SIZE) % (CHUNK_SIZE * 10) === 0) {
          runGarbageCollection(`phase3_terms_chunk_${shardKey}_${i}`);
        }
      }
      
      // Log progress
      LOGGER.info(`[Phase 3] Processed ${termKeys.length} terms from shard ${shardKey}`);
      
      // Force garbage collection after each shard
      runGarbageCollection(`phase3_shard_${shardKey}`);
      takeMemorySnapshot(`phase3_shard_${shardKey}`);
      
    } catch (err) {
      LOGGER.error(`Error processing terms from shard ${shardKey}: ${err.message}`);
      METRICS.errors++;
    }
  }
  
  LOGGER.info(`[Phase 3] Collected term information for ${documentMap.size} documents`);
  
  // Function to store documents in batches
  const docStoreResult = await storeDocumentsInBatches(documentMap, CONFIG.DOC_GROUP, resultsDir);
  
  // Clear document map to free memory
  documentMap.clear();
  
  // Force garbage collection
  runGarbageCollection('phase3_complete');
  endPhase('phase3');
  
  // ===== PHASE 4: Term Storage =====
  startPhase('phase4');
  LOGGER.info("\nPHASE 4: Term Storage");
  
  // Estimate processing time for Phase 4
  const estimatedTermsPerSecond = 10; // Estimated terms processed per second
  const estimatedProcessingTimePhase4 = totalTermsProcessed / estimatedTermsPerSecond * 1000;
  LOGGER.info(`Estimated processing time for Phase 4: ${formatTime(estimatedProcessingTimePhase4)}`);
  
  // Store terms from each shard to distributed store
  let totalTermsStored = 0;
  let totalTermErrors = 0;
  
  for (let shardIndex = 0; shardIndex < activeShardKeys.length; shardIndex++) {
    const shardKey = activeShardKeys[shardIndex];
    const shardDir = path.join(shardsDir, shardKey);
    const termsFile = path.join(shardDir, 'terms.json');
    
    if (!fs.existsSync(termsFile)) {
      LOGGER.info(`[Phase 4] Skipping shard ${shardKey}: no terms file found`);
      continue;
    }
    
    const percentComplete = ((shardIndex + 1) / activeShardKeys.length * 100).toFixed(2);
    LOGGER.info(`[Phase 4] Storing terms from shard ${shardIndex+1}/${activeShardKeys.length} (${percentComplete}%): ${shardKey}`);
    
    try {
      // Read terms data for this shard
      const termsData = JSON.parse(fs.readFileSync(termsFile, 'utf8'));
      const termCount = Object.keys(termsData).length;
      
      // Store terms in batches
      const storeResult = await storeTermsInBatches(termsData, CONFIG.TERM_GROUP, resultsDir);
      
      totalTermsStored += storeResult.completedTerms;
      totalTermErrors += storeResult.errors;
      
      // Estimate time remaining
      if (shardIndex > 0) {
        LOGGER.info(`Estimated time to complete Phase 4: ${
          estimateTimeToComplete(METRICS.phases.phase4.startTime, shardIndex + 1, activeShardKeys.length)
        }`);
      }
      
      // Force garbage collection after each shard
      runGarbageCollection(`phase4_shard_${shardKey}`);
      takeMemorySnapshot(`phase4_shard_${shardKey}`);
      
    } catch (err) {
      LOGGER.error(`Error storing terms from shard ${shardKey}: ${err.message}`);
      METRICS.errors++;
    }
  }
  
  LOGGER.info(`[Phase 4] Completed storing ${totalTermsStored} terms with ${totalTermErrors} errors`);
  
  endPhase('phase4');
  
  // ===== Final Metrics and Cleanup =====
  METRICS.endTime = performance.now();
  METRICS.totalDuration = METRICS.endTime - METRICS.startTime;
  
  LOGGER.info(`\n===== TF-IDF AGGREGATION COMPLETE =====`);
  LOGGER.info(`Created index for ${actualTotalDocuments} documents and ${totalTermsProcessed} terms`);
  LOGGER.info(`Total processing time: ${formatTime(METRICS.totalDuration)}`);
  LOGGER.info(`Documents stored: ${docStoreResult ? docStoreResult.processedDocs : 0}`);
  LOGGER.info(`Terms stored: ${totalTermsStored}`);
  LOGGER.info(`Total errors: ${METRICS.errors}`);
  
  // Save final metrics
  saveMetrics(resultsDir);
  
  // Clean up temporary files (optional)
  if (process.env.CLEAN_TMP) {
    LOGGER.info("Cleaning up temporary files...");
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
  
  // Return final statistics
  return {
    totalDocuments: actualTotalDocuments,
    totalTerms: totalTermsProcessed,
    totalTime: METRICS.totalDuration,
    errors: METRICS.errors
  };
}

/**
 * Initialize the distribution server and run the aggregation process
 */
distribution.node.start(async (server) => {
  try {
    LOGGER.info("===== INITIALIZING TF-IDF AGGREGATOR =====");
    
    // Set up nodes for distributed processing
    const num_nodes = 8; // Number of nodes to simulate
    const nodes = [];
    
    for(let i = 0; i < num_nodes; i++) {
      nodes.push({ ip: '127.0.0.1', port: 8110 + i });
    }
    
    // Helper function to spawn a node
    const spawn_node = (node) =>
      new Promise((resolve, reject) =>
        distribution.local.status.spawn(node, (e, v) => {
          LOGGER.info(`Spawned node at ${node.ip}:${node.port} with ID ${distribution.util.id.getNID(node)}`);
          resolve([e, v]);
        })
      );
    
    // Helper function to stop a node
    const stop_node = (node) =>
      new Promise((resolve, reject) =>
        distribution.local.comm.send(
          [],
          { service: "status", method: "stop", node: node },
          (e, v) => resolve([e, v])
        )
      );
    
    // Start the nodes
    LOGGER.info(`Starting ${nodes.length} distribution nodes...`);
    for (const node of nodes) {
      try {
        await spawn_node(node);
      } catch (e) {
        LOGGER.error(`Failed to start node at ${node.ip}:${node.port}`, e);
        server.close();
        return;
      }
    }
    
    // Set up groups
    const tfidfConfig = { gid: CONFIG.DOC_GROUP };
    const indexConfig = { gid: CONFIG.TERM_GROUP };
    
    const testGroup = {};
    const indexGroup = {};
    
    for(let i = 0; i < nodes.length; i++) {
      const sid = distribution.util.id.getSID(nodes[i]);
      testGroup[sid] = nodes[i];
      indexGroup[sid] = nodes[i];
    }
    
    LOGGER.info("Setting up distributed groups...");
    
    // Set up the TFIDF group
    await new Promise((resolve, reject) => {
      distribution.local.groups.put(tfidfConfig, testGroup, (e, v) => {
        if (e && !isEmptyObject(e)) {
          LOGGER.error("Error setting up TFIDF group:", e);
          reject(e);
        } else {
          LOGGER.info("TFIDF group set up successfully");
          resolve();
        }
      });
    });
    
    // Set up the INDEX group
    await new Promise((resolve, reject) => {
      distribution.local.groups.put(indexConfig, indexGroup, (e, v) => {
        if (e && !isEmptyObject(e)) {
          LOGGER.error("Error setting up INDEX group:", e);
          reject(e);
        } else {
          LOGGER.info("INDEX group set up successfully");
          resolve();
        }
      });
    });
    
    // Set up the group in TFIDF service
    await new Promise((resolve, reject) => {
      distribution.tfidf.groups.put(tfidfConfig, testGroup, (e, v) => {
        if (e && !isEmptyObject(e)) {
          LOGGER.error("Error setting up TFIDF service group:", e);
          reject(e);
        } else {
          LOGGER.info("TFIDF service group set up successfully");
          resolve();
        }
      });
    });
    
    // Run the aggregation process
    LOGGER.info("Starting TF-IDF aggregation process...");
    const result = await runAggregation();
    
    // Shutdown the nodes
    LOGGER.info("Shutting down distribution nodes...");
    for (const node of nodes) {
      try {
        await stop_node(node);
      } catch (e) {
        LOGGER.error(`Error stopping node at ${node.ip}:${node.port}`, e);
      }
    }
    
    LOGGER.info("TF-IDF aggregation completed successfully!");
    server.close();
    
  } catch (err) {
    LOGGER.error("Error during TF-IDF aggregation:", err);
    server.close();
    process.exit(1);
  }
});

// Main entry point when run directly
if (require.main === module) {
  // Enable garbage collection if possible
  try {
    if (!global.gc) {
      console.warn("Garbage collection not available - run with --expose-gc flag for better memory management");
    }
  } catch (e) {
    console.warn("Garbage collection not available - run with --expose-gc flag for better memory management");
  }
  
  // Parse command line arguments
  const args = process.argv.slice(2);
  args.forEach(arg => {
    if (arg.startsWith('--log=')) {
      CONFIG.LOGGING_LEVEL = arg.split('=')[1];
    } else if (arg === '--clean-tmp') {
      process.env.CLEAN_TMP = 'true';
    } else if (arg.startsWith('--max-docs-per-term=')) {
      CONFIG.MAX_DOCS_PER_TERM = parseInt(arg.split('=')[1]);
    } else if (arg.startsWith('--max-terms-per-doc=')) {
      CONFIG.MAX_TERMS_PER_DOC = parseInt(arg.split('=')[1]);
    }
  });
  
  // The distribution.node.start will handle running the main function
}

module.exports = {
  runAggregation,
  CONFIG
};