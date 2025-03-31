/**
 * mrPerfTest.js
 * 
 * Performance benchmark for MapReduce word count implementation
 */

const distribution = require('../config.js');
const id = distribution.util.id;
const fs = require('fs');

// Define node group
const wordcountGroup = {};

// Define nodes
const n1 = {ip: '127.0.0.1', port: 7110};
const n2 = {ip: '127.0.0.1', port: 7111};
const n3 = {ip: '127.0.0.1', port: 7112};

// Local server
let localServer = null;

// Configuration
const DATASET_SIZES = [5, 10, 20, 50]; // Different dataset sizes to test
const ITERATIONS = 3; // Number of iterations for each size
const OUTPUT_FILE = './wordcount-performance.json';

// Word count mapper and reducer
const mapper = (key, value) => {
  const words = value.toLowerCase().split(/\s+/).filter(word => word.length > 0);
  const out = [];
  
  words.forEach(word => {
    const cleanWord = word.replace(/[^\w]/g, '');
    if (cleanWord.length > 0) {
      const o = {};
      o[cleanWord] = 1;
      out.push(o);
    }
  });
  
  return out;
};

const reducer = (key, values) => {
  const out = {};
  out[key] = values.reduce((sum, v) => sum + v, 0);
  return out;
};

// Generate test data
function generateTestData(size) {
  const words = ['the', 'quick', 'brown', 'fox', 'jumps', 'over', 'lazy', 'dog',
                'hello', 'world', 'mapreduce', 'benchmark', 'performance', 'test'];
  const dataset = [];
  
  for (let i = 0; i < size; i++) {
    let doc = '';
    const wordCount = 10 + Math.floor(Math.random() * 10); // 10-20 words per doc
    
    for (let j = 0; j < wordCount; j++) {
      const randomWord = words[Math.floor(Math.random() * words.length)];
      doc += randomWord + ' ';
    }
    
    dataset.push({[`doc${i}`]: doc.trim()});
  }
  
  return dataset;
}

// Helper function to extract keys from dataset
function getDatasetKeys(dataset) {
  return dataset.map((o) => Object.keys(o)[0]);
}

// Run benchmark for a specific dataset size
function runBenchmarkForSize(size, results, callback) {
  console.log(`\nTesting with dataset size: ${size} documents`);
  
  const sizeResults = {
    datasetSize: size,
    iterations: []
  };
  
  let currentIteration = 0;
  
  function runIteration() {
    if (currentIteration >= ITERATIONS) {
      // Calculate averages
      const avgLatency = sizeResults.iterations.reduce((sum, it) => sum + it.latency, 0) / ITERATIONS;
      const avgThroughput = sizeResults.iterations.reduce((sum, it) => sum + it.throughput, 0) / ITERATIONS;
      
      sizeResults.averages = {
        latency: parseFloat(avgLatency.toFixed(2)),
        throughput: parseFloat(avgThroughput.toFixed(2))
      };
      
      console.log(`  Average latency: ${avgLatency.toFixed(2)} ms`);
      console.log(`  Average throughput: ${avgThroughput.toFixed(2)} docs/second`);
      
      results.configurations.push(sizeResults);
      callback();
      return;
    }
    
    console.log(`  Running iteration ${currentIteration + 1}/${ITERATIONS}`);
    
    // Generate dataset for this iteration
    const dataset = generateTestData(size);
    
    // Function to run MapReduce after data is loaded
    const doMapReduce = () => {
      const startTime = Date.now();
      
      distribution.wordcount.mr.exec({
        keys: getDatasetKeys(dataset), 
        map: mapper, 
        reduce: reducer
      }, (e, v) => {
        if (e) {
          console.error('Error running MapReduce:', e);
          callback(e);
          return;
        }
        
        const endTime = Date.now();
        const latency = endTime - startTime;
        const throughput = (size / latency) * 1000; // docs per second
        
        console.log(`    Latency: ${latency} ms`);
        console.log(`    Throughput: ${throughput.toFixed(2)} docs/second`);
        
        sizeResults.iterations.push({
          iteration: currentIteration + 1,
          latency: latency,
          throughput: parseFloat(throughput.toFixed(2)),
          uniqueWords: v.length
        });
        
        currentIteration++;
        runIteration();
      });
    };
    
    // Store the dataset
    let cntr = 0;
    dataset.forEach((o) => {
      const key = Object.keys(o)[0];
      const value = o[key];
      distribution.wordcount.store.put(value, key, (e, v) => {
        if (e) {
          console.error('Error storing data:', e);
          callback(e);
          return;
        }
        
        cntr++;
        if (cntr === dataset.length) {
          doMapReduce();
        }
      });
    });
  }
  
  // Start the first iteration
  runIteration();
}

// Main benchmark function
function runBenchmark() {
  console.log('Starting MapReduce performance benchmark...');
  
  const results = {
    environment: {
      platform: process.platform,
      arch: process.arch,
      nodeVersion: process.version,
      cpus: require('os').cpus().length,
      date: new Date().toISOString()
    },
    configurations: []
  };
  
  // Set up nodes in group
  wordcountGroup[id.getSID(n1)] = n1;
  wordcountGroup[id.getSID(n2)] = n2;
  wordcountGroup[id.getSID(n3)] = n3;
  
  const startNodes = (cb) => {
    distribution.local.status.spawn(n1, (e, v) => {
      console.log('Node 1 spawned');
      distribution.local.status.spawn(n2, (e, v) => {
        console.log('Node 2 spawned');
        distribution.local.status.spawn(n3, (e, v) => {
          console.log('Node 3 spawned');
          cb();
        });
      });
    });
  };
  
  // Start the node
  distribution.node.start((server) => {
    localServer = server;
    console.log('Local node started');
    
    startNodes(() => {
      const wordcountConfig = {gid: 'wordcount'};
      
      distribution.local.groups.put(wordcountConfig, wordcountGroup, (e, v) => {
        if (e) {
          console.error('Error setting up local group:', e);
          cleanup();
          return;
        }
        console.log('Local group configured');
        
        distribution.wordcount.groups.put(wordcountConfig, wordcountGroup, (e, v) => {
          
          console.log('Wordcount group configured');
          
          // Run benchmarks for each size sequentially
          let currentSizeIndex = 0;
          
          function benchmarkNextSize() {
            if (currentSizeIndex >= DATASET_SIZES.length) {
              // All sizes tested, save results and clean up
              fs.writeFileSync(OUTPUT_FILE, JSON.stringify(results, null, 2));
              console.log(`\nBenchmark complete. Results saved to ${OUTPUT_FILE}`);
              printSummary(results);
              cleanup();
              return;
            }
            
            const size = DATASET_SIZES[currentSizeIndex++];
            runBenchmarkForSize(size, results, (err) => {
              if (err) {
                console.error('Error during benchmark:', err);
                cleanup();
                return;
              }
              benchmarkNextSize();
            });
          }
          
          // Start with the first size
          benchmarkNextSize();
        });
      });
    });
  });
}

// Print benchmark summary
function printSummary(results) {
  console.log('\n===== BENCHMARK SUMMARY =====');
  console.log('Environment:');
  console.log(`  Platform: ${results.environment.platform}`);
  console.log(`  Architecture: ${results.environment.arch}`);
  console.log(`  Node.js Version: ${results.environment.nodeVersion}`);
  console.log(`  CPU Cores: ${results.environment.cpus}`);
  
  console.log('\nPerformance Results:');
  results.configurations.forEach(config => {
    console.log(`\nDataset Size: ${config.datasetSize} documents`);
    console.log(`  Average Latency: ${config.averages.latency} ms`);
    console.log(`  Average Throughput: ${config.averages.throughput} docs/second`);
  });
  
  // Analyze scalability if we have multiple sizes
  if (results.configurations.length > 1) {
    console.log('\nScalability Analysis:');
    const smallestConfig = results.configurations[0];
    const largestConfig = results.configurations[results.configurations.length - 1];
    
    const sizeFactor = largestConfig.datasetSize / smallestConfig.datasetSize;
    const latencyFactor = largestConfig.averages.latency / smallestConfig.averages.latency;
    
    console.log(`  Dataset size increased by factor: ${sizeFactor}`);
    console.log(`  Latency increased by factor: ${latencyFactor.toFixed(2)}`);
    
    if (latencyFactor < sizeFactor) {
      console.log('  The system shows sub-linear scaling in latency, which is good!');
    } else if (latencyFactor === sizeFactor) {
      console.log('  The system shows linear scaling in latency.');
    } else {
      console.log('  The system shows super-linear scaling in latency, indicating potential bottlenecks as data size increases.');
    }
    
    const throughputRatio = largestConfig.averages.throughput / smallestConfig.averages.throughput;
    console.log(`  Throughput ratio between smallest and largest dataset: ${throughputRatio.toFixed(2)}`);
    
    if (throughputRatio > 0.8) {
      console.log('  Throughput remains relatively stable as dataset size increases, indicating good scalability.');
    } else {
      console.log(`  Throughput decreases by ${((1 - throughputRatio) * 100).toFixed(2)}% as dataset size increases, suggesting potential optimization opportunities.`);
    }
  }
}

// Cleanup function
function cleanup() {
  console.log('\nCleaning up...');
  
  const remote = {service: 'status', method: 'stop'};
  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
        localServer.close();
        console.log('Cleanup complete');
      });
    });
  });
}

// Run the benchmark
runBenchmark();