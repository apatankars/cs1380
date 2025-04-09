#!/usr/bin/env node

/**
 * Distributed Crawler-Indexer with Metrics Reporting
 * This script runs the distributed crawler with integrated indexing
 * and displays real-time metrics as the system progresses.
 */

const distribution = require('./config.js');
const id = distribution.util.id;
const fs = require('fs');
const path = require('path');

// Configuration
const num_nodes = 8;
const nodes = [
    { ip: '127.0.0.1', port: 8000 },
    { ip: '127.0.0.1', port: 8001 },
    { ip: '127.0.0.1', port: 8002 },
    { ip: '127.0.0.1', port: 8003 },
    { ip: '127.0.0.1', port: 8004 },
    { ip: '127.0.0.1', port: 8005 },
    { ip: '127.0.0.1', port: 8006 },
    { ip: '127.0.0.1', port: 8007 },
];

// System metrics
const systemMetrics = {
    startTime: Date.now(),
    nodes: {},
    aggregated: {
        crawling: {
            totalPagesProcessed: 0,
            lastPagesProcessed: 0,
            totalBytesDownloaded: 0,
            avgProcessingTime: 0
        },
        indexing: {
            totalDocumentsIndexed: 0,
            lastDocumentsIndexed: 0,
            totalBytesTransferred: 0,
            totalTermsExtracted: 0,
            avgIndexTime: 0,
            uniqueTerms: new Set()
        },
        memory: {
            avgHeapUsed: 0,
            peakHeapUsed: 0
        }
    },
    timestamps: [],
    growthRates: []
};

// Initialize node setup
const nids = [];
const taxonomy_group_group = {};
const taxonomy_group_config = { gid: 'taxonomy_group' };
for(let i = 0; i < num_nodes; i++) {
    nids.push(id.getNID(nodes[i]));
    taxonomy_group_group[id.getSID(nodes[i])] = nodes[i];
}

// Create metrics directory
const metricsDir = path.join(__dirname, 'metrics');
if (!fs.existsSync(metricsDir)) {
    fs.mkdirSync(metricsDir, { recursive: true });
}

// Function to get dynamic reporting interval (more frequent at start)
function getDynamicReportingInterval(pagesProcessed) {
    if (pagesProcessed < 50) return 15 * 1000; // 15 seconds for first 50 pages
    if (pagesProcessed < 200) return 30 * 1000; // 30 seconds for next 150 pages
    if (pagesProcessed < 1000) return 60 * 1000; // 1 minute for next 800 pages
    return 5 * 60 * 1000; // 5 minutes thereafter
}

// Helper to format bytes
function formatBytes(bytes) {
    if (bytes < 1024) return bytes + " bytes";
    else if (bytes < 1048576) return (bytes / 1024).toFixed(2) + " KB";
    else if (bytes < 1073741824) return (bytes / 1048576).toFixed(2) + " MB";
    else return (bytes / 1073741824).toFixed(2) + " GB";
}

// Calculate growth rate
function calculateGrowthRate(current, previous, elapsedSeconds) {
    if (!previous || elapsedSeconds === 0) return 0;
    const rate = (current - previous) / elapsedSeconds;
    return rate;
}

// Start the distributed system
distribution.node.start(async (server) => {
    console.log("STARTING DISTRIBUTED CRAWLER-INDEXER");
    console.log("===================================");
    
    // Function to fetch metrics from all nodes
    async function fetchNodeMetrics() {
        return new Promise((resolve) => {
            const remote = { gid: 'local', service: 'crawler', method: 'get_stats' };
            distribution.taxonomy_group.comm.send([], remote, (err, results) => {
                if (err) {
                    console.error("Error fetching metrics:", err);
                    resolve({});
                } else {
                    resolve(results || {});
                }
            });
        });
    }
    
    // Function to display metrics
    function displayMetrics(nodeMetrics, isDetailed = false) {
        const now = Date.now();
        const elapsedMs = now - systemMetrics.startTime;
        const elapsedSeconds = elapsedMs / 1000;
        const elapsedMinutes = elapsedSeconds / 60;
        
        // Previous metrics for growth rate calculation
        const prevPagesProcessed = systemMetrics.aggregated.crawling.totalPagesProcessed;
        const prevDocsIndexed = systemMetrics.aggregated.indexing.totalDocumentsIndexed;
        
        // Reset aggregated metrics
        systemMetrics.aggregated.crawling.totalPagesProcessed = 0;
        systemMetrics.aggregated.crawling.totalBytesDownloaded = 0;
        systemMetrics.aggregated.indexing.totalDocumentsIndexed = 0;
        systemMetrics.aggregated.indexing.totalBytesTransferred = 0;
        systemMetrics.aggregated.indexing.totalTermsExtracted = 0;
        systemMetrics.aggregated.memory.totalHeapUsed = 0;
        
        // Process metrics from each node
        let activeNodes = 0;
        for (const nodeId in nodeMetrics) {
            if (nodeMetrics[nodeId] && nodeMetrics[nodeId].crawling) {
                activeNodes++;
                
                // Store node metrics
                systemMetrics.nodes[nodeId] = nodeMetrics[nodeId];
                
                // Update aggregated metrics
                const metrics = nodeMetrics[nodeId];
                
                // Crawling metrics
                systemMetrics.aggregated.crawling.totalPagesProcessed += metrics.crawling?.pagesProcessed || 0;
                systemMetrics.aggregated.crawling.totalBytesDownloaded += metrics.crawling?.bytesDownloaded || 0;
                
                // Indexing metrics
                systemMetrics.aggregated.indexing.totalDocumentsIndexed += metrics.indexing?.documentsIndexed || 0;
                systemMetrics.aggregated.indexing.totalBytesTransferred += metrics.indexing?.bytesTransferred || 0;
                systemMetrics.aggregated.indexing.totalTermsExtracted += metrics.indexing?.termsExtracted || 0;
                
                // Add any unique terms to our set
                if (metrics.indexing?.uniqueTermsSample) {
                    metrics.indexing.uniqueTermsSample.forEach(term => 
                        systemMetrics.aggregated.indexing.uniqueTerms.add(term)
                    );
                }
                
                // Memory metrics
                if (metrics.memory?.peaks && metrics.memory.peaks.length > 0) {
                    const latestPeak = metrics.memory.peaks[metrics.memory.peaks.length - 1];
                    systemMetrics.aggregated.memory.totalHeapUsed += latestPeak.heapUsed || 0;
                    
                    // Track peak memory usage
                    if (latestPeak.heapUsed > (systemMetrics.aggregated.memory.peakHeapUsed || 0)) {
                        systemMetrics.aggregated.memory.peakHeapUsed = latestPeak.heapUsed;
                    }
                }
            }
        }
        
        // Calculate averages
        if (activeNodes > 0) {
            systemMetrics.aggregated.memory.avgHeapUsed = 
                systemMetrics.aggregated.memory.totalHeapUsed / activeNodes;
        }
        
        // Calculate growth rates
        const pagesGrowthRate = calculateGrowthRate(
            systemMetrics.aggregated.crawling.totalPagesProcessed,
            prevPagesProcessed,
            elapsedSeconds - (systemMetrics.timestamps.length > 0 ? 
                (systemMetrics.timestamps[systemMetrics.timestamps.length-1] / 1000) : 0)
        );
        
        const indexingGrowthRate = calculateGrowthRate(
            systemMetrics.aggregated.indexing.totalDocumentsIndexed,
            prevDocsIndexed,
            elapsedSeconds - (systemMetrics.timestamps.length > 0 ? 
                (systemMetrics.timestamps[systemMetrics.timestamps.length-1] / 1000) : 0)
        );
        
        // Store for historical tracking
        systemMetrics.timestamps.push(elapsedMs);
        systemMetrics.growthRates.push({
            timestamp: elapsedMs,
            pagesPerSecond: pagesGrowthRate,
            docsIndexedPerSecond: indexingGrowthRate
        });
        
        // Update last values for future growth calculations
        systemMetrics.aggregated.crawling.lastPagesProcessed = 
            systemMetrics.aggregated.crawling.totalPagesProcessed;
        systemMetrics.aggregated.indexing.lastDocumentsIndexed = 
            systemMetrics.aggregated.indexing.totalDocumentsIndexed;
        
        // Display the metrics
        console.log("\n=== CRAWLER-INDEXER METRICS ===");
        console.log(`Time elapsed: ${Math.floor(elapsedMinutes)}m ${Math.floor(elapsedSeconds % 60)}s`);
        console.log(`Active nodes: ${activeNodes}`);
        
        console.log("\n--- CRAWLING ---");
        console.log(`Pages processed: ${systemMetrics.aggregated.crawling.totalPagesProcessed}`);
        console.log(`Pages/minute: ${(pagesGrowthRate * 60).toFixed(2)}`);
        console.log(`Downloaded data: ${formatBytes(systemMetrics.aggregated.crawling.totalBytesDownloaded)}`);
        
        console.log("\n--- INDEXING ---");
        console.log(`Documents indexed: ${systemMetrics.aggregated.indexing.totalDocumentsIndexed}`);
        console.log(`Documents/minute: ${(indexingGrowthRate * 60).toFixed(2)}`);
        console.log(`Terms extracted: ${systemMetrics.aggregated.indexing.totalTermsExtracted}`);
        console.log(`Unique terms seen: ${systemMetrics.aggregated.indexing.uniqueTerms.size}`);
        console.log(`Indexing data transferred: ${formatBytes(systemMetrics.aggregated.indexing.totalBytesTransferred)}`);
        
        console.log("\n--- MEMORY ---");
        console.log(`Average heap used: ${systemMetrics.aggregated.memory.avgHeapUsed.toFixed(2)} MB`);
        console.log(`Peak heap used: ${systemMetrics.aggregated.memory.peakHeapUsed.toFixed(2)} MB`);
        
        // Save metrics to file
        fs.writeFileSync(
            path.join(metricsDir, 'crawler-indexer-metrics.json'),
            JSON.stringify(systemMetrics, (key, value) => {
                // Handle the Set by converting to Array
                if (key === 'uniqueTerms' && value instanceof Set) {
                    return Array.from(value);
                }
                return value;
            }, 2)
        );
        
        // Generate a CSV record for time-series tracking
        const csvRecord = `${elapsedMs},${systemMetrics.aggregated.crawling.totalPagesProcessed},` +
            `${pagesGrowthRate.toFixed(4)},${systemMetrics.aggregated.indexing.totalDocumentsIndexed},` +
            `${indexingGrowthRate.toFixed(4)},${systemMetrics.aggregated.indexing.totalTermsExtracted},` +
            `${systemMetrics.aggregated.indexing.uniqueTerms.size},${systemMetrics.aggregated.memory.avgHeapUsed.toFixed(2)}\n`;
            
        // Append to CSV
        fs.appendFileSync(
            path.join(metricsDir, 'crawler-indexer-timeseries.csv'),
            csvRecord
        );
        
        // If this is the first write, add a header to the CSV
        if (systemMetrics.timestamps.length === 1) {
            const header = "timestamp,pages_processed,pages_per_second,docs_indexed,docs_per_second," +
                "terms_extracted,unique_terms,avg_heap_mb\n";
            
            fs.writeFileSync(
                path.join(metricsDir, 'crawler-indexer-timeseries.csv'),
                header + csvRecord
            );
        }
        
        // If detailed mode, show node-specific metrics
        if (isDetailed) {
            console.log("\n--- DETAILED NODE METRICS ---");
            for (const nodeId in systemMetrics.nodes) {
                const nodeMetrics = systemMetrics.nodes[nodeId];
                console.log(`\nNode ${nodeId}:`);
                console.log(`  Pages processed: ${nodeMetrics.crawling?.pagesProcessed || 0}`);
                console.log(`  Documents indexed: ${nodeMetrics.indexing?.documentsIndexed || 0}`);
                
                // Show memory trends for this node
                if (nodeMetrics.memory?.peaks && nodeMetrics.memory.peaks.length > 0) {
                    const latestPeak = nodeMetrics.memory.peaks[nodeMetrics.memory.peaks.length - 1];
                    console.log(`  Current heap: ${latestPeak.heapUsed} MB / ${latestPeak.heapTotal} MB`);
                }
            }
        }
    }
    
    // Set up crawler cluster
    async function setupCluster() {
        console.log("Setting up crawler cluster...");
        
        // Create the taxonomy group
        await new Promise((resolve, reject) => {
            distribution.local.groups.put(taxonomy_group_config, taxonomy_group_group, (err, result) => {
                if (err) {
                    console.error("Error setting up taxonomy group:", err);
                    reject(err);
                } else {
                    console.log("Taxonomy group created successfully");
                    resolve(result);
                }
            });
        });
        
        // Create the index group with the same nodes
        const indexGroupConfig = { gid: 'index' };
        await new Promise((resolve, reject) => {
            distribution.local.groups.put(indexGroupConfig, taxonomy_group_group, (err, result) => {
                if (err) {
                    console.error("Error setting up index group:", err);
                    reject(err);
                } else {
                    console.log("Index group created successfully");
                    resolve(result);
                }
            });
        });

        // Create the index service group for all indexer nodes
        await new Promise((resolve, reject) => {
            distribution.index.groups.put(indexGroupConfig, taxonomy_group_group, (err, val) => {
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
        
        // Set up the node services
        const crawlerService = {
            // ... Your crawlerService implementation here ...
            // Use the enhanced version with metrics tracking
        };
        
        // Load indexer module
        const indexerModule = require('./distribution/local/indexer');
        
        // Register the crawler service
        await new Promise((resolve, reject) => {
            distribution.taxonomy_group.routes.put(crawlerService, 'crawler', (err, result) => {
                if (err) {
                    console.error("Error registering crawler service:", err);
                    reject(err);
                } else {
                    console.log("Crawler service registered successfully");
                    resolve(result);
                }
            });
        });
        
        // Register the indexer service
        await new Promise((resolve, reject) => {
            distribution.index.routes.put(indexerModule, 'indexer', (err, result) => {
                if (err) {
                    console.error("Error registering indexer service:", err);
                    reject(err);
                } else {
                    console.log("Indexer service registered successfully");
                    resolve(result);
                }
            });
        });
        
        // Initialize the crawler on all nodes
        await new Promise((resolve, reject) => {
            const remote = { gid: 'local', service: 'crawler', method: 'initialize' };
            distribution.taxonomy_group.comm.send([], remote, (err, result) => {
                if (err) {
                    console.error("Error initializing crawler:", err);
                    reject(err);
                } else {
                    console.log("Crawler initialized on all nodes");
                    resolve(result);
                }
            });
        });
        
        // Set up global info for the nodes
        await new Promise((resolve, reject) => {
            const remote = { gid: 'local', service: 'mem', method: 'put' };
            distribution.taxonomy_group.comm.send([{ nodes, num_nodes }, 'global_info'], remote, (err, result) => {
                if (err) {
                    console.error("Error setting global info:", err);
                    reject(err);
                } else {
                    console.log("Global info set on all nodes");
                    resolve(result);
                }
            });
        });
        
        // Add the starting URL
        const startLinks = [
            '/wiki/Cnidaria',  // Sea life
            '/wiki/Plant',     // Plants
            '/wiki/Fungi',     // Fungi
            '/wiki/Lepidoptera' // Butterflies
        ];
        
        // Add each starting link
        for (const link of startLinks) {
            await new Promise((resolve) => {
                const targetNode = nodes[parseInt(id.getID(link).slice(0, 8), 16) % num_nodes];
                const remote = { node: targetNode, gid: 'local', service: 'crawler', method: 'add_link_to_crawl' };
                
                distribution.local.comm.send([link], remote, (err, result) => {
                    if (err) {
                        console.error(`Error adding start link ${link}:`, err);
                    } else {
                        console.log(`Added start link: ${link}`);
                    }
                    resolve();
                });
            });
        }
        
        console.log("Cluster setup complete");
    }
    
    // Run the crawling task with metrics reporting
    async function runCrawlTask() {
        console.log("Starting crawl task with metrics reporting");
        
        // Store the interval ID so we can clear it later
        let metricsIntervalId = null;
        
        // Function to update reporting interval based on current progress
        function updateReportingInterval() {
            const pagesProcessed = systemMetrics.aggregated.crawling.totalPagesProcessed;
            const newInterval = getDynamicReportingInterval(pagesProcessed);
            
            // Clear existing interval and set new one
            if (metricsIntervalId) clearInterval(metricsIntervalId);
            
            metricsIntervalId = setInterval(async () => {
                const metrics = await fetchNodeMetrics();
                displayMetrics(metrics);
                
                // Recursively update the interval
                updateReportingInterval();
            }, newInterval);
            
            console.log(`Metrics reporting interval updated to ${newInterval/1000} seconds`);
        }
        
        // Create CSV file with header
        const csvHeader = "timestamp,pages_processed,pages_per_second,docs_indexed,docs_per_second," +
            "terms_extracted,unique_terms,avg_heap_mb\n";
        fs.writeFileSync(path.join(metricsDir, 'crawler-indexer-timeseries.csv'), csvHeader);
        
        // Function to trigger crawling on all nodes
        const crawlBatch = async (batchSize = 5) => {
            for (let i = 0; i < batchSize; i++) {
                await new Promise((resolve) => {
                    const remote = { gid: 'local', service: 'crawler', method: 'crawl_one' };
                    distribution.taxonomy_group.comm.send([], remote, (err, result) => {
                        resolve();
                    });
                });
            }
        };
        
        // Start initial metrics collection
        const initialMetrics = await fetchNodeMetrics();
        displayMetrics(initialMetrics, true);  // Show detailed metrics for first report
        
        // Start with frequent reporting
        updateReportingInterval();
        
        // Run the crawler
        console.log("Crawler running - press Ctrl+C to stop");
        
        // Start batch crawling
        while (true) {
            await crawlBatch(10);  // Process 10 pages in parallel
            await new Promise(resolve => setTimeout(resolve, 500));  // Small pause
        }
    }
    
    // Function to shut down the system
    async function shutDown() {
        console.log("\nShutting down crawler-indexer system...");
        
        // Get final metrics
        const finalMetrics = await fetchNodeMetrics();
        console.log("\n=== FINAL METRICS REPORT ===");
        displayMetrics(finalMetrics, true);  // Show detailed metrics for final report
        
        // Clean up resources
        for (let i = 0; i < num_nodes; i++) {
            await new Promise((resolve) => {
                distribution.local.comm.send([], { 
                    service: 'status', 
                    method: 'stop', 
                    node: nodes[i] 
                }, (err, val) => {
                    if (err) {
                        console.error(`Error stopping node ${nodes[i].ip}:${nodes[i].port}:`, err);
                    } else {
                        console.log(`Node stopped at ${nodes[i].ip}:${nodes[i].port}`);
                    }
                    resolve();
                });
            });
        }
        
        // Close server
        server.close(() => {
            console.log("Server closed");
            console.log("Metrics data saved to:", path.join(metricsDir, 'crawler-indexer-metrics.json'));
            console.log("Time series data saved to:", path.join(metricsDir, 'crawler-indexer-timeseries.csv'));
            process.exit(0);
        });
    }
    
    // Trap Ctrl+C to shut down gracefully
    process.on('SIGINT', async () => {
        await shutDown();
    });
    
    try {
        // Run the setup and crawl task
        await setupCluster();
        await runCrawlTask();
    } catch (error) {
        console.error("Error running crawler-indexer:", error);
        await shutDown();
    }
});