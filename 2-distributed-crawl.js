const distribution = require('./config.js');
const id = distribution.util.id;

// const num_nodes = 8;
const num_nodes = 4;
const nodes = [
    { ip: '3.225.196.65', port: 8000},   
    { ip: '34.231.129.22', port: 8000},
    { ip: '35.168.108.255', port: 8000},
    { ip: '54.145.177.180', port: 8000},

    // { ip: '3.87.247.123', port: 8000},
    // { ip: '54.89.224.30', port: 8000},
    // { ip: '3.84.14.167', port: 8000},
    // { ip: '54.157.193.198', port: 8000}
];

// const nodes = [];
const nids = [];
const taxonomy_group_group = {};
const taxonomy_group_config = { gid: 'taxonomy_group' };
for(let i = 0; i < num_nodes; i++) {
    // nodes.push({ ip: '127.0.0.1', port: 7110 + i });
    nids.push(id.getNID(nodes[i]));
    taxonomy_group_group[id.getSID(nodes[i])] = nodes[i];
}

distribution.node.start(async (server) => {
    const spawn_nx = (nx) => new Promise((resolve, reject) => 
        distribution.local.status.spawn(nx, (e, v) => 
            resolve(e, v)));

    const stop_nx = (nx) => new Promise((resolve, reject) =>
        distribution.local.comm.send([], { service: 'status', method: 'stop', node: nx }, (e, v) =>
            resolve(e, v)));

    const get_nx = (link) => nodes[parseInt(id.getID(link).slice(0, 8), 16) % num_nodes];

    const setup_cluster = (cb) => {
        console.log("SETTING UP CLUSTER...")
        const crawlerService = {
            initialize: (cb) => {
                const fs = require('fs');

                if(!fs.existsSync(`./crawler-files`)) fs.mkdirSync(`./crawler-files`, { recursive: true });
                if(!fs.existsSync(`./crawler-files/logs`)) fs.mkdirSync(`./crawler-files/logs`, { recursive: true });
                if(!fs.existsSync(`./crawler-files/metrics`)) fs.mkdirSync(`./crawler-files/metrics`, { recursive: true });
                
                global.log_file_path = `./crawler-files/logs/log-${global.nodeConfig.port}.txt`;
                global.metrics_file_path = `./crawler-files/metrics/metrics-${global.nodeConfig.port}.json`;
                
                // Initialize metrics
                global.metrics = {
                    crawling: {
                        pagesProcessed: 0,
                        totalCrawlTime: 0,
                        bytesDownloaded: 0,
                        avgProcessingTime: 0
                    },
                    indexing: {
                        documentsIndexed: 0,
                        totalIndexTime: 0,
                        bytesTransferred: 0,
                        avgIndexTime: 0,
                        termsExtracted: 0
                    },
                    memory: {
                        peaks: [],
                        averages: []
                    },
                    startTime: Date.now()
                };
                
                // Save metrics periodically
                global.metricsInterval = setInterval(() => {
                    // Add memory metrics
                    const memUsage = process.memoryUsage();
                    global.metrics.memory.peaks.push({
                        timestamp: Date.now(),
                        heapUsed: Math.round(memUsage.heapUsed / 1024 / 1024),
                        heapTotal: Math.round(memUsage.heapTotal / 1024 / 1024)
                    });
                    
                    fs.writeFileSync(global.metrics_file_path, JSON.stringify(global.metrics, null, 2));
                }, 60000); // Every minute
                
                const links_to_crawl_map = new Map();
                const crawled_links_map = new Map();
                distribution.local.mem.put(links_to_crawl_map, 'links_to_crawl_map', (e, v) => {
                    distribution.local.mem.put(crawled_links_map, 'crawled_links_map', (e, v) => {
                        distribution.local.store.get('links_to_crawl', (e1, v1) => {
                            distribution.local.store.get('crawled_links', (e2, v2) => {
                                if(!e1 && !e2) {
                                    const saved_links_to_crawl = v1.split('\n').filter(s => s.length > 0);
                                    const saved_crawled_links = v2.split('\n').filter(s => s.length > 0);
                                    saved_links_to_crawl.map(link => links_to_crawl_map.set(link, true));
                                    saved_crawled_links.map(link => crawled_links_map.set(link, true));
                                }
                
                                cb();
                            });
                        });
                    });
                });
            },

            getStopWordsSet: () => {
                // Create a set of common stop words to filter out
                return new Set([
                    // Basic English stop words
                    "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while",
                    "of", "at", "by", "for", "with", "about", "against", "between", "into", "through",
                    "during", "before", "after", "above", "below", "to", "from", "up", "down", "in",
                    "out", "on", "off", "over", "under", "again", "further", "then", "once", "here",
                    "there", "when", "where", "why", "how", "all", "any", "both", "each", "few",
                    "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own",
                    "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don",
                    "should", "now", "d", "ll", "m", "o", "re", "ve", "y", "ain", "aren", "couldn",
                    "didn", "doesn", "hadn", "hasn", "haven", "isn", "ma", "mightn", "mustn", "needn",
                    "shan", "shouldn", "wasn", "weren", "won", "wouldn",
                    
                    // Wiki-specific terms
                    "wiki", "wikipedia", "edit", "article", "page", "reference", "https", "http",
                    "cite", "retrieved", "ref", "jpg", "png", "svg", "commons", "category", "categories"
                ]);
            },

            add_link_to_crawl: (link, cb) => {
                // const fs = require('fs');
                // fs.appendFileSync(global.log_file_path, `Adding ${link} to crawl\n`);

                distribution.local.mem.get('links_to_crawl_map', (e1, links_to_crawl_map) => {
                    distribution.local.mem.get('crawled_links_map', (e2, crawled_links_map) => {

                        if(links_to_crawl_map.has(link)) return cb();
                        if(crawled_links_map.has(link)) return cb();

                        distribution.local.mem.get('links_to_crawl_map', (e1, v1) => {
                            v1.set(link, true);
                            cb();
                        });
                    });
                });
            },

            get_stats: (cb) => {
                distribution.local.mem.get('links_to_crawl_map', (e1, links_to_crawl_map) => {
                    distribution.local.mem.get('crawled_links_map', (e2, crawled_links_map) => {

                        const fs = require('fs');
                        let num_target_found = 0;

                        try {
                            const store_path = '/home/ec2-user/cs1380-final-project-repo/store';
                            if (fs.existsSync(store_path)) {
                                const folders = fs.readdirSync(store_path).filter(folder => !folder.includes('.'));
                                const counts = folders.map(folder => {
                                    const subfolder = `${store_path}/${folder}`;
                                    if (fs.existsSync(subfolder) && fs.statSync(subfolder).isDirectory()) {
                                        return fs.readdirSync(subfolder).length;
                                    }
                                    return 0;
                                });
                                num_target_found = counts.reduce((a, b) => a + b, 0) - 2;
                            }
                        } catch (err) {
                            console.error("Error while counting targets:", err);
                            num_target_found = 0;
                        }

                        const stats = {
                            links_to_crawl: links_to_crawl_map.size,
                            crawled_links: crawled_links_map.size,
                            num_target_found: num_target_found
                        }

                        cb(null, stats);
                    });
                });
            },

            save_maps_to_disk: (cb) => {
                distribution.local.mem.get('links_to_crawl_map', (e1, links_to_crawl_map) => {
                    distribution.local.mem.get('crawled_links_map', (e2, crawled_links_map) => {

                        const links_to_crawl_data = Array.from(links_to_crawl_map.keys()).join('\n');
                        const crawled_links_data = Array.from(crawled_links_map.keys()).join('\n');

                        distribution.local.store.put(links_to_crawl_data, 'links_to_crawl', (e, v) => {
                            distribution.local.store.put(crawled_links_data, 'crawled_links', (e, v) => {
                                cb();
                            });
                        });
                    });
                });
            },

            crawl_one: (cb) => {
                // Record start time for metrics
                const crawlStartTime = Date.now();
                
                distribution.local.mem.get('links_to_crawl_map', (e1, links_to_crawl_map) => {
                    distribution.local.mem.get('crawled_links_map', (e2, crawled_links_map) => {                        
                        // get link to crawl
                        if(links_to_crawl_map.size === 0) return cb();
                        const [url, _] = links_to_crawl_map.entries().next().value;
                        links_to_crawl_map.delete(url);
                        if(crawled_links_map.has(url)) return cb();

                        // crawl it!
                        fetch(`https://en.wikipedia.org${url}`)
                            .then((response) => {
                                const contentLength = response.headers.get('content-length') || 0;
                                // Update metrics
                                global.metrics.crawling.bytesDownloaded += parseInt(contentLength);
                                return response.text();
                            })
                            .then((html) => {
                                const parse = require('node-html-parser').parse;
                                const root = parse(html);

                                const biota = root.querySelector('table.infobox.biota');
                                const biota_rows = biota?.querySelectorAll('tr');
                            
                                const hierarchy = biota_rows?.map((row) => {
                                    const td_data = row.querySelectorAll('td');
                                    if(td_data.length !== 2) return null;
                                
                                    const label = td_data[0].text.trim().toLocaleLowerCase().slice(0, -1);
                                    const value = td_data[1].text.trim().toLocaleLowerCase();
                                    return [label, value];
                                }).filter(item => item !== null);
                            
                                const binomial_name = biota?.querySelector('span.binomial')?.text?.trim().toLocaleLowerCase();
                                
                                const links_on_page = root.querySelectorAll('a').map(link => link.getAttribute('href'))
                                    .filter(link => link !== null && link !== undefined)
                                    .filter(link => link.startsWith('/wiki/'))
                                    .filter(link => !link.includes('.JPG'))
                                    .filter(link => !link.includes('.jpg'))
                                    .filter(link => !link.includes('.JPEG'))
                                    .filter(link => !link.includes('.jpeg'))
                                    .filter(link => !link.includes('.PNG'))
                                    .filter(link => !link.includes('.png'))
                                    .filter(link => !link.includes('#'))
                                    .filter(link => !link.includes(':'));
                        
                                const is_plant = hierarchy?.find(pair => pair[0] === 'kingdom' && pair[1].includes('plantae'));
                                const is_fungi = hierarchy?.find(pair => pair[0] === 'kingdom' && pair[1].includes('fungi'));
                                const is_sealife = hierarchy?.find(pair => pair[0] === 'phylum' && pair[1].includes('cnidaria'));
                                const is_butterfly = hierarchy?.find(pair => pair[0] === 'order' && pair[1].includes('lepidoptera'));
                                const is_target_class = is_plant || is_fungi || is_sealife || is_butterfly;
                                
                                if(is_target_class && binomial_name) {
                                    // Pre-process the page text - this was moved from indexer to crawler
                                    const page_text = root.text;
                                    const stopWords = crawlerService.getStopWordsSet();
                                    const alphaOnlyPattern = /^[a-z]+$/;
                                    
                                    // Extract and pre-process words
                                    const all_words = (page_text.match(/\b\w+\b/g) || [])
                                        .map(word => word.toLocaleLowerCase())
                                        .filter(word => word.length > 2) // Filter out very short words
                                        .filter(word => alphaOnlyPattern.test(word)) // Only alphabetic words
                                        .filter(word => !stopWords.has(word)); // Filter out stop words
                                    
                                    // Count word occurrences
                                    const wordCounts = new Map();
                                    for (const word of all_words) {
                                        wordCounts.set(word, (wordCounts.get(word) || 0) + 1);
                                    }
                                    
                                    // Convert to the format expected by the indexer
                                    const species_data = {
                                        hierarchy: hierarchy,
                                        binomial_name: binomial_name,
                                        url: url,
                                        // Send wordCounts instead of all words to reduce transfer size
                                        word_counts: Object.fromEntries(wordCounts),
                                        total_words: all_words.length,
                                        // Keep a small sample of unique words for debugging
                                        word_sample: Array.from(wordCounts.keys()).slice(0, 10)
                                    };
                                    
                                    // Store the original document
                                    const path_safe_url = url.replace(/\//g, '.');
                                    const uncompressed_data = JSON.stringify(species_data);
                                    
                                    // Calculate data size for metrics
                                    const dataSize = Buffer.byteLength(uncompressed_data, 'utf8');
                                    global.metrics.indexing.bytesTransferred += dataSize;
                                    global.metrics.indexing.termsExtracted += wordCounts.size;
                                    
                                    distribution.local.store.put(uncompressed_data, path_safe_url, (e, v) => {
                                        // After saving, send to the indexer
                                        const indexStartTime = Date.now();
                                        
                                        // Create an index group if it doesn't exist
                                        distribution.local.groups.get('index', (err, indexGroup) => {
                                            if (err || !indexGroup) {
                                                // If index group doesn't exist, create it using the taxonomy group
                                                distribution.local.mem.get('global_info', (e, v) => {
                                                    if (e) {
                                                        console.error('Error getting global info:', e);
                                                        return;
                                                    }
                                                    
                                                    const { nodes } = v;
                                                    const indexGroupConfig = { gid: 'index' };
                                                    const indexGroup = {};
                                                    
                                                    nodes.forEach(node => {
                                                        indexGroup[distribution.util.id.getSID(node)] = node;
                                                    });
                                                    
                                                    // Create the index group
                                                    distribution.local.groups.put(indexGroupConfig, indexGroup, (err, result) => {
                                                        if (err) {
                                                            console.error('Error creating index group:', err);
                                                            return;
                                                        }
                                                        
                                                        // Now call the indexer
                                                        callIndexService(species_data, indexStartTime);
                                                    });
                                                });
                                            } else {
                                                // Index group exists, call the indexer directly
                                                callIndexService(species_data, indexStartTime);
                                            }
                                        });
                                        
                                        // Helper function to call the index service
                                        function callIndexService(data, startTime) {
                                            // Set up the indexer service if needed
                                            distribution.index?.indexer?.index(data, (err, indexResult) => {
                                                const indexEndTime = Date.now();
                                                const indexDuration = indexEndTime - startTime;
                                                
                                                // Update indexing metrics
                                                global.metrics.indexing.documentsIndexed++;
                                                global.metrics.indexing.totalIndexTime += indexDuration;
                                                global.metrics.indexing.avgIndexTime = 
                                                    global.metrics.indexing.totalIndexTime / global.metrics.indexing.documentsIndexed;
                                                
                                                if (err) {
                                                    console.error(`Error indexing document ${url}:`, err);
                                                } else {
                                                    console.log(`Successfully indexed ${url} in ${indexDuration}ms`);
                                                    console.log(`Indexing metrics:`, indexResult?.metrics || {});
                                                }
                                            });
                                        }
                                    });
                                }
                                
                                crawled_links_map.set(url, true);

                                distribution.local.mem.get('global_info', (e, v) => {
                                    const { nodes, num_nodes } = v;

                                    const get_nx = (link) => nodes[parseInt(distribution.util.id.getID(link).slice(0, 8), 16) % num_nodes];
                                    const new_links = [...new Set(is_target_class ? links_on_page : [])];
                                    new_links.map(link => {
                                        const remote = { node: get_nx(link), gid: 'local', service: 'crawler', method: 'add_link_to_crawl'};
                                        distribution.local.comm.send([link], remote, (e, v) => {});
                                    });
                    
                                    // Update crawling metrics
                                    const crawlEndTime = Date.now();
                                    const crawlDuration = crawlEndTime - crawlStartTime;
                                    global.metrics.crawling.pagesProcessed++;
                                    global.metrics.crawling.totalCrawlTime += crawlDuration;
                                    global.metrics.crawling.avgProcessingTime = 
                                        global.metrics.crawling.totalCrawlTime / global.metrics.crawling.pagesProcessed;
                                    
                                    setTimeout(() => {
                                        cb();
                                    }, 1000);
                                });
                            });
                    });
                });
            },
            cleanup: (callback) => {
                if (global.metricsInterval) {
                    clearInterval(global.metricsInterval);
                }
                
                // Save final metrics
                const fs = require('fs');
                global.metrics.endTime = Date.now();
                global.metrics.totalRuntime = (global.metrics.endTime - global.metrics.startTime) / 1000;
                
                fs.writeFileSync(global.metrics_file_path, JSON.stringify(global.metrics, null, 2));
                
                callback(null, { status: 'success', metrics: global.metrics });
            }
        }

        distribution.local.groups.put(taxonomy_group_config, taxonomy_group_group, (e, v) => {
            distribution.taxonomy_group.groups.put(taxonomy_group_config, taxonomy_group_group, (e, v) => {

                distribution.taxonomy_group.routes.put(crawlerService, 'crawler', (e, v) => {

                    const remote = {gid: 'local', service: 'crawler', method: 'initialize'};
                    distribution.taxonomy_group.comm.send([], remote, (e, v) => {

                        const remote = { gid: 'local', service: 'mem', method: 'put'};
                        distribution.taxonomy_group.comm.send([{ nodes, num_nodes }, 'global_info'], remote, (e, v) => {

                            // const link = '/wiki/Plant';
                            // const link = '/wiki/Animal';
                            const link = '/wiki/Cnidaria';

                            const remote = { node: get_nx(link), gid: 'local', service: 'crawler', method: 'add_link_to_crawl'};
                            distribution.local.comm.send([link], remote, (e, v) => {

                                cb();
                                

                            });

                        });
                        
                    });
                    
                });
        
            });
                    
        });
    };

    const run_task = async (cb) => {
        console.log("STARTING MAIN RUN TASK...")
        
        console.log("Setting up index group...");
        const indexGroupConfig = { gid: 'index' };
        
        try {
            // Register the indexer service
            const indexerModule = require('./distribution/local/indexer');
            
            // Create the index group using the taxonomy group nodes
            await new Promise((resolve, reject) => {
                distribution.local.groups.get('taxonomy_group', (err, taxonomyGroup) => {
                    if (err) {
                        console.error("Error getting taxonomy group:", err);
                        reject(err);
                        return;
                    }
                    
                    distribution.local.groups.put(indexGroupConfig, taxonomyGroup, (err, result) => {
                        if (err) {
                            console.error("Error creating index group:", err);
                            reject(err);
                            return;
                        }
                        
                        // Now create the index group in the distributed system
                        distribution.index.groups.put(indexGroupConfig, taxonomyGroup, (err, result) => {
                            if (err && Object.keys(err).length > 0) {
                                console.error("Error creating distributed index group:", err);
                                reject(err);
                                return;
                            }
                            
                            // Register the indexer service
                            distribution.index.routes.put(indexerModule, 'indexer', (err, result) => {
                                if (err) {
                                    console.error("Error registering indexer service:", err);
                                    reject(err);
                                    return;
                                }
                                
                                console.log("Index group and indexer service set up successfully");
                                resolve();
                            });
                        });
                    });
                });
            });
            
            console.log("Starting crawling with indexing...");
        } catch (error) {
            console.error("Error in setup:", error);
        }

        const crawl_iter = () => new Promise((resolve, reject) => {
            const remote = { gid: 'local', service: 'crawler', method: 'crawl_one'};
            distribution.taxonomy_group.comm.send([], remote, (e, v) => {
                resolve();
            });
        })

        const save_iter = () => new Promise((resolve, reject) => {
            const remote = { gid: 'local', service: 'crawler', method: 'save_maps_to_disk'};
            distribution.taxonomy_group.comm.send([], remote, (e, v) => {
                resolve();
            });
        });

        const get_time_to_sleep = (n) => n - 50 + 100 * Math.random();
        const sleep_iter = () => new Promise((resolve, reject) => {
            setTimeout(() => {
                resolve();
            }, get_time_to_sleep(100));
        });

        let running_links_to_crawl = 0;
        let running_crawled_links = 0;
        let running_num_target_found = 0;

        const stat_iter = () => new Promise((resolve, reject) => {
            const remote = { gid: 'local', service: 'crawler', method: 'get_stats'};
            distribution.taxonomy_group.comm.send([], remote, (e, v) => {
                let sum_links_to_crawl = 0;
                let sum_crawled_links = 0;
                let sum_num_target_found = 0;
                Object.keys(v).forEach(key => {
                    sum_links_to_crawl += v[key].links_to_crawl;
                    sum_crawled_links += v[key].crawled_links;
                    sum_num_target_found += v[key].num_target_found;
                });

                running_crawled_links = sum_crawled_links;
                running_links_to_crawl = sum_links_to_crawl;
                running_num_target_found = sum_num_target_found;

                if(prev_running_crawled_links === 0) prev_running_crawled_links = sum_crawled_links;
                if(prev_running_links_to_crawl === 0) prev_running_links_to_crawl = sum_links_to_crawl;
                if(prev_running_num_target_found === 0) prev_running_num_target_found = sum_num_target_found;

                console.log(`sum_links_to_crawl = ${sum_links_to_crawl}, sum_crawled_links = ${sum_crawled_links}`);
                console.log("TOTAL PAGES SO FAR =", sum_num_target_found);

                resolve();
            });
        });

        let prev_running_links_to_crawl = 0;
        let prev_running_crawled_links = 0;
        let prev_running_num_target_found = 0;
        
        const perf = require('perf_hooks').performance;
        let start_time = perf.now();
        setInterval(() => {
            let current_time = perf.now();
            let elapsed_time = (current_time - start_time) / 1000;
            start_time = current_time;

            let rate_links_to_crawl = (running_links_to_crawl - prev_running_links_to_crawl) / elapsed_time;
            let rate_crawled_links = (running_crawled_links - prev_running_crawled_links) / elapsed_time;
            let rate_num_target_found = (running_num_target_found - prev_running_num_target_found) / elapsed_time;

            let rate_links_to_crawl_per_minute = rate_links_to_crawl * 60;
            let rate_crawled_links_per_minute = rate_crawled_links * 60;
            let rate_num_target_found_per_minute = rate_num_target_found * 60;
            console.log(`\n\n--- PERFORMANCE STATS ---`);
            console.log(`Elapsed Time: ${elapsed_time.toFixed(2)} seconds`);
            console.log(`Rate of links to crawl: ${rate_links_to_crawl.toFixed(2)} links/sec (${rate_links_to_crawl_per_minute.toFixed(2)} links/min)`);
            console.log(`Rate of crawled links: ${rate_crawled_links.toFixed(2)} links/sec (${rate_crawled_links_per_minute.toFixed(2)} links/min)`);
            console.log(`Rate of target pages found: ${rate_num_target_found.toFixed(2)} pages/sec (${rate_num_target_found_per_minute.toFixed(2)} pages/min)`);
            console.log('--- END PERFORMANCE STATS ---\n\n');

            prev_running_links_to_crawl = running_links_to_crawl;
            prev_running_crawled_links = running_crawled_links;
            prev_running_num_target_found = running_num_target_found;
            console.log();
        }, 1000 * 60 * 5);

        for(let i = 0; i < 100000; i++){
            console.log("ITER =", i);
            // await sleep_iter();
            await crawl_iter();
            if(i % 20 === 0) {
                await stat_iter();
            } if(i % 5 === 0) {
                await save_iter();
            }
        }
        await save_iter();

        cb();
    };

    // for(let i = 0; i < num_nodes; i++) {
    //     await spawn_nx(nodes[i]);
    // }

    setup_cluster(() => {
        run_task(() => {
            finish();
        })
    });

    const finish = async () => {
        console.log("SHUTTING DOWN CLUSTER...");
        
        // Clean up metrics tracking
        await new Promise((resolve) => {
            const remote = { gid: 'local', service: 'crawler', method: 'cleanup' };
            distribution.taxonomy_group.comm.send([], remote, (e, metrics) => {
                if (!e) {
                    console.log("Crawler metrics:", metrics);
                    
                    // Aggregate metrics from all nodes
                    const aggregatedMetrics = {
                        crawling: {
                            totalPagesProcessed: 0,
                            avgProcessingTime: 0,
                            totalBytesDownloaded: 0
                        },
                        indexing: {
                            totalDocumentsIndexed: 0,
                            avgIndexTime: 0,
                            totalBytesTransferred: 0,
                            totalTermsExtracted: 0
                        }
                    };
                    
                    // Process node metrics
                    for (const nodeId in metrics) {
                        if (metrics[nodeId] && metrics[nodeId].crawling) {
                            const nodeCrawlMetrics = metrics[nodeId].crawling;
                            const nodeIndexMetrics = metrics[nodeId].indexing;
                            
                            aggregatedMetrics.crawling.totalPagesProcessed += nodeCrawlMetrics.pagesProcessed;
                            aggregatedMetrics.crawling.totalBytesDownloaded += nodeCrawlMetrics.bytesDownloaded;
                            aggregatedMetrics.indexing.totalDocumentsIndexed += nodeIndexMetrics.documentsIndexed;
                            aggregatedMetrics.indexing.totalBytesTransferred += nodeIndexMetrics.bytesTransferred;
                            aggregatedMetrics.indexing.totalTermsExtracted += nodeIndexMetrics.termsExtracted;
                        }
                    }
                    
                    // Calculate averages
                    if (aggregatedMetrics.crawling.totalPagesProcessed > 0) {
                        aggregatedMetrics.crawling.avgProcessingTime = 
                            Object.values(metrics).reduce((sum, m) => sum + (m.crawling?.totalCrawlTime || 0), 0) / 
                            aggregatedMetrics.crawling.totalPagesProcessed;
                    }
                    
                    if (aggregatedMetrics.indexing.totalDocumentsIndexed > 0) {
                        aggregatedMetrics.indexing.avgIndexTime = 
                            Object.values(metrics).reduce((sum, m) => sum + (m.indexing?.totalIndexTime || 0), 0) / 
                            aggregatedMetrics.indexing.totalDocumentsIndexed;
                    }
                    
                    console.log("PERFORMANCE SUMMARY:");
                    console.log("-------------------");
                    console.log(`Total pages processed: ${aggregatedMetrics.crawling.totalPagesProcessed}`);
                    console.log(`Average page processing time: ${aggregatedMetrics.crawling.avgProcessingTime.toFixed(2)}ms`);
                    console.log(`Total data downloaded: ${(aggregatedMetrics.crawling.totalBytesDownloaded/1024/1024).toFixed(2)}MB`);
                    console.log(`Total documents indexed: ${aggregatedMetrics.indexing.totalDocumentsIndexed}`);
                    console.log(`Total terms extracted: ${aggregatedMetrics.indexing.totalTermsExtracted}`);
                    console.log(`Average indexing time: ${aggregatedMetrics.indexing.avgIndexTime.toFixed(2)}ms`);
                    console.log(`Total indexing data transferred: ${(aggregatedMetrics.indexing.totalBytesTransferred/1024/1024).toFixed(2)}MB`);
                }
                resolve();
            });
        });
        
        for(let i = 0; i < num_nodes; i++) await stop_nx(nodes[i]);
        server.close();
    };
});