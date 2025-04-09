// distribution/local/crawler.js
const fs = require('fs');
const path = require('path');
const parse = require('node-html-parser').parse;

// Default callback
const cb = (e, v) => {
  if (e) {
    console.error(e);
  } else {
    console.log(v);
  }
};

// Initialize metrics object
let metrics = null;
let metricsInterval = null;
let stopWordsSet = null;

/**
 * Initialize the crawler service
 */
function initialize(callback) {
  callback = callback || cb;
  
  const crawlerDir = path.join('crawler-files');
  const logsDir = path.join(crawlerDir, 'logs');
  const metricsDir = path.join(crawlerDir, 'metrics');
  
  // Create directories if they don't exist
  if (!fs.existsSync(crawlerDir)) fs.mkdirSync(crawlerDir, { recursive: true });
  if (!fs.existsSync(logsDir)) fs.mkdirSync(logsDir, { recursive: true });
  if (!fs.existsSync(metricsDir)) fs.mkdirSync(metricsDir, { recursive: true });
  
  const log_file_path = path.join(logsDir, `log-${global.nodeConfig.port}.txt`);
  const metrics_file_path = path.join(metricsDir, `metrics-${global.nodeConfig.port}.json`);
  
  // Initialize metrics
  metrics = {
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
  
  // Initialize stop words
  stopWordsSet = getStopWordsSet();
  
  // Save metrics periodically
  metricsInterval = setInterval(() => {
    // Add memory metrics
    const memUsage = process.memoryUsage();
    metrics.memory.peaks.push({
      timestamp: Date.now(),
      heapUsed: Math.round(memUsage.heapUsed / 1024 / 1024),
      heapTotal: Math.round(memUsage.heapTotal / 1024 / 1024)
    });
    
    fs.writeFileSync(metrics_file_path, JSON.stringify(metrics, null, 2));
  }, 60000); // Every minute
  
  const links_to_crawl_map = new Map();
  const crawled_links_map = new Map();

  global.distribution.local.mem.put(links_to_crawl_map, 'links_to_crawl_map', (e, v) => {
    global.distribution.local.mem.put(crawled_links_map, 'crawled_links_map', (e, v) => {
      global.distribution.local.store.get('links_to_crawl', (e1, v1) => {
        global.distribution.local.store.get('crawled_links', (e2, v2) => {
          if(!e1 && !e2 && v1 && v2) {
            const saved_links_to_crawl = v1.split('\n').filter(s => s.length > 0);
            const saved_crawled_links = v2.split('\n').filter(s => s.length > 0);
            saved_links_to_crawl.map(link => links_to_crawl_map.set(link, true));
            saved_crawled_links.map(link => crawled_links_map.set(link, true));
          }

          callback(null, {
            status: 'success',
            message: 'Crawler service initialized',
            links_to_crawl: links_to_crawl_map.size,
            crawled_links: crawled_links_map.size
          });
        });
      });
    });
  });
}

/**
 * Add a link to the crawl queue
 */
function add_link_to_crawl(link, callback) {
  callback = callback || cb;
  
  global.distribution.local.mem.get('links_to_crawl_map', (e1, links_to_crawl_map) => {
    global.distribution.local.mem.get('crawled_links_map', (e2, crawled_links_map) => {
      if(links_to_crawl_map.has(link)) return callback(null, { status: 'skipped', reason: 'already_in_queue' });
      if(crawled_links_map.has(link)) return callback(null, { status: 'skipped', reason: 'already_crawled' });

      links_to_crawl_map.set(link, true);
      callback(null, { status: 'success', message: 'Link added to crawl queue', link: link });
    });
  });
}

/**
 * Get a set of stop words to filter out common terms
 */
function getStopWordsSet() {
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
    "should", "now",
    
    // Wiki-specific terms
    "wiki", "wikipedia", "edit", "article", "page", "reference", "https", "http",
    "cite", "retrieved", "ref", "jpg", "png", "svg", "commons", "category", "categories"
  ]);
}

/**
 * Crawl and process a single page from the queue
 */
function crawl_one(callback) {
  callback = callback || cb;
  // Record start time for metrics
  const crawlStartTime = Date.now();
  
  global.distribution.local.mem.get('links_to_crawl_map', (e1, links_to_crawl_map) => {
    global.distribution.local.mem.get('crawled_links_map', (e2, crawled_links_map) => {                        
      // get link to crawl
      if(links_to_crawl_map.size === 0) return callback(null, { status: 'skipped', reason: 'no_links' });
      const [url, _] = links_to_crawl_map.entries().next().value;
      links_to_crawl_map.delete(url);
      if(crawled_links_map.has(url)) return callback(null, { status: 'skipped', reason: 'already_crawled' });

      // crawl it!
      fetch(`https://en.wikipedia.org${url}`)
        .then((response) => {
          const contentLength = response.headers.get('content-length') || 0;
          // Update metrics
          metrics.crawling.bytesDownloaded += parseInt(contentLength);
          return response.text();
        })
        .then((html) => {
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
          
          let result = {
            status: 'success',
            url: url,
            is_target_class: !!is_target_class,
            links_found: links_on_page.length
          };
          
          if(is_target_class && binomial_name) {
            // Pre-process the page text - moved from indexer to crawler
            const page_text = root.text;
            const alphaOnlyPattern = /^[a-z]+$/;
            
            // Extract and pre-process words
            const all_words = (page_text.match(/\b\w+\b/g) || [])
              .map(word => word.toLocaleLowerCase())
              .filter(word => word.length > 2) // Filter out very short words
              .filter(word => alphaOnlyPattern.test(word)) // Only alphabetic words
              .filter(word => !stopWordsSet.has(word)); // Filter out stop words
            
            // // Count word occurrences
            // const wordCounts = new Map();
            // for (const word of all_words) {
            //   wordCounts.set(word, (wordCounts.get(word) || 0) + 1);
            // }
            
            // Convert to the format expected by the indexer
            const species_data = {
              hierarchy: hierarchy,
              binomial_name: binomial_name,
              url: url,
              // Send wordCounts instead of all words to reduce transfer size
              word_counts: Object.fromEntries(wordCounts),
              total_words: all_words.length,
            //   article_words: all_words, // Commented out for now 
              // Keep a small sample of unique words for debugging
              word_sample: Array.from(wordCounts.keys()).slice(0, 10)
            };
            
            // Store the original document
            const path_safe_url = url.replace(/\//g, '.');
            const uncompressed_data = JSON.stringify(species_data);
            
            // Calculate data size for metrics
            const dataSize = Buffer.byteLength(uncompressed_data, 'utf8');
            metrics.indexing.bytesTransferred += dataSize;
            metrics.indexing.termsExtracted += wordCounts.size;
            
            global.distribution.local.store.put(species_data, path_safe_url, (e, v) => {
              // After saving, send to the indexer
              const indexStartTime = Date.now();
              
              // Check if index group exists and send to indexer
              global.distribution.local.groups.get('index', (err, indexGroup) => {
                if (!err && indexGroup) {
                  // Index group exists, try to send to indexer
                  if (global.distribution.index && global.distribution.index.indexer) {
                    global.distribution.index.indexer.index(species_data, (err, indexResult) => {
                      const indexEndTime = Date.now();
                      const indexDuration = indexEndTime - indexStartTime;
                      
                      // Update indexing metrics
                      metrics.indexing.documentsIndexed++;
                      metrics.indexing.totalIndexTime += indexDuration;
                      metrics.indexing.avgIndexTime = 
                        metrics.indexing.totalIndexTime / metrics.indexing.documentsIndexed;
                      
                      if (err) {
                        console.error(`Error indexing document ${url}:`, err);
                        result.indexing = { status: 'error', error: err.message };
                      } else {
                        console.log(`Successfully indexed ${url} in ${indexDuration}ms`);
                        result.indexing = { 
                          status: 'success', 
                          duration_ms: indexDuration,
                          metrics: indexResult?.metrics || {}
                        };
                      }
                      
                      processCrawlResult(url, links_on_page, result, crawlStartTime, is_target_class, callback);
                    });
                  } else {
                    // Indexer service not available
                    result.indexing = { status: 'skipped', reason: 'indexer_not_available' };
                    processCrawlResult(url, links_on_page, result, crawlStartTime, is_target_class, callback);
                  }
                } else {
                  // Index group doesn't exist
                  result.indexing = { status: 'skipped', reason: 'index_group_not_available' };
                  processCrawlResult(url, links_on_page, result, crawlStartTime, is_target_class, callback);
                }
              });
            });
          } else {
            // Not a target class or no binomial name
            result.indexing = { status: 'skipped', reason: 'not_target_or_no_binomial' };
            processCrawlResult(url, links_on_page, result, crawlStartTime, is_target_class, callback);
          }
        })
        .catch((error) => {
          // Handle fetch errors
          console.error(`Error fetching ${url}:`, error);
          crawled_links_map.set(url, true);
          
          const crawlEndTime = Date.now();
          const crawlDuration = crawlEndTime - crawlStartTime;
          metrics.crawling.pagesProcessed++;
          metrics.crawling.totalCrawlTime += crawlDuration;
          
          callback(null, {
            status: 'error',
            url: url,
            error: error.message,
            duration_ms: crawlDuration
          });
        });
    });
  });
}

/**
 * Helper function to process crawl results and add links to the queue
 */
function processCrawlResult(url, links_on_page, result, crawlStartTime, is_target_class, callback) {
  global.distribution.local.mem.get('crawled_links_map', (e, crawled_links_map) => {
    crawled_links_map.set(url, true);

    global.distribution.taxonomy.mem.get('global_info', (e, v) => {
      if (e) {
        console.error('Error getting global info:', e);
        return callback(null, { 
          status: 'error', 
          error: 'Failed to get global_info',
          url: url 
        });
      }

      const { nodes, num_nodes } = v;

      const get_nx = (link) => nodes[parseInt(global.distribution.util.id.getID(link).slice(0, 8), 16) % num_nodes];
      const new_links = [...new Set(is_target_class ? links_on_page : [])];
      
      // Track progress
      let processed = 0;
      const total = new_links.length;
      
      if (total === 0) {
        // No links to process, finish immediately
        finishCrawl();
        return;
      }
      
      // Process each link
      new_links.forEach(link => {
        const remote = { node: get_nx(link), service: 'crawler', method: 'add_link_to_crawl'};
        global.distribution.local.comm.send([link], remote, (e, v) => {
          processed++;
          if (processed === total) {
            finishCrawl();
          }
        });
      });
      
      function finishCrawl() {
        // Update crawling metrics
        const crawlEndTime = Date.now();
        const crawlDuration = crawlEndTime - crawlStartTime;
        metrics.crawling.pagesProcessed++;
        metrics.crawling.totalCrawlTime += crawlDuration;
        metrics.crawling.avgProcessingTime = 
          metrics.crawling.totalCrawlTime / metrics.crawling.pagesProcessed;
        
        result.duration_ms = crawlDuration;
        result.new_links_added = total;
        
        callback(null, result);
      }
    });
  });
}

/**
 * Get statistics about the crawler
 */
function get_stats(callback) {
  callback = callback || cb;
  
  global.distribution.local.mem.get('links_to_crawl_map', (e1, links_to_crawl_map) => {
    global.distribution.local.mem.get('crawled_links_map', (e2, crawled_links_map) => {
      const fs = require('fs');
      let num_target_found = 0;

      try {
        // Get the node ID
        const nodeConfig = global.nodeConfig;
        const nodeID = global.distribution.util.id.getNID(nodeConfig);
        
        // Use path.join to navigate to the store directory
        const store_path = path.join('store', nodeID);
        
        if (fs.existsSync(store_path)) {
          const folders = fs.readdirSync(store_path).filter(folder => !folder.includes('.'));
          const counts = folders.map(folder => {
            const subfolder = path.join(store_path, folder);
            if (fs.existsSync(subfolder) && fs.statSync(subfolder).isDirectory()) {
              return fs.readdirSync(subfolder).length;
            }
            return 0;
          });
          num_target_found = counts.reduce((a, b) => a + b, 0) - 2; // Subtract 2 for .links_to_crawl and .crawled_links
        }
      } catch (err) {
        console.error("Error while counting targets:", err);
        num_target_found = 0;
      }

      const stats = {
        links_to_crawl: links_to_crawl_map.size,
        crawled_links: crawled_links_map.size,
        num_target_found: num_target_found,
        metrics: metrics
      };

      callback(null, stats);
    });
  });
}

/**
 * Save crawler data to disk
 */
function save_maps_to_disk(callback) {
  callback = callback || cb;
  
  global.distribution.local.mem.get('links_to_crawl_map', (e1, links_to_crawl_map) => {
    global.distribution.local.mem.get('crawled_links_map', (e2, crawled_links_map) => {
      const links_to_crawl_data = Array.from(links_to_crawl_map.keys()).join('\n');
      const crawled_links_data = Array.from(crawled_links_map.keys()).join('\n');

      global.distribution.local.store.put(links_to_crawl_data, 'links_to_crawl', (e, v) => {
        global.distribution.local.store.put(crawled_links_data, 'crawled_links', (e, v) => {
          callback(null, {
            status: 'success',
            links_to_crawl_saved: links_to_crawl_map.size,
            crawled_links_saved: crawled_links_map.size
          });
        });
      });
    });
  });
}

/**
 * Clean up resources
 */
function cleanup(callback) {
  callback = callback || cb;
  
  if (metricsInterval) {
    clearInterval(metricsInterval);
    metricsInterval = null;
  }
  
  // Save final metrics
  const metrics_file_path = path.join('crawler-files', 'metrics', `metrics-${global.nodeConfig.port}.json`);
  
  if (metrics) {
    metrics.endTime = Date.now();
    metrics.totalRuntime = (metrics.endTime - metrics.startTime) / 1000;
    
    fs.writeFileSync(metrics_file_path, JSON.stringify(metrics, null, 2));
  }
  
  // Save maps to disk
  save_maps_to_disk((err, result) => {
    callback(null, { 
      status: 'success', 
      metrics: metrics,
      saved_data: result
    });
  });
}

module.exports = {
  initialize,
  add_link_to_crawl,
  crawl_one,
  get_stats,
  save_maps_to_disk,
  cleanup
};