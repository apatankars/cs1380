const distribution = require("../config.js");
const id = distribution.util.id;
const fs = require("fs");

// Helper function to check if an object is empty
function isEmptyObject(obj) {
  return obj && typeof obj === 'object' && Object.keys(obj).length === 0;
}

// Configuration
const CONFIG = {
  // Group configuration
  TERM_GROUP: "index",             // Group for storing terms
  DOC_GROUP: "tfidf",              // Group for storing documents
  
  // Key prefixes
  INDEX_PREFIX: "term:",           // Prefix for index keys
  DOC_PREFIX: "doc:",              // Prefix for document keys
  
  MAX_RESULTS: 20,                 // Maximum number of results to return
  
  // Scoring config
  MIN_IDF: 0.001,                  // Minimum IDF value to prevent zero scores
  
  // Debug settings
  DEBUG: true,                     // Enable debug logging
};

// Set up nodes for the distributed system
const num_nodes = 4;
const nodes = [];
const nids = [];
const tfidfGroup = {};
const indexGroup = {};
const tfidfConfig = { gid: CONFIG.DOC_GROUP };
const indexConfig = { gid: CONFIG.TERM_GROUP };

for(let i = 0; i < num_nodes; i++) {
    const nodeConfig = { ip: '127.0.0.1', port: 7112 + i };
    nodes.push(nodeConfig);
    nids.push(id.getNID(nodeConfig));
    
    // Add node to both groups
    const sid = id.getSID(nodeConfig);
    tfidfGroup[sid] = nodeConfig;
    indexGroup[sid] = nodeConfig;
}

// Simple logging utility
function log(message, level = 'info') {
  if (level === 'debug' && !CONFIG.DEBUG) return;
  
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] [${level.toUpperCase()}] ${message}`);
}

// The PostingListSearch class focuses on direct term posting list retrieval
class PostingListSearch {
  constructor(config) {
    this.config = config;
    this.termGroup = config.TERM_GROUP;
    this.docGroup = config.DOC_GROUP;
    
    log(`Initializing posting list search with term group "${this.termGroup}" and doc group "${this.docGroup}"`);
    
    // Load global terms info if available
    try {
      const globalTermsFile = './tfidf-results/global-terms-index.json';
      if (fs.existsSync(globalTermsFile)) {
        this.globalTerms = JSON.parse(fs.readFileSync(globalTermsFile, 'utf8'));
        log(`Loaded global terms information with ${this.globalTerms.length} terms`);
      }
    } catch (err) {
      console.error("Error loading global terms information:", err);
      this.globalTerms = [];
    }
  }
  
  /**
   * Get a term from the distributed store
   */
  async getTerm(term) {
    const key = `${this.config.INDEX_PREFIX}${term}`;
    log(`Getting term from ${this.termGroup} store with key "${key}"`, 'debug');
    
    return new Promise((resolve, reject) => {
      distribution[this.termGroup].store.get(key, (err, value) => {
        if (err && !isEmptyObject(err)) {
          log(`Error getting term ${term}: ${JSON.stringify(err)}`, 'error');
          reject(err);
          return;
        }
        
        // Debug the structure
        if (value) {
          log(`Term structure for "${term}": ${JSON.stringify(value).substring(0, 200)}...`, 'debug');
        } else {
          log(`Term not found: ${term}`, 'debug');
        }
        
        resolve(value);
      });
    });
  }
  
  /**
   * Get a document from the distributed store
   */
  async getDocument(docId) {
    const key = `${this.config.DOC_PREFIX}${docId}`;
    log(`Getting document from ${this.docGroup} store with key "${key}"`, 'debug');
    
    return new Promise((resolve, reject) => {
      distribution[this.docGroup].store.get(key, (err, value) => {
        if (err && !isEmptyObject(err)) {
          log(`Error getting document ${docId}: ${JSON.stringify(err)}`, 'error');
          reject(err);
          return;
        }
        
        if (value) {
          log(`Document structure for "${docId}": ${JSON.stringify(value).substring(0, 200)}...`, 'debug');
        } else {
          log(`Document not found: ${docId}`, 'debug');
        }
        
        resolve(value);
      });
    });
  }
  
  /**
   * Fix IDF scores to prevent zeros
   */
  fixIdfScore(termObject) {
    if (!termObject) return termObject;
    
    // Apply minimum IDF if needed
    if (termObject.idf === 0 || termObject.idf < this.config.MIN_IDF) {
      log(`Fixing zero/low IDF for term "${termObject.term}" (original: ${termObject.idf})`, 'debug');
      termObject.idf = this.config.MIN_IDF;
      
      // Recalculate scores in postings if they're zero
      if (Array.isArray(termObject.postings)) {
        termObject.postings = termObject.postings.map(posting => {
          if (Array.isArray(posting)) {
            // If score is zero or very low, calculate a new score based on count and MIN_IDF
            if (posting[1] === 0 || posting[1] < 0.0001) {
              const count = posting[2] || 1;
              const docWords = 1000; // Assume 1000 words if we don't know
              const tf = count / docWords;
              const newScore = tf * this.config.MIN_IDF;
              return [posting[0], newScore, posting[2]];
            }
          }
          return posting;
        });
      }
    }
    
    return termObject;
  }
  
  /**
   * Get posting list for a term sorted by decreasing importance
   */
  async getPostingList(term) {
    log(`Getting posting list for term "${term}"`);
    
    try {
      let termObject = await this.getTerm(term);
      
      if (!termObject) {
        log(`Term "${term}" not found in index`);
        return [];
      }
      
      // Fix zero IDF scores
      termObject = this.fixIdfScore(termObject);
      
      log(`Retrieved term "${term}" with document frequency ${termObject.df || termObject.documentFrequency || 0}`);
      
      // Extract the posting list
      let postings = termObject.postings || [];
      
      // Check if postings is empty or in wrong format
      if (!Array.isArray(postings) || postings.length === 0) {
        log(`No postings found for term "${term}" or invalid format`);
        
        // Try alternative property names
        if (termObject.scores && Array.isArray(termObject.scores)) {
          log(`Found alternative postings in "scores" property`);
          postings = termObject.scores.map(score => {
            if (Array.isArray(score)) return score;
            return [score.docId, score.score || score.tfidf || 0, score.count || 1];
          });
        }
      }
      
      // Sort by score (descending)
      const sortedPostings = [...postings].sort((a, b) => {
        // Handle different posting list formats
        const scoreA = Array.isArray(a) ? a[1] : (a.score || a.tfidf || 0);
        const scoreB = Array.isArray(b) ? b[1] : (b.score || b.tfidf || 0);
        return scoreB - scoreA;
      });
      
      // Format the results
      return sortedPostings.map(posting => {
        if (Array.isArray(posting)) {
          return {
            docId: posting[0],
            score: posting[1],
            count: posting[2] || 1
          };
        }
        return {
          docId: posting.docId,
          score: posting.score || posting.tfidf || 0,
          count: posting.count || 1
        };
      });
    } catch (err) {
      log(`Error getting posting list for term "${term}": ${err}`, 'error');
      return [];
    }
  }
  
  /**
   * Get multiple posting lists
   */
  async getMultiplePostingLists(terms) {
    log(`Getting posting lists for terms: ${terms.join(', ')}`);
    
    const results = {};
    
    for (const term of terms) {
      results[term] = await this.getPostingList(term);
    }
    
    return results;
  }
  
  /**
   * Get most important terms in the index
   */
  async getMostImportantTerms(count = 20) {
    if (this.globalTerms && this.globalTerms.length > 0) {
      log(`Getting ${count} most important terms from global terms index`);
      
      return this.globalTerms
        .sort((a, b) => b.importance - a.importance)
        .slice(0, count)
        .map(term => ({
          term: term.term,
          importance: term.importance,
          documentFrequency: term.df
        }));
    }
    
    log(`Global terms index not available, fetching from storage`);
    
    // If no global terms index is available, try to get a sample of terms
    return [];
  }
  
  /**
   * Get document details
   */
  async getDocumentDetails(docId) {
    try {
      const doc = await this.getDocument(docId);
      
      if (!doc) {
        log(`Document ${docId} not found`);
        return null;
      }
      
      // Format the document for display
      return {
        id: doc.id || docId,
        url: doc.url || docId,
        totalWords: doc.totalWords || 0,
        terms: (doc.terms || []).map(term => {
          if (Array.isArray(term)) {
            return {
              term: term[0],
              score: term[1],
              count: term[2] || 1
            };
          }
          return term;
        }).slice(0, 10) // Just show top 10 terms
      };
    } catch (err) {
      log(`Error getting document ${docId}: ${err}`, 'error');
      return null;
    }
  }
}

// Run the interactive REPL
function startInteractiveMode(search) {
  if (!process.stdin.isTTY) {
    console.log("Interactive mode requires a TTY terminal");
    return;
  }
  
  // Make sure we're in line-by-line mode
  process.stdin.setEncoding('utf8');
  process.stdin.resume();
  
  console.log("\n===== INTERACTIVE TERM SEARCH =====");
  console.log("Enter a term to get its posting list, or 'exit' to quit");
  console.log("Enter 'top N' to get the top N most important terms (e.g., 'top 10')");
  console.log("Enter 'doc [ID]' to get details for a document (e.g., 'doc /wiki/Hakea_actites')");
  console.log("> ");
  
  // Handle user input
  process.stdin.on('data', async (input) => {
    const line = input.toString().trim();
    
    if (line.toLowerCase() === 'exit' || line.toLowerCase() === 'quit') {
      console.log("Exiting...");
      process.exit(0);
      return;
    }
    
    try {
      // Check for special commands
      if (line.toLowerCase().startsWith('top ')) {
        const count = parseInt(line.substring(4), 10);
        if (isNaN(count) || count <= 0) {
          console.log("Invalid count. Please use 'top N' where N is a positive number.");
        } else {
          const topTerms = await search.getMostImportantTerms(count);
          console.log(`\nTop ${count} most important terms:`);
          topTerms.forEach((term, index) => {
            console.log(`${index + 1}. ${term.term} (importance: ${term.importance?.toFixed(4) || 'N/A'}, doc freq: ${term.documentFrequency || 'N/A'})`);
          });
        }
      } else if (line.toLowerCase().startsWith('doc ')) {
        const docId = line.substring(4).trim();
        console.log(`\nGetting details for document "${docId}"...`);
        const docDetails = await search.getDocumentDetails(docId);
        
        if (docDetails) {
          console.log(`Document ID: ${docDetails.id}`);
          console.log(`URL: ${docDetails.url}`);
          console.log(`Total Words: ${docDetails.totalWords}`);
          console.log("Top Terms:");
          
          if (docDetails.terms && docDetails.terms.length > 0) {
            docDetails.terms.forEach((term, i) => {
              console.log(`  ${i+1}. ${term.term} (score: ${term.score.toFixed(6)}, count: ${term.count})`);
            });
          } else {
            console.log("  No terms found");
          }
        } else {
          console.log(`Document "${docId}" not found`);
        }
      } else {
        // Regular term search
        const term = line;
        console.log(`\nGetting posting list for term "${term}"...`);
        const postings = await search.getPostingList(term);
        
        console.log(`Found ${postings.length} documents containing term "${term}"`);
        
        if (postings.length > 0) {
          console.log("\nTop documents by score:");
          postings.slice(0, 10).forEach((posting, index) => {
            console.log(`${index + 1}. Document: ${posting.docId}`);
            console.log(`URL: https://en.wikipedia.org${posting.docId}`)
            console.log(`   Score: ${posting.score.toFixed(6)}`);
            console.log(`   Count: ${posting.count}`);
          });
        } else {
          console.log("No documents found containing this term.");
        }
      }
    } catch (err) {
      console.error(`Error processing input: ${err.message || err}`);
    }
    
    console.log("\n> ");
  });
}

// Example usage
async function runExamples(search) {
  // Example 1: Get posting list for a specific term
  console.log("\n===== EXAMPLE 1: POSTING LIST FOR 'biology' =====");
  const biologyPostings = await search.getPostingList('biology');
  console.log(`Found ${biologyPostings.length} documents for term 'biology'`);
  
  if (biologyPostings.length > 0) {
    console.log("Top 5 documents:");
    biologyPostings.slice(0, 5).forEach((posting, index) => {
      console.log(`${index + 1}. Document: ${posting.docId}`);
      console.log(`   Score: ${posting.score.toFixed(4)}`);
      console.log(`   Count: ${posting.count}`);
    });
    
    // Get details for the top document
    if (biologyPostings.length > 0) {
      console.log("\nDetails for top document:");
      const topDocId = biologyPostings[0].docId;
      const docDetails = await search.getDocumentDetails(topDocId);
      
      if (docDetails) {
        console.log(`Document ID: ${docDetails.id}`);
        console.log(`URL: ${docDetails.url}`);
        console.log(`Total Words: ${docDetails.totalWords}`);
        console.log("Top Terms:");
        docDetails.terms.forEach((term, i) => {
          console.log(`  ${i+1}. ${term.term} (score: ${term.score.toFixed(4)}, count: ${term.count})`);
        });
      } else {
        console.log(`Could not retrieve details for document ${topDocId}`);
      }
    }
  } else {
    console.log("No documents found for term 'biology'");
  }
  
  // Example 2: Get posting lists for multiple terms
  console.log("\n===== EXAMPLE 2: MULTIPLE POSTING LISTS =====");
  const terms = ['biology', 'evolution', 'species'];
  const multiplePostings = await search.getMultiplePostingLists(terms);
  
  for (const term of terms) {
    const postings = multiplePostings[term];
    console.log(`\nTerm: ${term} - Found ${postings.length} documents`);
    
    if (postings.length > 0) {
      console.log("Top 3 documents:");
      postings.slice(0, 3).forEach((posting, index) => {
        console.log(`${index + 1}. Document: ${posting.docId} (Score: ${posting.score.toFixed(4)})`);
      });
    } else {
      console.log("No documents found for this term");
    }
  }
  
  // Example 3: Get most important terms
  console.log("\n===== EXAMPLE 3: MOST IMPORTANT TERMS =====");
  const importantTerms = await search.getMostImportantTerms(10);
  
  if (importantTerms.length > 0) {
    console.log("Top 10 most important terms:");
    importantTerms.forEach((term, index) => {
      console.log(`${index + 1}. ${term.term} (importance: ${term.importance?.toFixed(4) || 'N/A'}, doc freq: ${term.documentFrequency || 'N/A'})`);
    });
    
    // Get posting list for the most important term
    if (importantTerms.length > 0) {
      const mostImportantTerm = importantTerms[0].term;
      console.log(`\nPosting list for most important term '${mostImportantTerm}':`);
      
      const importantTermPostings = await search.getPostingList(mostImportantTerm);
      
      if (importantTermPostings.length > 0) {
        console.log("Top 5 documents:");
        importantTermPostings.slice(0, 5).forEach((posting, index) => {
          console.log(`${index + 1}. Document: ${posting.docId} (Score: ${posting.score.toFixed(4)})`);
        });
      } else {
        console.log("No documents found for this term");
      }
    }
  } else {
    console.log("Could not retrieve important terms");
  }
}

// Main function to set up and run the search engine
distribution.node.start(async (server) => {
  console.log("SETTING UP POSTING LIST SEARCH ENGINE...");

  // Helper function to spawn a node
  const spawn_node = (node) =>
    new Promise((resolve, reject) =>
      distribution.local.status.spawn(node, (e, v) => {
        console.log(
          `Spawned node at ${node.ip}:${node.port} ${distribution.util.id.getNID(node)} with result:`,
          e ? e : v
        );
        resolve(e, v);
      })
    );

  // Helper function to stop a node
  const stop_node = (node) =>
    new Promise((resolve, reject) =>
      distribution.local.comm.send(
        [],
        { service: "status", method: "stop", node: node },
        (e, v) => resolve(e, v)
      )
    );

  // Set up cleanup handler
  const finish = async () => {
    console.log("SHUTTING DOWN...");
    for (const node of nodes) {
      await stop_node(node);
    }
    server.close();
    process.exit(0);
  };
  
  // Handle Ctrl+C gracefully
  process.on('SIGINT', function() {
    console.log("\nCaught interrupt signal (Ctrl+C)");
    finish();
  });

  // Start the nodes
  for (let i = 0; i < nodes.length; i++) {
    const node = nodes[i];
    try {
      await spawn_node(node);
      console.log(`Node started at ${node.ip}:${node.port}`);
    } catch (e) {
      console.error(`Failed to start node at ${node.ip}:${node.port}`, e);
      finish();
      return;
    }
  }

  // Set up the TFIDF group
  distribution.local.groups.put(tfidfConfig, tfidfGroup, (e, v) => {
    if (e && !isEmptyObject(e)) {
      console.error("Error setting up TFIDF group:", e);
      finish();
      return;
    }
    
    console.log("TFIDF group set up successfully");
    
    // Set up the INDEX group
    distribution.local.groups.put(indexConfig, indexGroup, (e, v) => {
      if (e && !isEmptyObject(e)) {
        console.error("Error setting up INDEX group:", e);
        finish();
        return;
      }
      
      console.log("INDEX group set up successfully, initializing search engine...");
      
      // Create the search engine instance
      const search = new PostingListSearch(CONFIG);
      
      // Run example searches, then start interactive mode
      runExamples(search)
        .then(() => {
          // Start the interactive mode in a separate function
          startInteractiveMode(search);
        })
        .catch(err => {
          console.error("Error running examples:", err);
          // Start interactive mode even if examples fail
          startInteractiveMode(search);
        });
    });
  });
});