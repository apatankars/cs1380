const distribution = require("../config.js");
const id = distribution.util.id;
const fs = require("fs");

// Helper function to check if an object is empty
function isEmptyObject(obj) {
  return obj && typeof obj === 'object' && Object.keys(obj).length === 0;
}

// Search engine configuration
const CONFIG = {
  // Group configuration
  TERM_GROUP: "index",             // Group for storing terms
  DOC_GROUP: "tfidf",              // Group for storing documents
  
  // Key prefixes
  INDEX_PREFIX: "term:",           // Prefix for index keys
  DOC_PREFIX: "doc:",              // Prefix for document keys
  
  MAX_RESULTS: 20,                 // Maximum number of results to return
  ENABLE_HIGHLIGHTING: true,       // Whether to highlight matching terms in snippets
  SNIPPET_LENGTH: 150,             // Maximum length of document snippets
  
  // Search optimization
  USE_SHARD_INFO: true,            // Whether to use shard information to optimize search
  BATCH_GET_SIZE: 10,              // Number of terms/docs to get in a single batch
  
  // Ranking factors
  TF_IDF_WEIGHT: 1.0,              // Weight for TF-IDF score
  TERM_PROXIMITY_WEIGHT: 0.2,      // Weight for term proximity
  TERM_RARITY_WEIGHT: 0.5,         // Weight for term rarity
  
  // Cache settings
  ENABLE_CACHE: true,              // Whether to enable term/document caching
  CACHE_SIZE: 1000,                // Maximum number of items to cache
  CACHE_TTL: 3600,                 // Cache TTL in seconds
  
  // Debug settings
  DEBUG: true,                     // Enable debug logging
  VERBOSE: false,                  // Enable verbose logging
};

// Set up nodes for the distributed system
const num_nodes = 3;
const nodes = [];
const nids = [];
const tfidfGroup = {};
const indexGroup = {};
const tfidfConfig = { gid: CONFIG.DOC_GROUP };
const indexConfig = { gid: CONFIG.TERM_GROUP };

for(let i = 0; i < num_nodes; i++) {
    const nodeConfig = { ip: '127.0.0.1', port: 7110 + i };
    nodes.push(nodeConfig);
    nids.push(id.getNID(nodeConfig));
    
    // Add node to both groups
    const sid = id.getSID(nodeConfig);
    tfidfGroup[sid] = nodeConfig;
    indexGroup[sid] = nodeConfig;
}

// Simple cache implementation
class Cache {
  constructor(maxSize = 1000, ttl = 3600) {
    this.maxSize = maxSize;
    this.ttl = ttl * 1000; // Convert to milliseconds
    this.cache = new Map();
  }
  
  get(key) {
    if (!this.cache.has(key)) {
      return null;
    }
    
    const entry = this.cache.get(key);
    
    // Check if the entry has expired
    if (Date.now() > entry.expiry) {
      this.cache.delete(key);
      return null;
    }
    
    // Update access timestamp for LRU eviction
    entry.lastAccessed = Date.now();
    
    return entry.value;
  }
  
  set(key, value) {
    // If cache is full, evict least recently used entry
    if (this.cache.size >= this.maxSize) {
      let oldest = null;
      let oldestKey = null;
      
      for (const [k, v] of this.cache.entries()) {
        if (!oldest || v.lastAccessed < oldest) {
          oldest = v.lastAccessed;
          oldestKey = k;
        }
      }
      
      if (oldestKey) {
        this.cache.delete(oldestKey);
      }
    }
    
    this.cache.set(key, {
      value,
      expiry: Date.now() + this.ttl,
      lastAccessed: Date.now()
    });
  }
  
  clear() {
    this.cache.clear();
  }
  
  size() {
    return this.cache.size;
  }
}

// Create caches
const termCache = new Cache(CONFIG.CACHE_SIZE, CONFIG.CACHE_TTL);
const docCache = new Cache(CONFIG.CACHE_SIZE, CONFIG.CACHE_TTL);

// Simple logging utility
function log(message, level = 'info') {
  if (level === 'debug' && !CONFIG.DEBUG) return;
  if (level === 'verbose' && !CONFIG.VERBOSE) return;
  
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] [${level.toUpperCase()}] ${message}`);
}

// The SearchEngine class handles all search operations
class SearchEngine {
  constructor(config) {
    this.config = config;
    this.termGroup = config.TERM_GROUP;
    this.docGroup = config.DOC_GROUP;
    
    log(`Initializing search engine with term group "${this.termGroup}" and doc group "${this.docGroup}"`);
    
    // Load shard info if available and enabled
    this.shardInfo = null;
    if (config.USE_SHARD_INFO) {
      try {
        const shardFile = './tfidf-results/shard-metadata.json';
        const termShardMappingFile = './tfidf-results/term-shard-mapping.json';
        
        if (fs.existsSync(shardFile) && fs.existsSync(termShardMappingFile)) {
          this.shardInfo = JSON.parse(fs.readFileSync(shardFile, 'utf8'));
          this.termShardMapping = JSON.parse(fs.readFileSync(termShardMappingFile, 'utf8'));
          log(`Loaded shard information with ${Object.keys(this.shardInfo).length} shards`);
        }
      } catch (err) {
        console.error("Error loading shard information:", err);
        this.shardInfo = null;
        this.termShardMapping = null;
      }
    }
    
    // Load global terms info if available
    try {
      const globalTermsFile = './tfidf-results/global-terms-index.json';
      if (fs.existsSync(globalTermsFile)) {
        this.globalTerms = JSON.parse(fs.readFileSync(globalTermsFile, 'utf8'));
        log(`Loaded global terms information with ${this.globalTerms.length} terms`);
        
        // Create a map for faster lookups
        this.globalTermsMap = {};
        this.globalTerms.forEach(term => {
          this.globalTermsMap[term.term] = term;
        });
      }
    } catch (err) {
      console.error("Error loading global terms information:", err);
      this.globalTerms = [];
      this.globalTermsMap = {};
    }
  }
  
  /**
   * Get a term from the distributed store
   */
  async getTerm(term) {
    // Check cache first if enabled
    if (this.config.ENABLE_CACHE) {
      const cachedTerm = termCache.get(term);
      if (cachedTerm) {
        log(`Cache hit for term: ${term}`, 'verbose');
        return cachedTerm;
      }
    }
    
    // Format the storage key
    const key = `${this.config.INDEX_PREFIX}${term}`;
    log(`Getting term from ${this.termGroup} store with key "${key}"`, 'debug');
    
    return new Promise((resolve, reject) => {
      // Access the term group directly
      distribution[this.termGroup].store.get(key, (err, value) => {
        if (err && !isEmptyObject(err)) {
          log(`Error getting term ${term}: ${JSON.stringify(err)}`, 'error');
          reject(err);
          return;
        }
        
        // Check if value is undefined or null
        if (value === undefined || value === null) {
          log(`Term not found: ${term}`, 'debug');
          resolve(null);
          return;
        }
        
        // Cache the result if enabled
        if (this.config.ENABLE_CACHE) {
          termCache.set(term, value);
        }
        
        log(`Successfully retrieved term: ${term}`, 'verbose');
        resolve(value);
      });
    });
  }
  
  /**
   * Get a document from the distributed store
   */
  async getDocument(docId) {
    // Check cache first if enabled
    if (this.config.ENABLE_CACHE) {
      const cachedDoc = docCache.get(docId);
      if (cachedDoc) {
        log(`Cache hit for document: ${docId}`, 'verbose');
        return cachedDoc;
      }
    }
    
    // Format the storage key
    const key = `${this.config.DOC_PREFIX}${docId}`;
    log(`Getting document from ${this.docGroup} store with key "${key}"`, 'debug');
    
    return new Promise((resolve, reject) => {
      // Access the document group directly
      distribution[this.docGroup].store.get(key, (err, value) => {
        if (err && !isEmptyObject(err)) {
          log(`Error getting document ${docId}: ${JSON.stringify(err)}`, 'error');
          reject(err);
          return;
        }
        
        // Check if value is undefined or null
        if (value === undefined || value === null) {
          log(`Document not found: ${docId}`, 'debug');
          resolve(null);
          return;
        }
        
        // Cache the result if enabled
        if (this.config.ENABLE_CACHE) {
          docCache.set(docId, value);
        }
        
        log(`Successfully retrieved document: ${docId}`, 'verbose');
        resolve(value);
      });
    });
  }
  
  /**
   * Get multiple terms in a batch
   */
  async getTermsBatch(terms) {
    // Filter out terms we already have in cache
    let termsToFetch = terms;
    
    if (this.config.ENABLE_CACHE) {
      termsToFetch = terms.filter(term => !termCache.get(term));
    }
    
    if (termsToFetch.length === 0) {
      // All terms are in cache
      log(`All ${terms.length} terms found in cache`, 'debug');
      return terms.map(term => termCache.get(term)).filter(term => term !== null);
    }
    
    // Split into batches to avoid overwhelming the system
    const batches = [];
    for (let i = 0; i < termsToFetch.length; i += this.config.BATCH_GET_SIZE) {
      batches.push(termsToFetch.slice(i, i + this.config.BATCH_GET_SIZE));
    }
    
    log(`Fetching ${termsToFetch.length} terms in ${batches.length} batches`, 'debug');
    
    const resultMap = {};
    
    // Process each batch sequentially to avoid overwhelming the system
    for (const batch of batches) {
      const batchPromises = batch.map(term => {
        return this.getTerm(term)
          .then(result => {
            if (result) {
              resultMap[term] = result;
            }
            return result;
          })
          .catch(err => {
            log(`Error in batch get for term ${term}: ${err}`, 'error');
            return null;
          });
      });
      
      await Promise.all(batchPromises);
      
      // Small delay between batches
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    
    // Add cached terms to result map
    if (this.config.ENABLE_CACHE) {
      for (const term of terms) {
        if (!resultMap[term]) {
          const cachedTerm = termCache.get(term);
          if (cachedTerm) {
            resultMap[term] = cachedTerm;
          }
        }
      }
    }
    
    // Create final array in original term order, filtering out missing terms
    const results = terms.map(term => resultMap[term]).filter(term => term !== undefined && term !== null);
    log(`Retrieved ${results.length} terms out of ${terms.length} requested`, 'debug');
    
    return results;
  }
  
  /**
   * Get multiple documents in a batch
   */
  async getDocumentsBatch(docIds) {
    if (!docIds || docIds.length === 0) {
      return [];
    }
    
    // Filter out docs we already have in cache
    let docsToFetch = docIds;
    
    if (this.config.ENABLE_CACHE) {
      docsToFetch = docIds.filter(docId => !docCache.get(docId));
    }
    
    if (docsToFetch.length === 0) {
      // All docs are in cache
      log(`All ${docIds.length} documents found in cache`, 'debug');
      return docIds.map(docId => docCache.get(docId)).filter(doc => doc !== null);
    }
    
    // Split into batches
    const batches = [];
    for (let i = 0; i < docsToFetch.length; i += this.config.BATCH_GET_SIZE) {
      batches.push(docsToFetch.slice(i, i + this.config.BATCH_GET_SIZE));
    }
    
    log(`Fetching ${docsToFetch.length} documents in ${batches.length} batches`, 'debug');
    
    const resultMap = {};
    
    // Process each batch sequentially
    for (const batch of batches) {
      const batchPromises = batch.map(docId => {
        return this.getDocument(docId)
          .then(result => {
            if (result) {
              resultMap[docId] = result;
            }
            return result;
          })
          .catch(err => {
            log(`Error in batch get for document ${docId}: ${err}`, 'error');
            return null;
          });
      });
      
      await Promise.all(batchPromises);
      
      // Small delay between batches
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    
    // Add cached docs to result map
    if (this.config.ENABLE_CACHE) {
      for (const docId of docIds) {
        if (!resultMap[docId]) {
          const cachedDoc = docCache.get(docId);
          if (cachedDoc) {
            resultMap[docId] = cachedDoc;
          }
        }
      }
    }
    
    // Create final array in original doc order, filtering out missing docs
    const results = docIds.map(docId => resultMap[docId]).filter(doc => doc !== undefined && doc !== null);
    log(`Retrieved ${results.length} documents out of ${docIds.length} requested`, 'debug');
    
    return results;
  }
  
  /**
   * Tokenize and normalize a query string
   */
  tokenizeQuery(query) {
    // Convert to lowercase
    query = query.toLowerCase();
    
    // Remove punctuation
    query = query.replace(/[^\w\s]/g, ' ');
    
    // Split on whitespace
    let tokens = query.split(/\s+/).filter(token => token.length > 0);
    
    // Remove common stop words
    const stopWords = new Set(["a", "an", "the", "and", "or", "but", "is", "are", "was", "were", 
                              "be", "been", "being", "in", "on", "at", "to", "for", "with", "by", 
                              "about", "against", "between", "into", "through", "during", "before", 
                              "after", "above", "below", "from", "up", "down", "of"]);
    
    tokens = tokens.filter(token => !stopWords.has(token) && token.length > 2);
    
    return tokens;
  }
  
  /**
   * Extract phrases from a query (for exact phrase matching)
   */
  extractPhrases(query) {
    const phraseRegex = /"([^"]*)"/g;
    const phrases = [];
    let match;
    
    while ((match = phraseRegex.exec(query)) !== null) {
      phrases.push(match[1].toLowerCase());
    }
    
    return phrases;
  }
  
  /**
   * Perform a search with the given query
   */
  async search(query, options = {}) {
    const startTime = Date.now();
    
    // Extract phrases for exact matching
    const phrases = this.extractPhrases(query);
    
    // Remove phrases from query for regular token processing
    phrases.forEach(phrase => {
      query = query.replace(`"${phrase}"`, ' ');
    });
    
    // Tokenize the query
    const tokens = this.tokenizeQuery(query);
    
    // Add tokens from phrases
    const phraseTokens = phrases.flatMap(phrase => this.tokenizeQuery(phrase));
    const allTokens = [...new Set([...tokens, ...phraseTokens])];
    
    if (allTokens.length === 0) {
      log("No valid tokens found in query", 'debug');
      return {
        results: [],
        timing: {
          total: Date.now() - startTime,
          tokenization: 0,
          termFetching: 0,
          scoring: 0,
          docFetching: 0
        },
        query: {
          original: query,
          tokens: allTokens,
          phrases
        }
      };
    }
    
    log(`Searching for query: "${query}" with tokens: [${allTokens.join(', ')}]`);
    if (phrases.length > 0) {
      log(`Including exact phrase matches for: [${phrases.join(', ')}]`);
    }
    
    const tokenizationTime = Date.now() - startTime;
    
    // Fetch term information for all tokens
    const termFetchStart = Date.now();
    const terms = await this.getTermsBatch(allTokens);
    const termFetchTime = Date.now() - termFetchStart;
    
    log(`Retrieved ${terms.length} terms from index (out of ${allTokens.length} requested)`, 'debug');
    
    // Map of documents to their scores
    const docScores = new Map();
    
    // Map of terms to their document lists (for phrase processing)
    const termDocs = {};
    
    // Track which documents contain which query terms
    const docTerms = new Map();
    
    const scoringStart = Date.now();
    
    // Process each term
    for (let i = 0; i < terms.length; i++) {
      const term = terms[i];
      
      if (!term || !term.term) {
        log(`Term at index ${i} is missing or malformed`, 'debug');
        continue;
      }
      
      const token = term.term;
      
      log(`Processing term: ${token} (appears in ${term.df} documents)`, 'debug');
      
      // Skip terms that appear in too many documents (likely not useful)
      if (term.df > 10000 && allTokens.length > 1) {
        log(`Skipping common term: ${token} (appears in ${term.df} documents)`, 'debug');
        continue;
      }
      
      // Get the term's postings list
      const postings = term.postings || [];
      
      // Store for phrase processing
      termDocs[token] = new Map();
      
      // Process each document in the postings list
      for (const [docId, score, count] of postings) {
        // Skip documents with very low scores
        if (score < 0.01 && allTokens.length > 1) {
          continue;
        }
        
        // Add to document score
        const currentScore = docScores.get(docId) || 0;
        let termScore = score * this.config.TF_IDF_WEIGHT;
        
        // Apply term rarity bonus for rare terms
        if (term.df < 100) {
          termScore *= (1 + this.config.TERM_RARITY_WEIGHT);
        }
        
        docScores.set(docId, currentScore + termScore);
        
        // Track which terms appear in this document
        if (!docTerms.has(docId)) {
          docTerms.set(docId, new Set());
        }
        docTerms.get(docId).add(token);
        
        // Store for phrase processing
        termDocs[token].set(docId, { score, count });
      }
    }
    
    // Process phrases for exact matching
    if (phrases.length > 0) {
      log(`Processing ${phrases.length} phrases for exact matching`, 'debug');
      
      for (const phrase of phrases) {
        const phraseTokens = this.tokenizeQuery(phrase);
        
        if (phraseTokens.length < 2) {
          continue; // Skip single-token phrases
        }
        
        // Find documents containing all phrase tokens
        const docIds = new Set();
        let firstToken = true;
        
        for (const token of phraseTokens) {
          if (!termDocs[token]) {
            // If any token is missing, no documents can match
            docIds.clear();
            break;
          }
          
          if (firstToken) {
            // Initialize with documents containing first token
            termDocs[token].forEach((_, docId) => docIds.add(docId));
            firstToken = false;
          } else {
            // Intersect with documents containing this token
            const newDocIds = new Set();
            docIds.forEach(docId => {
              if (termDocs[token].has(docId)) {
                newDocIds.add(docId);
              }
            });
            docIds.clear();
            newDocIds.forEach(docId => docIds.add(docId));
          }
        }
        
        // Apply proximity boost to documents with the exact phrase
        docIds.forEach(docId => {
          // Add a significant boost for exact phrase matches
          const currentScore = docScores.get(docId) || 0;
          docScores.set(docId, currentScore * 1.5);
        });
      }
    }
    
    // Apply multi-term coordination factor (boost docs with more query terms)
    if (allTokens.length > 1) {
      docTerms.forEach((terms, docId) => {
        const matchRatio = terms.size / allTokens.length;
        const boost = 1 + (matchRatio * 0.5); // Up to 50% boost for matching all terms
        docScores.set(docId, docScores.get(docId) * boost);
      });
    }
    
    // Convert to array and sort by score
    const scoredDocs = Array.from(docScores.entries())
                           .map(([docId, score]) => ({ docId, score }))
                           .sort((a, b) => b.score - a.score);
    
    const scoringTime = Date.now() - scoringStart;
    
    log(`Scored ${scoredDocs.length} documents`, 'debug');
    
    // Limit to maximum number of results
    const topDocs = scoredDocs.slice(0, this.config.MAX_RESULTS);
    
    // Fetch the actual documents
    const docFetchStart = Date.now();
    const docIds = topDocs.map(doc => doc.docId);
    const docs = await this.getDocumentsBatch(docIds);
    const docFetchTime = Date.now() - docFetchStart;
    
    log(`Retrieved ${docs.length} documents (out of ${docIds.length} requested)`, 'debug');
    
    // Format the results
    const results = [];
    
    for (let i = 0; i < topDocs.length; i++) {
      const scoredDoc = topDocs[i];
      const doc = docs.find(d => d && d.id === scoredDoc.docId);
      
      if (!doc) {
        // Skip documents we couldn't retrieve
        log(`Document not found for docId ${scoredDoc.docId}`, 'debug');
        continue;
      }
      
      results.push({
        docId: scoredDoc.docId,
        score: scoredDoc.score,
        docNorm: doc.docNorm,
        // Convert compact term format to readable format for output
        terms: (doc.terms || []).slice(0, 10).map(([term, score, count]) => ({
          term, score, count
        })),
        matchedTerms: Array.from(docTerms.get(scoredDoc.docId) || []),
        totalTerms: doc.terms ? doc.terms.length : 0,
        totalWords: doc.totalWords || 0
      });
    }
    
    const totalTime = Date.now() - startTime;
    
    return {
      results,
      timing: {
        total: totalTime,
        tokenization: tokenizationTime,
        termFetching: termFetchTime,
        scoring: scoringTime,
        docFetching: docFetchTime
      },
      query: {
        original: query,
        tokens: allTokens,
        phrases
      },
      stats: {
        totalMatches: docScores.size,
        returnedMatches: results.length,
        cacheSizes: {
          terms: termCache.size(),
          docs: docCache.size()
        }
      }
    };
  }
  
  /**
   * Perform a similarity search to find documents similar to the given document ID
   */
  async findSimilarDocuments(docId, options = {}) {
    const startTime = Date.now();
    
    // First, get the document
    const sourceDoc = await this.getDocument(docId);
    
    if (!sourceDoc) {
      log(`Source document not found: ${docId}`, 'debug');
      return {
        results: [],
        timing: {
          total: Date.now() - startTime
        },
        error: "Source document not found"
      };
    }
    
    // Get the document's terms
    const sourceTerms = sourceDoc.terms || [];
    
    if (sourceTerms.length === 0) {
      log(`Source document has no indexed terms: ${docId}`, 'debug');
      return {
        results: [],
        timing: {
          total: Date.now() - startTime
        },
        error: "Source document has no indexed terms"
      };
    }
    
    log(`Finding documents similar to ${docId} with ${sourceTerms.length} terms`);
    
    // Extract the top terms for querying
    const topTerms = sourceTerms
      .slice(0, 20) // Use top 20 terms
      .map(([term, score, count]) => term);
    
    // Get the term information for all terms
    const terms = await this.getTermsBatch(topTerms);
    
    log(`Retrieved ${terms.length} terms for similarity search`, 'debug');
    
    // Map of documents to their similarity scores
    const docScores = new Map();
    
    // Track which documents contain which query terms
    const docTerms = new Map();
    
    // Process each term
    for (let i = 0; i < terms.length; i++) {
      const term = terms[i];
      
      if (!term || !term.term) {
        continue;
      }
      
      const token = term.term;
      
      // Get the term's postings list
      const postings = term.postings || [];
      
      // Process each document in the postings list
      for (const [candDocId, score, count] of postings) {
        // Skip the source document itself
        if (candDocId === docId) {
          continue;
        }
        
        // Add to document score
        const currentScore = docScores.get(candDocId) || 0;
        docScores.set(candDocId, currentScore + score);
        
        // Track which terms appear in this document
        if (!docTerms.has(candDocId)) {
          docTerms.set(candDocId, new Set());
        }
        docTerms.get(candDocId).add(token);
      }
    }
    
    // Apply term coordination factor
    docTerms.forEach((terms, candDocId) => {
      const matchRatio = terms.size / topTerms.length;
      docScores.set(candDocId, docScores.get(candDocId) * matchRatio);
    });
    
    // Convert to array and sort by score
    const scoredDocs = Array.from(docScores.entries())
                           .map(([candDocId, score]) => ({ docId: candDocId, score }))
                           .sort((a, b) => b.score - a.score);
    
    log(`Found ${scoredDocs.length} similar documents`, 'debug');
    
    // Limit to maximum number of results
    const maxResults = options.maxResults || this.config.MAX_RESULTS;
    const topDocs = scoredDocs.slice(0, maxResults);
    
    // Fetch the actual documents
    const docIds = topDocs.map(doc => doc.docId);
    const docs = await this.getDocumentsBatch(docIds);
    
    log(`Retrieved ${docs.length} documents for similarity results`, 'debug');
    
    // Format the results
    const results = [];
    
    for (let i = 0; i < topDocs.length; i++) {
      const scoredDoc = topDocs[i];
      const doc = docs.find(d => d && d.id === scoredDoc.docId);
      
      if (!doc) {
        // Skip documents we couldn't retrieve
        continue;
      }
      
      results.push({
        docId: scoredDoc.docId,
        score: scoredDoc.score,
        docNorm: doc.docNorm,
        terms: (doc.terms || []).slice(0, 10).map(([term, score, count]) => ({
          term, score, count
        })),
        matchedTerms: Array.from(docTerms.get(scoredDoc.docId) || []),
        totalTerms: doc.terms ? doc.terms.length : 0,
        totalWords: doc.totalWords || 0,
        commonTerms: Array.from(docTerms.get(scoredDoc.docId) || [])
      });
    }
    
    const totalTime = Date.now() - startTime;
    
    return {
      sourceDocument: {
        docId,
        terms: sourceDoc.terms.slice(0, 10).map(([term, score, count]) => ({
          term, score, count
        })),
        totalTerms: sourceDoc.terms.length,
        totalWords: sourceDoc.totalWords
      },
      results,
      timing: {
        total: totalTime
      },
      stats: {
        totalMatches: docScores.size,
        returnedMatches: results.length
      }
    };
  }
}

// Example usage
async function runExampleSearches(searchEngine) {
  // Example 1: Simple keyword search
  console.log("\n===== EXAMPLE 1: SIMPLE KEYWORD SEARCH =====");
  
  try {
    const results1 = await searchEngine.search("biology evolution species");
    console.log(`Found ${results1.results.length} results in ${results1.timing.total}ms`);
    
    if (results1.results.length > 0) {
      console.log("Top 3 results:");
      results1.results.slice(0, 3).forEach((result, index) => {
        console.log(`${index + 1}. Document: ${result.docId}`);
        console.log(`   Score: ${result.score.toFixed(4)}`);
        console.log(`   Matched Terms: ${result.matchedTerms.join(", ")}`);
        console.log(`   Top Terms: ${result.terms.slice(0, 5).map(t => t.term).join(", ")}`);
        console.log();
      });
      
      // Example 3: Find similar documents
      if (results1.results.length > 0) {
        const sourceDocId = results1.results[0].docId;
        console.log(`\n===== EXAMPLE 3: SIMILAR DOCUMENTS TO ${sourceDocId} =====`);
        
        try {
          const similarResults = await searchEngine.findSimilarDocuments(sourceDocId);
          console.log(`Found ${similarResults.results.length} similar documents in ${similarResults.timing.total}ms`);
          
          if (similarResults.results.length > 0) {
            console.log("Top 3 similar documents:");
            similarResults.results.slice(0, 3).forEach((result, index) => {
              console.log(`${index + 1}. Document: ${result.docId}`);
              console.log(`   Similarity Score: ${result.score.toFixed(4)}`);
              console.log(`   Common Terms: ${result.commonTerms.slice(0, 5).join(", ")}`);
              console.log();
            });
          } else {
            console.log("No similar documents found.");
          }
        } catch (err) {
          console.error("Error finding similar documents:", err);
        }
      }
    } else {
      console.log("No results found for keyword search.");
    }
  } catch (err) {
    console.error("Error in keyword search:", err);
  }
  
  // Example 2: Phrase search
  console.log("\n===== EXAMPLE 2: PHRASE SEARCH =====");
  
  try {
    const results2 = await searchEngine.search('"natural selection" evolution');
    console.log(`Found ${results2.results.length} results in ${results2.timing.total}ms`);
    
    if (results2.results.length > 0) {
      console.log("Top 3 results:");
      results2.results.slice(0, 3).forEach((result, index) => {
        console.log(`${index + 1}. Document: ${result.docId}`);
        console.log(`   Score: ${result.score.toFixed(4)}`);
        console.log(`   Matched Terms: ${result.matchedTerms.join(", ")}`);
        console.log(`   Top Terms: ${result.terms.slice(0, 5).map(t => t.term).join(", ")}`);
        console.log();
      });
    } else {
      console.log("No results found for phrase search.");
    }
  } catch (err) {
    console.error("Error in phrase search:", err);
  }
}

// Main function to set up and run the search engine
distribution.node.start(async (server) => {
  console.log("SETTING UP SEARCH ENGINE...");

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
      const searchEngine = new SearchEngine(CONFIG);
      
      // Run example searches
      try {
        runExampleSearches(searchEngine).catch(err => {
          console.error("Error in example searches:", err);
        });
      } catch (err) {
        console.error("Error running example searches:", err);
      }
      
      // Keep the server running for interactive use
      console.log("\nSearch engine ready for interactive use.");
      console.log("Press Ctrl+C to exit.");
      
      // Handle interactive searches from command line
      if (process.stdin.isTTY) {
        process.stdin.setEncoding('utf8');
        console.log("\nEnter search query (or 'exit' to quit):");
        
        process.stdin.on('data', async (input) => {
          const query = input.trim();
          
          if (query.toLowerCase() === 'exit' || query.toLowerCase() === 'quit') {
            console.log("Exiting...");
            finish();
            return;
          }
          
          try {
            console.log(`\nSearching for: "${query}"`);
            const results = await searchEngine.search(query);
            
            console.log(`Found ${results.results.length} results in ${results.timing.total}ms`);
            
            if (results.results.length > 0) {
              console.log("\nTop 5 results:");
              results.results.slice(0, 5).forEach((result, index) => {
                console.log(`${index + 1}. Document: ${result.docId}`);
                console.log(`   Score: ${result.score.toFixed(4)}`);
                console.log(`   Matched Terms: ${result.matchedTerms.join(", ")}`);
                console.log();
              });
            } else {
              console.log("No matching documents found.");
            }
            
            console.log("\nEnter search query (or 'exit' to quit):");
          } catch (err) {
            console.error("Error processing search:", err);
            console.log("\nEnter search query (or 'exit' to quit):");
          }
        });
      }
    });
  });
  
  // Cleanup function
  const finish = async () => {
    console.log("SHUTTING DOWN...");
    for (const node of nodes) {
      await stop_node(node);
    }
    server.close();
    process.exit(0);
  };
});