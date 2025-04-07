const distribution = require("./config.js");
const id = distribution.util.id;
const fs = require("fs");

// Set up a single node for testing
const node = { ip: "127.0.0.1", port: 7110 };
const num_nodes = 3;
const nids = [];
const nodes = [];
const testGroup = {};
const testConfig = { gid: "tfidf" };
// testGroup[distribution.util.id.getSID(node)] = node;
for(let i = 0; i < num_nodes; i++) {
    nodes.push({ ip: '127.0.0.1', port: 7110 + i });
    nids.push(id.getNID(nodes[i]));
    testGroup[id.getSID(nodes[i])] = nodes[i];
}

// Main function to run the TF-IDF calculation
distribution.node.start(async (server) => {
  console.log("SETTING UP TF-IDF TEST NODE...");

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

  // Start the node
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

  // Set up the group
  distribution.local.groups.put(testConfig, testGroup, (e, v) => {
    if (e) {
      console.error("Error setting up group:", e);
      finish();
      return;
    }

    

    distribution.tfidf.groups.put(testConfig, testGroup, (e, v) => {

    console.log("Group set up successfully, starting TF-IDF calculation...");

    // global.LZString = LZ;

    // Define the mapper function
    // This processes each document and emits word -> [doc, count] pairs
    const mapper = function (key, value) {
      try {
        // Expanded stop list combining standard English stop words (inspired by NLTK)
        // with additional HTML/wiki-specific tokens that are common in Wikipedia pages.
        const stopWords = new Set([
          // Common English stop words (keep your original list)
          "i", "me", "my", "myself", "we", "our", "ours", "ourselves",
          "you", "your", "yours", "yourself", "yourselves",
          "he", "him", "his", "himself", "she", "her", "hers", "herself",
          "it", "its", "itself", "they", "them", "their", "theirs", "themselves",
          "what", "which", "who", "whom", "this", "that", "these", "those",
          "am", "is", "are", "was", "were", "be", "been", "being",
          "have", "has", "had", "having", "do", "does", "did", "doing",
          "a", "an", "the", "and", "but", "if", "or", "because", "as",
          "until", "while", "of", "at", "by", "for", "with", "about",
          "against", "between", "into", "through", "during", "before",
          "after", "above", "below", "to", "from", "up", "down", "in",
          "out", "on", "off", "over", "under", "again", "further", "then",
          "once", "here", "there", "when", "where", "why", "how", "all",
          "any", "both", "each", "few", "more", "most", "other", "some",
          "such", "no", "nor", "not", "only", "own", "same", "so", "than",
          "too", "very", "s", "t", "can", "will", "just", "don", "should", "now",
          
          // Wiki structure and formatting elements
          "hlist", "ext", "child", "color", "vector", "skin", "margin", "lock", "background", 
          "font", "ready", "asbox", "first", "type", "navbar", "list", "false", "true", "last", 
          "none", "size", "feature", "limit", "left", "wikimedia", "display", "width", "padding", 
          "right", "top", "text", "value", "theme", "styles", "media", "border", "inherit", 
          "inline", "user", "main", "align", "lower", "group", "search", "important", "biota", 
          "center", "var", "transparent", "https", "img", "timeless", "function", "cookie", 
          "classname", "screen", "table", "articles", "minerva", "limited", "night", "commons",
          "sticky", "side", "even", "total", "inner", "hatnote", "bold", "italic", "wrap", "clear",
          "format", "selflink", "page", "wrap", "order", "visible", "float", "solid", "wide",
          "overflow", "hide", "empty", "relative", "auto", "depth", "full", "collapse", "split",
          "cursor", "hidden",
          
          // Wiki metadata terms
          "stub", "abbr", "name", "columns", "upload", "page", "svg", "title", "subgroup", 
          "error", "upper", "abovebelow", "tools", "hide", "counter", "enabled", "pinned", 
          "rlq", "sidebar", "listitem", "description", "weight", "repeat", "free",
          "alpha", "roman", "height", "ambox", "box", "move", "skins", "solid", "www",
          "short", "maint", "retrieved", "registration", "column", "appearance", "word", "site",
          "category", "stubshidden", "commons", "boxtext", "taxonbar", "taxonbars", "gallery",
          "note", "article", "one", "two", "three", "edit", "cite", "portal", "help", "special",
          "talk", "user", "module", "template", "tnc", "form", "ids", "see", "sources", "caps",
          "version", "long", "move", "vte", "open", "contents", "small", "png", "jpg", "add",
          "item", "items",
          
          // Time and date terms
          "utc", "november", "june", "july", "august", "september", "october", "december", 
          "january", "february", "march", "april", "day", "dates", "created", "retrieved",
          "archived",
          
          // Geographic and spatial terms (when not specifically relevant to taxonomy)
          "north", "south", "east", "west", "central", "northern", "southern", "eastern", "western",
          "upper", "lower", "nearby", "along", "near", "region", "regions", "county", "state",
          "states", "america", "australia", "california", "europe", "africa", "china", "asia",
          "islands", "island", "mexico", "brazil", "chile", "india", "borneo", "queensland",
          "nevada", "arizona", "ecuador",
          
          // References and citation terms
          "doi", "isbn", "pmid", "citation", "cite", "reference", "references", "info", "sources",
          "source", "statement", "footnote", "note", "notes", "publisher", "published", "press",
          "journal", "vol", "department", "university", "link", "links", "external", "pdf",
          "attribution", "online", "license", "creative", "sharealike", "information", "data",
          "checklist", "ipni", "database", "foundation", "royal", "society", "national", "park",
          "web", "api", "itis", "gbif", "tropicos", "kew", "eol", "natureserve", "grin", "eppo",
          "microbank", "powo", "botanic", "gardens", "speciesfungorum", "indexfungorum", "nzor",
          "mycobank", "wayback", "worms", "florabase", "calflora", "rhs", "apni", "urn", "lsid",
          "forest", "field", "guide", "encyclopedia", "col", "biodiversity", "production",
          "government", "org",
          
          // Wiki technical and UI terms
          "document", "categories", "replace", "window", "options", "centralnotice", "donate",
          "interlanguage", "globalcssjs", "desktoparticletarget", "gadget", "centralauth",
          "centralautologin", "eventlogging", "create", "log", "policy", "unstrip", "tree",
          "history", "kern", "greek", "float", "http", "available", "terms",
          "foundation", "account", "actions", "mobile", "mediawiki", "wikibase", "push",
          "changesupload", "projects", "privacy", "entity", "nowrap", "eqiad", "visualeditor",
          "init", "break", "subscription", "dark", "alt", "white", "prefers", "scheme",
          "limitreport", "pref", "may", "uls", "language", "print", "image", "base", "brackets",
          "normal", "decoration", "spacing", "mini", "position", "different", "red", "responsive",
          "plainlist", "view", "toggle", "inside", "icon", "logo", "avoid", "taxobox",
          "pages", "inc", "set", "readeditview", "taxonomy", "organization", "disabled", "client",
          "menu", "line", "code", "bottom", "context", "scribunto", "platform", "export",
          "contribute", "apply", "login", "logged", "model", "statistics", "personal", "bootstrap",
          "mmv", "mode", "start", "quick", "core", "editors", "learn", "general", "author",
          "privacy", "policy", "enabled", "disabled", "using", "make", "makecollapsible",
          
          // Measurement and quantity terms
          "cm", "mm", "inches", "meters", "metres", "high", "tall", "wide", "diameter", "maximum",
          "minimum", "min", "max", "limit", "length", "centimeters", "frac", "specific", "well",
          "static", "typically", "commonly", "sometimes", "often", "without", "secure",
          
          // Status and classification terms
          "concern", "secure",
          "iucn", "rlts", "least", "basionyms", "synonyms",
          "information", "delink", "original", "naming", 
          "described", "added", "expanded", "unsourced", "common", "like",
          
          // MediaWiki system variables and technical programming terms
          "wg", "wgtitle", "wgrevisionid", "wgisredirect", "wgpageviewlanguage", "wgbackendresponsetime", 
          "wghostname", "wgcurrevisionid", "wgarticleid", "wgisarticle", "wgusergroups", "wgcategories", 
          "wgpagecontentlanguage", "wgpagecontentmodel", "wgrelevantpagename", "wgisprobablyeditable", 
          "wgnoticeproject", "wgcitereferencepreviewsactive", "wgpopupsflags", "wgvisualeditor", 
          "wgmfdisplaywikibasedescriptions", "wgwmepagelength", "wgeditsubmitbuttonlabelpublish", 
          "wgulsiscompactlinksenabled", "wgwikibaseitemid", "wgcheckuserclienthintsheadersjsapi", 
          "wggelevelingupenabledforuser", "wgmediaviewerenabledbydefault", "wgwmeschemaeditattemptstepoversample", 
          "wgulsposition", "wgulsislanguageselectorempty", "wgbreakframes", "wgdigittransformtable", 
          "wgrequestid", "wgcanonicalnamespace", "wgaction", "wgusername", "wgrelevantarticleid", 
          "wgrelevantpageisprobablyeditable", "wgrestrictionedit", "wgrestrictionmove", "wgflaggedrevsparams", 
          "wgmediavieweronclick", "wgpageparsereport", "null", "function", "return", "foreach", 
          "documentelement", "target", "tags", "brands", "platformversion", "rlstate", "wikimediamessages", 
          "startup", "jquery", "popup", "increment", "developers", "ppvisitednodes", "templateargumentsize",
          "entityaccesscount", "cachereport", "timestamp", "transientcontent", "wmf", "custom",
          "match", "enwikimwclientpreferences", "regexp", "wgseparatortransformtable", "wgdefaultdateformat",
          "wgmonthnames", "wgcanonicalspecialpagename", "wgnamespacenumber", "wgpagename", "walltime",
          "expensivefunctioncount", "timeusage", "ttl", "pagelanguagecode", "pagevariantfallbacks",
          "watchlist", "tagline", "architecture", "bitness", "fullversionlist", "loading", "codex",
          "icons", "noscript", "rlpagemodules", "geoip", "toolbar", "popups", "targetloader",
          "echo", "wikimediaevents", "navigationtiming", "checkuser", "clienthints", "suggestededitsession",
          "loader", "impl", "tokens", "jump", "eventsrandom", "csrftoken", "navigation", "filepermanent", 
          "pageget", "urldownload", "download", "pdfprintable", "php", "descriptionshort", "trademark", 
          "topic", "schema", "mainentity", "imageobject", "datepublished", "cputime", "postexpandincludesize", 
          "expansiondepth", "timingprofile", "memusage", "sameas", "contributors", "campaigs", "hor", 
          "googpub", "datemodified", "headline", "config", "output", "parser", "navbox", "reflist", "content",
          "html", "infobox", "url", "wfo", "header", "toc", "named", "distribution", "names", "new", "subsp",
          "absolute", "world", "block", "authomatically", "also", "found", "known", "sistersitebox", "multiple",
          "ncbi", "src", "use", "shaped", "irmng", "non", "contact", "decimal", "images", "quicksurveys", "require",
          "doctype", "rlconf", "gehomepagesuggestededitsenabletopics", "wggetopicsmatchmodeenabled", "index", "wikidataarticles",
          "commonswikispecieswikidata", "john", "speciesbox", "ccf", "taxonrow", "contains", "meaning", "several", "slightly",
          "part", "less", "far", "gas", "foc", "study", "great", "five", "year", "made", "den", "row", "wginternalredirecttargeturl",
          "jstor", "wgredirectedfrom", "wggetopicsmatchmodeenabled", "wginternalredirecttargeturl", "wikidatataxonbars", "wikidatause",
          "wikidataarticles", "clientpref", "body", "fdfdfd", "space", "sizing", "pagelanguagedir", "referencetooltips", "switcher",
          "urlshortener", "growthexperiments", "enhancements", "clientpref", "additional", "smaller", "twinaray", "items","matches", "growing",
          "microformatscommons", "fna", "subtle", "subject", "occurs", "usually", "interactive", "many", "action", "nbn", "redirect", "apdb", "sdcat",
          "eds", "biolib", "photo", "gcc", "svenskawinaray", "sub", "ais", "around", "single", "issn", "due", "time", "identifiershakea", "include",
          "codfw", "refers", "jepson", "early", "throughout", "section", "either", "especially", "occasionally", "however", "listed", "expected", "bot", 
          "photos", "ending", "towards", "recognized", "cap", "nom", "elt", "clip", "service", "tro", "aaa", "flex", "formally", "occurs", "fna",
          
          // Wiki site references
          "wikimedia", "wikipedia", "wiki", "wikidata", "wiktionary", "wikisource", "wikispecies",
          "commons", "commonswikidata", "wikispecieswikidata", "cebuano", "cebuanosvenskati",
          
          // Language and internationalization
          "english", "latin", "french", "german", "languages", "spanish", "portuguese", "italian",
          "russian", "japanese", "chinese", "korean", "greek", "ltr", "dmy",
          
          // Additional relevant wiki related terms from the JSON
          "reset", "ddf", "odd", "useformat", "desktop", "oldid", "edited", "agree", "registered",
          "profit", "disclaimers", "conduct", "patroltoken", "watchtoken", "pagecontentscurrent", 
          "linkpage", "informationcite", "shortened", "articleabout", "wikipediacontact", "helplearn", 
          "editcommunity", "portalrecent", "filespecial", "contributionstalk", "articletalk", 
          "hererelated", "microformats", "microformatstaxonomy", "microformatsall", "microformatstaxonbars",
          "pagetype", "quotes", "rgba", "keyword", "wikitext", "interface", 
          
          // Format identifiers and general descriptors that don't add taxonomic value
          "bold", "italic", "size", "large", "small", "contain", "containing", "related", "similar",
          "used", "uses", "pink", "purple", "green", "blue", "yellow", "orange", "red", "brown",
          "black", "white", "gray", "grey", "smooth", "id", "style", "div", "span"
        ]);

        // Pre-compile the regex pattern for better performance
        const alphaOnlyPattern = /^[a-z]+$/;

        const docData = value;
        const docId = docData.url;
        const words = docData.article_words || [];

        // Use a Map for word counts - slightly more efficient than object literals
        const wordCounts = new Map();
        const totalWords = words.length;

        // Process all words in a single pass
        for (const word of words) {
          // Skip words with 2 or fewer characters
          if (word.length <= 2) continue;

          // Normalize to lowercase for consistency
          const cleanWord = word.toLowerCase();

          // Skip if the word is in the stop list - O(1) lookup with Set
          if (stopWords.has(cleanWord)) continue;

          // Filter out words containing any non-alphabetic characters
          if (!alphaOnlyPattern.test(cleanWord)) continue;

          // Count the word occurrence
          wordCounts.set(cleanWord, (wordCounts.get(cleanWord) || 0) + 1);
        }

        // Emit each word with document ID, count, and total number of words processed
        // Use Array.from for better performance on large maps
        return Array.from(wordCounts, ([word, count]) => {
          // console.log(`Mapper emitting word: ${word}, count: ${count}, docId: ${docId}, totalWords: ${totalWords}`);
          return { [word]: { docId, count, totalWords } };
        });
      } catch (err) {
        console.error(`Error in mapper for ${key}:`, err);
        return [];
      }
    };

    // Define the reducer function
    // This calculates TF-IDF for each word across all documents
    const reducer = function (word, values) {
      // console.log(
      //   `Reducer processing word: ${word}, with ${
      //     values.length
      //   } values : ${JSON.stringify(values).substring(0, 100)}...`
      // );
      try {
        // Total number of documents
        const totalDocs = values.length; // Use the actual number of documents in your dataset, or set it dynamically

        // Calculate term frequency for each document
        const docScores = values.map((value) => {
          const { docId, count, totalWords } = value;
          // console.log(
          //   `Calculating TF for docId: ${docId}, count: ${count}, totalWords: ${totalWords}`
          // );
          // TF = (Number of times term t appears in document) / (Total number of terms in document)
          const tf = count / totalWords;
          return { docId, tf, count };
        });

        // Calculate inverse document frequency

        // Calculate TF-IDF for each document
        const tfScores = docScores.map((doc) => {
          return {
            docId: doc.docId,
            tf: doc.tf,
            count: doc.count,
          };
        });

        // Return word with its TF-IDF scores across documents
        // console.log(
        //   `Reducer returning word: ${word}, with TF-IDF scores: ${JSON.stringify(
        //     tfidfScores
        //   )}`
        // );
        return {
          word: word,
          documentFrequency: totalDocs,
          scores: tfScores,
        };
      } catch (err) {
        console.error(`Error in reducer for ${word}:`, err);
        return { word: word, error: err.message };
      }
    };

  
    
    distribution.tfidf.store.get({key: null}, (err, keys) => {

      // Set total documents count as a global variable for use in reducer
      global.totalDocuments = keys.length;
      console.log(`Processing ${global.totalDocuments} documents...`);

      if (global.totalDocuments === 0) {
        console.log("No documents found to process!");
      }

      // Configure and run the MapReduce job
      const mrConfig = {
        map: mapper,
        reduce: reducer,
        keys: keys,
        enable_checkpoints: true, // Enable checkpoints
        checkpoint_interval: 3,   // Save every 3 batches
        batch_size: 10            // Match your existing batch size
        // checkPointID: "ae39b11c"
      };

      //   console.log(`MapReduce job configuration: ${JSON.stringify(mrConfig)}`);

      // Execute the MapReduce job
      distribution.tfidf.mr.exec(mrConfig, (err, rawResults) => {
        if (err) {
          console.error("Error executing MapReduce job:", err);
          finish();
          return;
        }

        // console.log(`MapReduce job completed. Found ${rawResults.length} results before aggregation...`);

        console.log(`MapReduce job completed. Found ${rawResults.length} results before aggregation...`);

        const resultsDir = "./tfidf-results"; // Directory to save results

        // IMPROVED AGGREGATION FUNCTION
        /**
         * Process MapReduce results in batches to avoid memory issues
         * @param {Array} rawResults - Results from the MapReduce job
         * @param {Object} options - Configuration options
         * @returns {Array} - Final aggregated results
         */
        function batchedAggregateResults(rawResults, options = {}) {
          console.log(`Starting batched aggregation of ${rawResults.length} results...`);
          
          // Configuration with defaults
          const config = {
            batchSize: 5000,                      // Number of results to process per batch
            tempDir: "./tfidf-results/temp",      // Directory for temporary files
            totalDocuments: 54825,                // Total document count
            maxMemoryPercent: 80,                 // Maximum memory usage percent before forcing GC
            reportInterval: 5000,                 // How often to log progress (items processed)
            ...options
          };
          
          // Create temp directory if it doesn't exist
          if (!fs.existsSync(config.tempDir)) {
            fs.mkdirSync(config.tempDir, { recursive: true });
          }
          
          // Statistics tracking
          const stats = {
            batchesProcessed: 0,
            totalItemsProcessed: 0,
            uniqueTermsFound: 0,
            startTime: Date.now(),
            tempFilesCreated: 0
          };
          
          // Global map to store term metadata across batches
          // We keep this minimal to reduce memory footprint
          const globalTermsMetadata = new Map();
          
          // Calculate the number of batches
          const totalBatches = Math.ceil(rawResults.length / config.batchSize);
          console.log(`Will process data in ${totalBatches} batches of ${config.batchSize} items each`);
          
          // Process each batch of results
          for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
            const batchStartTime = Date.now();
            const startIdx = batchIndex * config.batchSize;
            const endIdx = Math.min(startIdx + config.batchSize, rawResults.length);
            const batchItems = rawResults.slice(startIdx, endIdx);
            
            console.log(`Processing batch ${batchIndex+1}/${totalBatches} with ${batchItems.length} items`);
            
            // Map to store terms data for this batch
            const batchTermsMap = new Map();
            
            // First pass: Collect and combine data for this batch
            for (const result of batchItems) {
              stats.totalItemsProcessed++;
              
              // Skip invalid results
              if (!result || !result.word) continue;
              
              // Get or create entry for this word in the batch map
              if (!batchTermsMap.has(result.word)) {
                batchTermsMap.set(result.word, {
                  word: result.word,
                  uniqueDocIds: new Set(),
                  scores: []
                });
              }
              
              const termEntry = batchTermsMap.get(result.word);
              
              // Also update the global metadata map
              if (!globalTermsMetadata.has(result.word)) {
                globalTermsMetadata.set(result.word, {
                  docCount: 0,
                  scoreCount: 0
                });
                stats.uniqueTermsFound++;
              }
              
              // Add all scores from this result
              if (Array.isArray(result.scores)) {
                result.scores.forEach(score => {
                  if (!score || !score.docId) return;
                  
                  // Add this document ID to the set of unique docs
                  termEntry.uniqueDocIds.add(score.docId);
                  
                  // Add score to our collection
                  termEntry.scores.push({...score});
                  
                  // Update global metadata
                  globalTermsMetadata.get(result.word).scoreCount++;
                });
              }
              
              // Progress reporting
              if (stats.totalItemsProcessed % config.reportInterval === 0) {
                const elapsed = (Date.now() - stats.startTime) / 1000;
                const itemsPerSecond = stats.totalItemsProcessed / elapsed;
                console.log(`Processed ${stats.totalItemsProcessed}/${rawResults.length} items (${Math.round(stats.totalItemsProcessed/rawResults.length*100)}%) at ${Math.round(itemsPerSecond)} items/sec`);
                console.log(`Current memory usage: ${Math.round(process.memoryUsage().heapUsed/1024/1024)}MB`);
              }
            }
            
            // Calculate document frequencies and update global metadata
            for (const [word, entry] of batchTermsMap.entries()) {
              const docCount = entry.uniqueDocIds.size;
              globalTermsMetadata.get(word).docCount += docCount;
            }
            
            // Save this batch's data to a temporary file
            const batchTempFile = `${config.tempDir}/batch_${batchIndex}.json`;
            fs.writeFileSync(batchTempFile, JSON.stringify(Array.from(batchTermsMap.entries())));
            stats.tempFilesCreated++;
            
            // Record batch statistics
            stats.batchesProcessed++;
            
            // Log batch completion
            const batchDuration = (Date.now() - batchStartTime) / 1000;
            console.log(`Completed batch ${batchIndex+1}/${totalBatches} in ${batchDuration.toFixed(2)}s`);
            console.log(`Stored ${batchTermsMap.size} unique terms from this batch`);
            
            // Check memory usage and force GC if needed
            const memUsage = process.memoryUsage();
            const usedPercent = (memUsage.heapUsed / memUsage.heapTotal) * 100;
            console.log(`Memory usage: ${Math.round(memUsage.heapUsed/1024/1024)}MB / ${Math.round(memUsage.heapTotal/1024/1024)}MB (${Math.round(usedPercent)}%)`);
            
            if (usedPercent > config.maxMemoryPercent && global.gc) {
              console.log(`Memory usage high (${Math.round(usedPercent)}%), forcing garbage collection`);
              global.gc();
              console.log(`Memory after GC: ${Math.round(process.memoryUsage().heapUsed/1024/1024)}MB`);
            }
            
            // Clear the batch map to free memory
            batchTermsMap.clear();
          }
          
          console.log(`\nCompleted first pass processing. Statistics:`);
          console.log(`- Items processed: ${stats.totalItemsProcessed}`);
          console.log(`- Unique terms found: ${stats.uniqueTermsFound}`);
          console.log(`- Batches processed: ${stats.batchesProcessed}`);
          console.log(`- Temporary files created: ${stats.tempFilesCreated}`);
          
          // Second pass: Combine all temporary files and calculate final values
          console.log(`\nStarting second pass: combining results from ${stats.tempFilesCreated} temporary files`);
          
          // Final map to hold combined results with optimized structure
          const finalTermsMap = new Map();
          
          // Process each temp file
          for (let i = 0; i < stats.tempFilesCreated; i++) {
            const tempFile = `${config.tempDir}/batch_${i}.json`;
            console.log(`Processing temp file ${i+1}/${stats.tempFilesCreated}: ${tempFile}`);
            
            try {
              // Read the batch data
              const batchData = JSON.parse(fs.readFileSync(tempFile, 'utf8'));
              
              // Process each term in the batch
              for (const [word, entry] of batchData) {
                // Get or create entry in final map
                if (!finalTermsMap.has(word)) {
                  finalTermsMap.set(word, {
                    word,
                    documentFrequency: 0,
                    scores: [],
                    uniqueDocIds: new Set()
                  });
                }
                
                const finalEntry = finalTermsMap.get(word);
                
                // Add scores from this batch
                entry.scores.forEach(score => {
                  if (!score || !score.docId) return;
                  
                  // Add to unique doc IDs
                  finalEntry.uniqueDocIds.add(score.docId);
                  
                  // Add the score
                  finalEntry.scores.push(score);
                });
              }
              
              // Delete the temp file after processing
              fs.unlinkSync(tempFile);
              
            } catch (error) {
              console.error(`Error processing temp file ${tempFile}: ${error.message}`);
            }
            
            // Log memory usage and force GC if needed
            const memUsage = process.memoryUsage();
            const usedPercent = (memUsage.heapUsed / memUsage.heapTotal) * 100;
            
            if (usedPercent > config.maxMemoryPercent && global.gc) {
              console.log(`Memory usage high (${Math.round(usedPercent)}%), forcing garbage collection`);
              global.gc();
              console.log(`Memory after GC: ${Math.round(process.memoryUsage().heapUsed/1024/1024)}MB`);
            }
          }
          
          console.log(`\nCalculating final term statistics for ${finalTermsMap.size} terms`);
          
          // Calculate final results
          const finalResults = [];
          let processedTerms = 0;
          
          for (const [word, entry] of finalTermsMap.entries()) {
            processedTerms++;
            
            // Calculate document frequency from unique doc IDs
            const documentFrequency = entry.uniqueDocIds.size;
            
            // Calculate IDF
            const idf = Math.log(config.totalDocuments / documentFrequency);
            
            // Deduplicate scores by document ID
            const uniqueScores = {};
            
            // Process all scores for this word
            entry.scores.forEach(score => {
              const docId = score.docId;
              
              // Update the IDF for all scores
              score.idf = idf;
              
              // Recalculate TF-IDF with new IDF
              score.tfidf = score.tf * idf;
              
              // Keep the highest TF-IDF score for each document ID
              if (!uniqueScores[docId] || uniqueScores[docId].tfidf < score.tfidf) {
                uniqueScores[docId] = score;
              }
            });
            
            // Convert to array and calculate importance
            const finalScores = Object.values(uniqueScores);
            const importance = finalScores.reduce((sum, score) => sum + score.tfidf, 0);
            
            // Create final result object
            finalResults.push({
              word,
              documentFrequency,
              scores: finalScores,
              importance
            });
            
            // Progress reporting
            if (processedTerms % 1000 === 0) {
              console.log(`Processed ${processedTerms}/${finalTermsMap.size} terms (${Math.round(processedTerms/finalTermsMap.size*100)}%)`);
            }
            
            // Clean up memory
            entry.uniqueDocIds = null;
            entry.scores = null;
          }
          
          // Clean up the temp directory
          try {
            fs.rmdirSync(config.tempDir);
            console.log(`Removed temporary directory: ${config.tempDir}`);
          } catch (error) {
            console.warn(`Warning: Could not remove temp directory: ${error.message}`);
          }
          
          // Log final statistics
          const totalTime = (Date.now() - stats.startTime) / 1000;
          console.log(`\nBatched aggregation complete!`);
          console.log(`- Total time: ${totalTime.toFixed(2)} seconds`);
          console.log(`- Final result size: ${finalResults.length} terms`);
          console.log(`- Processing rate: ${Math.round(stats.totalItemsProcessed/totalTime)} items/second`);
          
          return finalResults;
        }
        
        // Call the improved function instead of the original
        const results = batchedAggregateResults(rawResults, {
          batchSize: 10000,                 // Adjust based on available memory
          totalDocuments: global.totalDocuments || 54825,  // Use actual document count
          tempDir: `${resultsDir}/temp`,    // Store temp files in your results directory
          maxMemoryPercent: 70              // Trigger GC at 70% memory usage
        });

        console.log(`Processing ${results.length} unique terms...`);

        // 1. Sort the results by importance (most important terms first)
        results.sort((a, b) => b.importance - a.importance);
        
        // Do NOT filter out any terms with zero importance
        
        // 2. Save the results
        // const resultsDir = "./tfidf-results";
        if (!fs.existsSync(resultsDir)) {
          fs.mkdirSync(resultsDir, { recursive: true });
        }

        fs.writeFileSync(
          `${resultsDir}/tfidf-results.json`,
          JSON.stringify(results, null, 2)
        );

        console.log(`Results saved to ${resultsDir}/tfidf-results.json`);

        // Continue with your document index creation
        const docIndex = {};

        // 3. When creating document index, preserve the global sorting
        results.forEach((result) => {
          if (!result.scores) {
            console.warn(`Term "${result.word}" has no scores array`);
            return;
          }

          result.scores.forEach((score) => {
            if (!docIndex[score.docId]) {
              docIndex[score.docId] = [];
            }

            docIndex[score.docId].push({
              word: result.word,
              tfidf: score.tfidf || 0,
              count: score.count || 0,
              globalImportance: result.importance || 0, // Use 0 as fallback instead of undefined
            });
          });
        });

        // 4. For each document, sort by TF-IDF score specific to that document
        Object.keys(docIndex).forEach((docId) => {
          docIndex[docId].sort((a, b) => b.tfidf - a.tfidf);
          // Keep only top 100 terms per document
          docIndex[docId] = docIndex[docId].slice(0, 100);
        });

        // 5. Save the enhanced document index
        fs.writeFileSync(
          `${resultsDir}/document-index.json`,
          JSON.stringify(docIndex, null, 2)
        );

        console.log(`Document index saved to ${resultsDir}/document-index.json`);

        // 6. Create a global terms index of most important terms (keeping all terms)
        const globalTermsIndex = results.slice(0, 1000).map(result => ({
          word: result.word,
          importance: result.importance || 0, // Use 0 as fallback
          documentFrequency: result.documentFrequency || 0 // Use 0 as fallback
        }));

        fs.writeFileSync(
          `${resultsDir}/global-terms-index.json`,
          JSON.stringify(globalTermsIndex, null, 2)
        );

        console.log(`Global terms index saved to ${resultsDir}/global-terms-index.json`);

        finish();
      });
    });
  });

  // Cleanup function
  const finish = async () => {
    console.log("SHUTTING DOWN...");
    await stop_node(node);
    server.close();
  };
  });
});
