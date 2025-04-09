const distribution = require('./config.js');
const id = distribution.util.id;
const performance = require('perf_hooks').performance;

function parseArticleData(rawString) {
  try {
    // First parse the outer JSON structure
    const outerObject = JSON.parse(rawString);

    // console.log("Outer object:", outerObject);
    if (outerObject.url) {
      return outerObject;
    }
    
    // Now parse the inner JSON string contained in the value property
    if (outerObject && outerObject.type === 'string' &&outerObject.value) {
      const innerObject = JSON.parse(outerObject.value);
      console.log("Inner object:", innerObject);
      return innerObject;
    } else {
      console.error("Unexpected data format");
      return null;
    }
  } catch (error) {
    console.error("Error parsing JSON:", error);
    console.log("First 100 chars:", rawString.substring(0, 100));
    return null;
  }
}

function isEmptyObject(obj) {
  return obj && typeof obj === "object" && Object.keys(obj).length === 0;
}

const nodes = [
  { ip: "127.0.0.1", port: 8110 },
  { ip: "127.0.0.1", port: 8111 },
  { ip: "127.0.0.1", port: 8112 },
  { ip: "127.0.0.1", port: 8113 },
  { ip: "127.0.0.1", port: 8114 },
  { ip: "127.0.0.1", port: 8115 },
  { ip: "127.0.0.1", port: 8116 },
  { ip: "127.0.0.1", port: 8117 },
];

const tfidfConfig = {gid: "tfidf"}
const nids = [];

const testGroup = {};

for(let i = 0; i < nodes.length; i++) {
  let nodeConfig = nodes[i];
  let nid = id.getNID(nodeConfig);
  nids.push(nid);
  testGroup[id.getSID(nodeConfig)] = nodeConfig
}


// Add these at the top of your file
const PREFIX_STATS = {};
const COMMON_PREFIXES = new Set([
  'th', 'an', 'co', 're', 'in', 'de', 'pr', 'st', 'en', 'tr', 'di', 'ch', 'pe'
]);

// Improved prefix function
function getSmartPrefix(term) {
  if (!term) return 'aa';
  
  const normalized = term.toLowerCase();
  const basePrefix = normalized.substring(0, 2);
  
  // For common prefixes, use 3 characters to distribute more evenly
  if (COMMON_PREFIXES.has(basePrefix) && term.length >= 3) {
    return normalized.substring(0, 3);
  }
  
  // For uncommon prefixes or short words, use 2 characters
  return basePrefix;
}

function getChosenNode(key, nids, nodes) {
  // 1) Get the key id
  const kid = id.getID(key);

  // 2) Use our chosen hash function to pick exactly one NID
  const chosenNID = distribution.util.id.naiveHash(kid, nids);

  // 3) find the node config whose NID matches chosenNID
  chosenNode = nodes.find((nc) => id.getNID(nc) === chosenNID);
  return chosenNode;
}

distribution.node.start(async (server) => {
  console.log("SETTING UP OPTIMIZED TF-IDF TEST NODE...");

  const metrics = {
    totalTerms: 0,
    totalPrefixes: 0,
    processingStartTime: 0,
    processingEndTime: 0,
    nodeBatchTimes: new Map(), // Map<nodeId, {batchCount, totalTime}>
    prefixBatchSizes: []
  };

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

  // Set up the TFIDF group
  distribution.local.groups.put(tfidfConfig, testGroup, (e, v) => {
    if (e && !isEmptyObject(e)) {
      console.error("Error setting up TFIDF group:", e);
      finish();
      return;
    }
    
    console.log("TFIDF group set up successfully");
    
    distribution.tfidf.store.get('-wiki-A--sojae', async (error, value) => {
      if (e && !isEmptyObject(e)) {
        console.log("Could not retrieve results: ", error);
        finish();
      } else {
        // Performance metrics
        const metrics = {
          totalTerms: 0,
          totalPrefixes: 0,
          processingStartTime: performance.now(),
          processingEndTime: 0,
          nodeBatchTimes: new Map(), // Map<nodeId, {batchCount, totalTime}>
          prefixBatchSizes: []
        };
        
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
          
          
          // References and citation terms
          "doi", "isbn", "pmid", "citation", "cite", "reference", "references", "info", "sources",
          "source", "statement", "footnote", "note", "notes", "publisher", "published", "press",
          "journal", "vol", "department", "university", "link", "links", "external", "pdf",
          "attribution", "online", "license", "creative", "sharealike", "information", "data",
          "checklist", "ipni", "database", "foundation", "royal", "society", "academic", "research",
          "web", "api", "itis", "gbif", "tropicos", "kew", "eol", "natureserve", "grin", "eppo",
          "microbank", "powo", "gardens", "speciesfungorum", "indexfungorum", "nzor",
          "mycobank", "wayback", "calflora", "rhs", "apni", "urn", "lsid", "field", "guide", "encyclopedia", "col", "production",
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
        metrics.processingStartTime = performance.now();
        // Pre-compile the regex pattern for better performance
        const alphaOnlyPattern = /^[a-z]+$/;

        const docData = parseArticleData(value);
        const docId = docData.url;
        const words = docData.article_words || [];
        console.log("Processing document:", docId);

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

        console.log(`Document ID: ${docId}, Total words processed: ${totalWords}, Total Clean Words: ${wordCounts.size}`);
        metrics.totalTerms = wordCounts.size;
        
        // Create maps for grouping
        const prefixGroups = new Map(); // prefix -> terms
        const nodeToPrefix = new Map(); // node -> prefixes
        
        // First pass: Group terms by prefix
        Array.from(wordCounts, ([word, count]) => {
          const prefix = getSmartPrefix(word);
          if (!prefixGroups.has(prefix)) {
            prefixGroups.set(prefix, new Map());
          }
          prefixGroups.get(prefix).set(word, count);
        });
        
        metrics.totalPrefixes = prefixGroups.size;
        
        // Second pass: Assign prefixes to nodes
        for (const [prefix, terms] of prefixGroups) {
          const chosenNode = getChosenNode(prefix, nids, nodes);
          if (!nodeToPrefix.has(chosenNode)) {
            nodeToPrefix.set(chosenNode, new Map());
          }
          nodeToPrefix.get(chosenNode).set(prefix, terms);
        }
        
        // Third pass: Prepare data for each node
        const allSendPromises = []; // Define outside the loop, collect all promises
        
        for (const [node, prefixes] of nodeToPrefix) {
          const nodeId = id.getNID(node);
          const nodePrefixBatches = [];
          
          // Initialize metrics for this node
          if (!metrics.nodeBatchTimes.has(nodeId)) {
            metrics.nodeBatchTimes.set(nodeId, {
              batchCount: 0,
              totalTime: 0,
              termCount: 0
            });
          }
          
          let nodeTermCount = 0;
          
          for (const [prefix, terms] of prefixes) {
            const prefixData = {};
            for (const [word, count] of terms) {
              prefixData[word] = [{
                url: docId,
                tf: count / wordCounts.size
              }];
              nodeTermCount++;
            }
            
            // Create a batch for this prefix
            nodePrefixBatches.push({
              prefix,
              data: prefixData
            });
          }
          
          // Update metrics
          metrics.nodeBatchTimes.get(nodeId).termCount = nodeTermCount;
          
          console.log(`Sending ONE batch with ${nodePrefixBatches.length} prefixes to node ${nodeId}`);
          metrics.prefixBatchSizes.push(nodePrefixBatches.length);
          
          // Send ONE batch per node
          if (nodePrefixBatches.length > 0) {
            const sendPromise = new Promise((resolve, reject) => {
              const batchStartTime = performance.now();
              distribution.local.comm.send([{
                prefixBatches: nodePrefixBatches,
                gid: 'index'
              }], {
                service: "store",
                method: "bulk_append",
                node: node
              }, (e, v) => {
                const batchEndTime = performance.now();
                const batchTime = batchEndTime - batchStartTime;
              
                // Update metrics
                metrics.nodeBatchTimes.get(nodeId).batchCount++;
                metrics.nodeBatchTimes.get(nodeId).totalTime += batchTime;
                if (e) {
                  console.error(`Error sending to ${node.ip}:${node.port}:`, e);
                  reject(e);
                } else {
                  console.log(`Batch sent to ${node.ip}:${node.port} (${batchTime.toFixed(2)}ms)`);
                  resolve(v);
                }
              });
            });
            
            allSendPromises.push(sendPromise);
          }
        }
        
        // Wait for all batches to complete
        try {
          await Promise.all(allSendPromises);
          console.log("All batches sent successfully!");
        } catch (err) {
          console.error("Error in batch processing:", err);
        } finally {
          // Always calculate metrics, even if errors occurred
          metrics.processingEndTime = performance.now();
          logPerformanceMetrics(metrics);
          // finish();
        }
        distribution.local.groups.put({gid: "index"}, testGroup, (e, v) => {
          distribution.index.store.get("prefix-str", (error, value) => {
            if (error) {
              console.error("Error fetching prefix-the:", error, value);
              finish();
            } else {
              console.log("FOUND PREFIX: ", value);
              finish();
            }
          });
        })
      }
    });
  });
  const finish = async () => {
    console.log("SHUTTING DOWN...");
    for(let i = 0; i < nodes.length; i++) {

      // console.log(`Cleaning up node ${nodes[i].ip}:${nodes[i].port}`);
      let node_config = nodes[i];
      try {
        await stop_node(node_config);
        console.log(`Node at ${node_config.ip}:${node_config.port} stopped successfully`);
      } catch (e) {
        console.error(`Failed to stop node at ${node_config.ip}:${node_config.port}`, e);
      }
    }
    server.close();
  };
  // Add this function to log performance metrics
  function logPerformanceMetrics(metrics) {
    const totalProcessingTime = metrics.processingEndTime - metrics.processingStartTime;
    const termsPerSecond = metrics.totalTerms > 0 ? (metrics.totalTerms / totalProcessingTime) * 1000 : 0;
    
    console.log("\n===== PERFORMANCE METRICS =====");
    console.log(`Total Processing Time: ${totalProcessingTime.toFixed(2)}ms`);
    console.log(`Total Terms Processed: ${metrics.totalTerms}`);
    console.log(`Total Prefixes: ${metrics.totalPrefixes}`);
    console.log(`Throughput: ${termsPerSecond.toFixed(2)} terms/second`);
    
    console.log("\n--- Node Distribution ---");
    
    // Default values in case there's no data
    let maxTerms = 0, minTerms = Infinity;
    let maxLatency = 0, minLatency = Infinity;
    let totalLatency = 0, totalBatches = 0;
    
    // Only process if there are entries
    if (metrics.nodeBatchTimes.size > 0) {
      metrics.nodeBatchTimes.forEach((stats, nodeId) => {
        const avgLatency = stats.batchCount > 0 ? stats.totalTime / stats.batchCount : 0;
        totalLatency += stats.totalTime;
        totalBatches += stats.batchCount;
        
        if (stats.termCount > 0) minTerms = Math.min(minTerms, stats.termCount);
        maxTerms = Math.max(maxTerms, stats.termCount);
        
        if (avgLatency > 0) minLatency = Math.min(minLatency, avgLatency);
        maxLatency = Math.max(maxLatency, avgLatency);
        
        console.log(`Node ${nodeId}: ${stats.termCount} terms, ${stats.batchCount} batches, Avg Latency: ${avgLatency.toFixed(2)}ms`);
      });
    }
    
    // Handle edge cases
    if (minTerms === Infinity) minTerms = 0;
    if (minLatency === Infinity) minLatency = 0;
    
    const termRatio = minTerms > 0 ? (maxTerms/minTerms).toFixed(2) : 0;
    const avgBatchLatency = totalBatches > 0 ? (totalLatency/totalBatches).toFixed(2) : 0;
    
    console.log("\n--- Load Balancing ---");
    console.log(`Terms Distribution - Min: ${minTerms}, Max: ${maxTerms}, Ratio: ${termRatio}`);
    console.log(`Latency - Min: ${minLatency.toFixed(2)}ms, Max: ${maxLatency.toFixed(2)}ms, Avg: ${avgBatchLatency}ms`);
    
    // Calculate average batch size safely
    const totalBatchSizes = metrics.prefixBatchSizes.reduce((sum, size) => sum + size, 0);
    const avgBatchSize = metrics.prefixBatchSizes.length > 0 ? 
      totalBatchSizes / metrics.prefixBatchSizes.length : 0;
    
    console.log(`Average Batch Size: ${avgBatchSize.toFixed(2)} prefixes/batch`);
    console.log("================================\n");
  }
});
