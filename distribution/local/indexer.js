// distribution/local/indexer.js
const util = require("../util/util");
const id = util.id;

// Default callback
const cb = (e, v) => {
  if (e) {
    console.error(e);
  } else {
    console.log(v);
  }
};



// const indexer = {
  /**
   * Process a document and distribute its terms to appropriate index nodes
   * 
   * @param {Object} configuration - Contains the document data to index
   * @param {Function} callback - Callback function (error, result)
   */
function index(configuration, callback) {
    callback = callback || cb;

    // Create a set of stop words (common words to exclude)
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

    // Pre-compile regex pattern for alphabetic-only words
    const alphaOnlyPattern = /^[a-z]+$/;

    /**
     * Helper function to extract a smart prefix from a word
     * Using first two letters creates a good distribution (up to 676 prefixes)
     */
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
      const kid = distribution.util.id.getID(key);

      // 2) Use our chosen hash function to pick exactly one NID
      const chosenNID = distribution.util.id.naiveHash(kid, nids);

      // 3) find the node config whose NID matches chosenNID
      const chosenNode = nodes.find((nc) => distribution.util.id.getNID(nc) === chosenNID);
      return chosenNode;
    }
    
    if (!configuration) {
      return callback(new Error('Configuration is required for indexing'), null);
    }
    
    // Extract document data from configuration
    const document = configuration.value || configuration;
    
    if (!document || !document.url || !document.article_words) {
      return callback(new Error('Document data is missing required fields'), null);
    }
    
    // Initialize metrics
    const metrics = {
      processingStartTime: Date.now(),
      totalTerms: 0,
      totalPrefixes: 0,
      prefixBatchSizes: [],
      nodeBatchTimes: new Map(),
      processingEndTime: 0
    };
    
    try {
      const docId = document.url;
      const words = document.article_words || [];
      const hierarchy = document.hierarchy || [];
      const binomialName = document.binomial_name || '';
      
      console.log(`Processing document: ${docId}`);
      
      // Extract taxonomic information for ranking
      const taxonomyInfo = {};
      if (hierarchy && Array.isArray(hierarchy)) {
        hierarchy.forEach(pair => {
          if (Array.isArray(pair) && pair.length === 2) {
            const [label, value] = pair;
            taxonomyInfo[label] = value;
          }
        });
      }
      
      // Calculate kingdom and family for special weighting
      const kingdom = taxonomyInfo['kingdom'] || '';
      const family = taxonomyInfo['family'] || '';
      
      // Process words and count occurrences
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
      
      console.log(`Document ID: ${docId}, Total words processed: ${totalWords}, Total unique terms: ${wordCounts.size}`);
      metrics.totalTerms = wordCounts.size;
      
      // Get nodes information for distribution
      distribution.local.groups.get('index', (err, group) => {
        if (err || !group) {
          console.log(`Failed to get 'index' group: ${err ? err.message : 'Group not found'}`);
          return callback(new Error(`Failed to get 'index' group: ${err ? err.message : 'Group not found'}`), null);
        }
        
        const nodes = Object.values(group);
        const nids = nodes.map(node => distribution.util.id.getNID(node));
        
        // Create maps for grouping
        const prefixGroups = new Map(); // prefix -> terms
        const nodeToPrefix = new Map(); // node -> prefixes
        
        // First pass: Group terms by prefix
        for (const [word, count] of wordCounts) {
          const prefix = getSmartPrefix(word);
          if (!prefixGroups.has(prefix)) {
            prefixGroups.set(prefix, new Map());
          }
          prefixGroups.get(prefix).set(word, count);
        }
        
        metrics.totalPrefixes = prefixGroups.size;
        
        // Second pass: Assign prefixes to nodes
        for (const [prefix, terms] of prefixGroups) {
          const chosenNode = getChosenNode(prefix, nids, nodes);
          if (!nodeToPrefix.has(chosenNode)) {
            nodeToPrefix.set(chosenNode, new Map());
          }
          nodeToPrefix.get(chosenNode).set(prefix, terms);
        }
        
        // Third pass: Prepare data for each node and send it
        let completedBatches = 0;
        let totalBatches = 0;
        
        // Count total batches first
        for (const [node, prefixes] of nodeToPrefix) {
          if (prefixes.size > 0) {
            totalBatches++;
          }
        }
        
        // If no batches to send, return immediately
        if (totalBatches === 0) {
          metrics.processingEndTime = Date.now();
          return callback(null, {
            status: 'success',
            docId: docId,
            metrics: {
              totalTerms: metrics.totalTerms,
              totalPrefixes: metrics.totalPrefixes,
              processingTime: metrics.processingEndTime - metrics.processingStartTime,
              batchesSent: 0
            }
          });
        }
        
        // Process each node's data
        for (const [node, prefixes] of nodeToPrefix) {
          const nodeId = distribution.util.id.getNID(node);
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
              // Calculate term frequency (TF)
              const tf = count / wordCounts.size;
              
              // Enhanced ranking factors
              
              // 1. Check if word appears in taxonomy information
              const taxonomyMatch = Object.entries(taxonomyInfo).find(
                ([key, value]) => value.toLowerCase().includes(word)
              );
              const inTaxonomy = !!taxonomyMatch;
              const taxonomyLevel = inTaxonomy ? taxonomyMatch[0] : null;
              
              // 2. Check if word is part of binomial name (scientific name)
              const inBinomialName = binomialName.toLowerCase().includes(word);
              
              // 3. Calculate position importance (terms appearing in kingdom/family get boost)
              const inKingdom = kingdom.toLowerCase().includes(word);
              const inFamily = family.toLowerCase().includes(word);
              
              // 4. Calculate proximity to taxonomic terms
              // This would need context of the full text, simplified here
              
              // Create enhanced ranking score with multiple factors
              const rankingFactors = {
                // Base weight is term frequency
                tf: tf,
                // Taxonomy level boosts (kingdom is more important than species for classification)
                taxonomyBoost: inTaxonomy ? (
                  taxonomyLevel === 'kingdom' ? 5.0 :
                  taxonomyLevel === 'phylum' ? 4.0 :
                  taxonomyLevel === 'class' ? 3.0 :
                  taxonomyLevel === 'order' ? 2.5 :
                  taxonomyLevel === 'family' ? 2.0 :
                  taxonomyLevel === 'genus' ? 1.5 : 1.0
                ) : 1.0,
                // Scientific name is highly specific to the entity
                binomialBoost: inBinomialName ? 4.0 : 1.0,
                // Position importance (kingdom/family terms are important classifiers)
                positionBoost: inKingdom ? 3.0 : (inFamily ? 2.0 : 1.0),
                // Final score combines all factors
                score: 0 // Calculated below
              };
              
              // Calculate final score as product of all factors × tf
              rankingFactors.score = tf * 
                rankingFactors.taxonomyBoost * 
                rankingFactors.binomialBoost * 
                rankingFactors.positionBoost;
              
              // Create entry for this term with all the enhanced data
              prefixData[word] = [{
                url: docId,
                tf: tf,
                ranking: rankingFactors,
                taxonomyLevel: taxonomyLevel,
                isBinomial: inBinomialName,
                // Store page metadata for later query refinement
                pageInfo: {
                  kingdom: kingdom,
                  family: family,
                  binomialName: binomialName
                }
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
          
          // Only send if we have prefixes to send
          if (nodePrefixBatches.length > 0) {
            console.log(`Sending batch with ${nodePrefixBatches.length} prefixes to node ${nodeId}`);
            metrics.prefixBatchSizes.push(nodePrefixBatches.length);
            
            const batchStartTime = Date.now();
            
            // Send batch using bulk_append
            distribution.local.comm.send([{
              prefixBatches: nodePrefixBatches,
              gid: 'index'
            }], {
              service: "store",
              method: "bulk_append",
              node: node
            }, (err, val) => {
              const batchEndTime = Date.now();
              const batchTime = batchEndTime - batchStartTime;
              
              // Update metrics
              metrics.nodeBatchTimes.get(nodeId).batchCount++;
              metrics.nodeBatchTimes.get(nodeId).totalTime += batchTime;
              
              if (err) {
                console.error(`Error sending to ${node.ip}:${node.port}:`, err);
                completedBatches++;
                
                // Check if all batches are completed
                if (completedBatches === totalBatches) {
                  finishProcessing(false);
                }
              } else {
                console.log(`Batch sent to ${node.ip}:${node.port} (${batchTime.toFixed(2)}ms)`);
                completedBatches++;
                
                // Check if all batches are completed
                if (completedBatches === totalBatches) {
                  finishProcessing(true);
                }
              }
            });
          }
        }
        
        // Helper function to finish processing and report metrics
        function finishProcessing(success) {
          metrics.processingEndTime = Date.now();
          
          // Log performance metrics
          const totalProcessingTime = metrics.processingEndTime - metrics.processingStartTime;
          console.log(`Total processing time: ${totalProcessingTime}ms`);
          console.log(`Total terms processed: ${metrics.totalTerms}`);
          console.log(`Total prefixes: ${metrics.totalPrefixes}`);
          
          // Calculate average batch size
          if (metrics.prefixBatchSizes.length > 0) {
            const avgBatchSize = metrics.prefixBatchSizes.reduce((sum, size) => sum + size, 0) / 
                               metrics.prefixBatchSizes.length;
            console.log(`Average batch size: ${avgBatchSize.toFixed(2)} prefixes`);
          }
          
          // Calculate node processing stats
          let totalBatchCount = 0;
          let totalBatchTime = 0;
          
          for (const [nodeId, stats] of metrics.nodeBatchTimes) {
            totalBatchCount += stats.batchCount;
            totalBatchTime += stats.totalTime;
            
            if (stats.batchCount > 0) {
              const avgNodeBatchTime = stats.totalTime / stats.batchCount;
              console.log(`Node ${nodeId}: ${stats.batchCount} batches, avg ${avgNodeBatchTime.toFixed(2)}ms, ${stats.termCount} terms`);
            }
          }
          
          if (totalBatchCount > 0) {
            const avgBatchTime = totalBatchTime / totalBatchCount;
            console.log(`Overall average batch time: ${avgBatchTime.toFixed(2)}ms`);
          }
          
          // Return result
          callback(null, {
            status: success ? 'success' : 'partial_success',
            docId: docId,
            metrics: {
              totalTerms: metrics.totalTerms,
              totalPrefixes: metrics.totalPrefixes,
              processingTime: totalProcessingTime,
              batchesSent: totalBatchCount
            }
          });
        }
      });
    } catch (error) {
      console.error("Error processing document:", error);
      metrics.processingEndTime = Date.now();
      callback(error, null);
    }
  }
// };

module.exports = { index };