const distribution = require("./config.js");
const id = distribution.util.id;
const fs = require("fs");

// Set up a single node for testing
const node = { ip: "127.0.0.1", port: 7112 };
const num_nodes = 4;
const nids = [];
const nodes = [];
const testGroup = {};
const testConfig = { gid: "tfidf" };
// testGroup[distribution.util.id.getSID(node)] = node;
for(let i = 0; i < num_nodes; i++) {
    nodes.push({ ip: '127.0.0.1', port: 7112 + i });
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
        const stopList = [
          // NLTK-inspired common English stop words:
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
          "too", "very", "s", "t", "can", "will", "just", "don", "should",
          "now",
          // Additional HTML, Wiki, and domain-specific terms:
          "doctype", "html", "head", "body", "parser", "output", "navbox",
          "reflist", "css", "clientpref", "template", "wikipedia", "org", "url",
          "wikidata", "wiki", "infobox", "toc", "citation", "references", "special",
          "edit", "content", "class", "div", "span", "id", "style", "script",
          "link", "meta", "nav", "footer", "header"
        ];

        const docData = value;
        const docId = docData.url;
        const words = docData.article_words || [];

        // Count word occurrences in this document
        const wordCounts = {};

        words.forEach((word) => {
          // Skip words with 2 or fewer characters
          if (word.length <= 2) return;

          // Normalize to lowercase for consistency
          const cleanWord = word.toLowerCase();

          // Skip if the word is in the stop list
          if (stopList.indexOf(cleanWord) !== -1) return;

          // Filter out words containing any non-alphabetic characters 
          // (this removes numbers, punctuation, and mixed tokens)
          if (!/^[a-z]+$/.test(cleanWord)) return;

          // Count the word occurrence
          wordCounts[cleanWord] = (wordCounts[cleanWord] || 0) + 1;
        });

        // Emit each word with document ID, count, and total number of words processed
        return Object.entries(wordCounts).map(([word, count]) => {
          return { [word]: { docId, count, totalWords: words.length } };
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
        function improvedAggregateResults(rawResults) {
          console.log("Properly aggregating duplicate terms...");
          
          // Create a map to store merged results by word
          const mergedResultsMap = new Map();
          const totalDocuments = 54825; // Use actual count from earlier
          
          // First pass: combine all entries for the same word
          rawResults.forEach(result => {
            if (!result.word) return;
            
            // Get or create entry for this word
            if (!mergedResultsMap.has(result.word)) {
              mergedResultsMap.set(result.word, {
                word: result.word,
                uniqueDocIds: new Set(), // Track unique document IDs
                allScores: [] // Collect all score objects
              });
            }
            
            const wordEntry = mergedResultsMap.get(result.word);
            
            // Add all scores from this result
            if (Array.isArray(result.scores)) {
              result.scores.forEach(score => {
                if (score.docId) {
                  // Add this document ID to our unique set
                  wordEntry.uniqueDocIds.add(score.docId);
                  
                  // Add this score to our collection
                  wordEntry.allScores.push({...score});
                }
              });
            }
          });
          
          // Track debug metrics
          let debugMetrics = {
            totalTerms: mergedResultsMap.size,
            totalScores: 0,
            maxDocsForTerm: 0,
            maxTerm: ""
          };
          
          // Second pass: recalculate values based on combined data
          const finalResults = [];
          
          for (const [word, wordEntry] of mergedResultsMap.entries()) {
            // Get the correct document frequency (unique docs)
            const documentFrequency = wordEntry.uniqueDocIds.size;
            
            // Track term with most docs
            if (documentFrequency > debugMetrics.maxDocsForTerm) {
              debugMetrics.maxDocsForTerm = documentFrequency;
              debugMetrics.maxTerm = word;
            }
            
            // Calculate new IDF value based on actual document frequency
            const idf = Math.log(totalDocuments / documentFrequency);
            
            // Deduplicate scores by document ID
            const uniqueScores = {};
            
            // Process all scores for this word
            wordEntry.allScores.forEach(score => {
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
            
            const finalScores = Object.values(uniqueScores);
            debugMetrics.totalScores += finalScores.length;
            
            // Create the final result object
            const finalResult = {
              word: word,
              documentFrequency: documentFrequency,
              scores: finalScores
            };
            
            // Calculate importance as sum of TF-IDF values
            finalResult.importance = finalResult.scores.reduce(
              (sum, score) => sum + score.tfidf, 0
            );
            
            finalResults.push(finalResult);
          }
          
          console.log(`Aggregation complete. Debug metrics:
          - Unique terms: ${debugMetrics.totalTerms}
          - Total document scores: ${debugMetrics.totalScores}
          - Most frequent term: "${debugMetrics.maxTerm}" (in ${debugMetrics.maxDocsForTerm} documents)`);
          
          return finalResults;
        }
        
        // Call the improved function instead of the original
        const results = improvedAggregateResults(rawResults);

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
