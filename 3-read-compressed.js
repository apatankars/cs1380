const LZ = require("lz-string");

const distribution = require("../../config.js");

distribution.node.start(async (server) => {
  console.log("Node started for testing read-compressed functionality.");

  let node = { ip: "127.0.0.1", port: 7110 };

  distribution.local.status.spawn(node, (e, v) => {
    let testGroup = {};
    testGroup[distribution.util.id.getSID(node)] = node;
    let testConfig = { gid: "tfidf" };

    if (e) {
      console.error("Error spawning node:", e);
      server.stop(() => {
        console.log("Server stopped due to spawn error.");
      });
      return;
    }
    console.log(
      "Spawned node at",
      `${node.ip}:${node.port}`,
      "with result:",
      v
    );
    const decompressData = (data) => {
      try {
        // Decompress the base64 encoded string
        const decompressed = LZ.decompressFromBase64(data);
        if (!decompressed) {
          console.error("Failed to decompress data");
          return null;
        }
        return decompressed;
      } catch (error) {
        console.error("Error during decompression:", error);
        return null;
      }
    };
    // Set up the group for testing
    distribution.local.groups.put(testConfig, testGroup, (e, v) => {
      let messageConfig = {
        service: "store",
        method: "get",
        node: node,
      };
      let message = [
        {
          gid: "tfidf", // Group ID where documents are stored
          key: null, // Get all keys in the group
        },
      ];

      distribution.local.comm.send(message, messageConfig, (err, result) => {
        if (err) {
          console.error("Error retrieving data from store:", err);
          server.close(() => {
            console.log("Server stopped due to store retrieval error.");
          });
          return;
        }
        if (!result) {
          console.error("No result found in the store.");
          server.close(() => {
            console.log("Server stopped due to no result found.");
          });
          return;
        }

        let keys = result.slice(0, 12);

        console.log(
          "Successfully retrieved keys from store:",
          keys,
          Array.isArray(keys) ? keys.length : 0
        );

        let pending = keys.length;

        function parseArticleData(rawString) {
          try {
            // Find where the actual JSON begins (after "string ")
            const jsonStartIndex = rawString.indexOf('{"hierarchy"');
            if (jsonStartIndex === -1) {
              console.error("Could not find JSON data in the string");
              return null;
            }

            // Extract just the JSON part
            const jsonString = rawString.substring(jsonStartIndex);

            // Parse it into an object
            const articleObject = JSON.parse(jsonString);
            return articleObject;
          } catch (error) {
            console.error("Error parsing JSON:", error);
            console.log("First 100 chars:", rawString.substring(0, 100));
            return null;
          }
        }

        Array.from(keys).forEach((key) => {
          distribution.tfidf.store.get(key, (e, v) => {
            if (e) {
              console.error(`Error retrieving key ${key} from store:`, e);
              pending--;
              if (pending === 0) {
                server.close(() => {
                  console.log("Server stopped after retrieval errors.");
                });
              }
              return;
            }

            const articleData = parseArticleData(v);

            if (articleData) {
              console.log("Successfully parsed article data for:", key);

              // Now you can access properties easily
              console.log("Binomial name:", articleData.binomial_name);
              console.log("URL:", articleData.url);

              // Access the taxonomy
              console.log("Taxonomy:");
              articleData.hierarchy.forEach(([level, value]) => {
                console.log(`  ${level}: ${value}`);
              });

              // You can even get specific words from the article
              console.log(
                "First 10 words:",
                articleData.article_words.slice(0, 10)
              );

              // Do further processing with the object...
            }

            distribution.tfidf.store.put(articleData, key, (err, val) => {});

            pending--;
            if (pending === 0) {
              console.log(
                "All TF-IDF retrievals completed, shutting down server."
              );

              // All done, close the server
              server.close(() => {
                console.log("Server stopped after all retrievals.");
              });
            }
          });
        });
      });
    });
  });

  // You can add any additional logic here if needed
  // For example, you might want to perform some operations using the decompressed data
  // distribution.local.comm.send(
  //   [],
  //   { service: "status", method: "stop", node: node },
  //   (e, v) => {
  //     if (e) {
  //       console.error("Failed to stop the node:", e);
  //     } else {
  //       console.log("Node stopped successfully.");
  //     }
  //   }
  // );
});
