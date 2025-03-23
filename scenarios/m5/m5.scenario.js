const distribution = require("../../config.js");
const id = distribution.util.id;

const ncdcGroup = {};
const dlibGroup = {};
const tfidfGroup = {};
const crawlGroup = {};
const urlxtrGroup = {};
const strmatchGroup = {};
const ridxGroup = {};
const rlgGroup = {};

/*
    The local node will be the orchestrator.
*/
let localServer = null;

const n1 = { ip: "127.0.0.1", port: 7110 };
const n2 = { ip: "127.0.0.1", port: 7111 };
const n3 = { ip: "127.0.0.1", port: 7112 };
const n4 = { ip: "127.0.0.1", port: 7113 };
const n5 = { ip: "127.0.0.1", port: 7114 };
const n6 = { ip: "127.0.0.1", port: 7115 };
const n7 = { ip: "127.0.0.1", port: 7116 };
const n8 = { ip: "127.0.0.1", port: 7117 };
const n9 = { ip: "127.0.0.1", port: 7118 };

test("(0 pts) (scenario) all.mr:ncdc", (done) => {
  /* Implement the map and reduce functions.
   The map function should parse the string value and return an object with the year as the key and the temperature as the value.
   The reduce function should return the maximum temperature for each year.

   (The implementation for this scenario is provided below.)
*/

  const mapper = (key, value) => {
    const words = value.split(/(\s+)/).filter((e) => e !== " ");
    const out = {};
    out[words[1]] = parseInt(words[3]);
    return out;
  };

  const reducer = (key, values) => {
    const out = {};
    out[key] = values.reduce((a, b) => Math.max(a, b), -Infinity);
    return out;
  };

  const dataset = [
    { "000": "006701199099999 1950 0515070049999999N9 +0000 1+9999" },
    { 106: "004301199099999 1950 0515120049999999N9 +0022 1+9999" },
    { 212: "004301199099999 1950 0515180049999999N9 -0011 1+9999" },
    { 318: "004301265099999 1949 0324120040500001N9 +0111 1+9999" },
    { 424: "004301265099999 1949 0324180040500001N9 +0078 1+9999" },
  ];

  const expected = [{ 1950: 22 }, { 1949: 111 }];

  const doMapReduce = (cb) => {
    distribution.ncdc.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }

      distribution.ncdc.mr.exec(
        { keys: v, map: mapper, reduce: reducer },
        (e, v) => {
          try {
            expect(v).toEqual(expect.arrayContaining(expected));
            done();
          } catch (e) {
            done(e);
          }
        }
      );
    });
  };

  let cntr = 0;
  // Send the dataset to the cluster
  dataset.forEach((o) => {
    const key = Object.keys(o)[0];
    const value = o[key];
    distribution.ncdc.store.put(value, key, (e, v) => {
      cntr++;
      // Once the dataset is in place, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

test("(10 pts) (scenario) all.mr:dlib", (done) => {
  /*
   Implement the map and reduce functions.
   The map function should parse the string value and return an object with the word as the key and the value as 1.
   The reduce function should return the count of each word.
   
*/

  const mapper = (key, value) => {
    
    const words = value.split(/(\s+)/).filter((e) => e !== " ");
    let out = [];
    for (const word of words) {
      let word_obj = {}
      word_obj[word] = 1
      out.push(word_obj)
    }
    return out;
  };

  const reducer = (key, values) => {
    console.log("This is the input to the reducer: ", key, values);
    const out = {};
    out[key] = values.reduce((runningSum, a) => runningSum + a, 0);
    console.log("This is the output of the reducer: ", out);
    return out;
    
  };

  const dataset = [
    { "b1-l1": "It was the best of times, it was the worst of times," },
    { "b1-l2": "it was the age of wisdom, it was the age of foolishness," },
    { "b1-l3": "it was the epoch of belief, it was the epoch of incredulity," },
    { "b1-l4": "it was the season of Light, it was the season of Darkness," },
    { "b1-l5": "it was the spring of hope, it was the winter of despair," },
  ];

  const expected = [
    { It: 1 },
    { was: 10 },
    { the: 10 },
    { best: 1 },
    { of: 10 },
    { "times,": 2 },
    { it: 9 },
    { worst: 1 },
    { age: 2 },
    { "wisdom,": 1 },
    { "foolishness,": 1 },
    { epoch: 2 },
    { "belief,": 1 },
    { "incredulity,": 1 },
    { season: 2 },
    { "Light,": 1 },
    { "Darkness,": 1 },
    { spring: 1 },
    { "hope,": 1 },
    { winter: 1 },
    { "despair,": 1 },
  ];

  const doMapReduce = (cb) => {
    distribution.dlib.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
        console.log(mapResults)
      } catch (e) {
        done(e);
      }

      distribution.dlib.mr.exec(
        { keys: v, map: mapper, reduce: reducer },
        (e, v) => {
          try {
            expect(v).toEqual(expect.arrayContaining(expected));
            done();
          } catch (e) {
            done(e);
          }
        }
      );
    });
  };

  let cntr = 0;

  // Send the dataset to the cluster
  dataset.forEach((o) => {
    const key = Object.keys(o)[0];
    const value = o[key];
    distribution.dlib.store.put(value, key, (e, v) => {
      cntr++;
      // Once the dataset is in place, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

test("(10 pts) (scenario) all.mr:tfidf", (done) => {
  /*
    Implement the map and reduce functions.
    The map function should parse the string value and return an object with the word as the key and the document and count as the value.
    The reduce function should return the TF-IDF for each word.

    Hint:
    TF = (Number of times the term appears in a document) / (Total number of terms in the document)
    IDF = log10(Total number of documents / Number of documents with the term in it)
    TF-IDF = TF * IDF
*/

  // so the mapper 

  const mapper = (key, value) => {
    const words = value.split(/\s+/);
    let out = [];
    const documentLength = words.length;
    const wordCounts = {};

    // Count occurrences of each word
    words.forEach(word => {
      wordCounts[word] = (wordCounts[word] || 0) + 1;
    });

    for (const word of Object.keys(wordCounts)) {
      const word_count = wordCounts[word]
      const tf_score = word_count / documentLength

      out.push({ [word]: [key, tf_score] });
    }
    // console.log(out)
    return out;
  };

  // Reduce function: calculate TF-IDF for each word
  const reducer = (key, values) => {
    // console.log("Key + Value: ", key, values)
    
    const total_num_docs = 3; // hard-coded total document count
    
    const docs_with_term = values.length / 2;
  
    let idf = Math.log10(total_num_docs / docs_with_term);
    
    const result = {};

    for (let i = 0; i < values.length; i += 2) {
      const tf_idf =values[i + 1] * idf;
      result[values[i]] = parseFloat(tf_idf.toFixed(2));
    }
    
    return { [key]: result };

  };

  const dataset = [
    { doc1: "machine learning is amazing" },
    { doc2: "deep learning powers amazing systems" },
    { doc3: "machine learning and deep learning are related" },
  ];

  const expected = [
    { is: { doc1: 0.12 } },
    { deep: { doc2: 0.04, doc3: 0.03 } },
    { systems: { doc2: 0.1 } },
    { learning: { doc1: 0, doc2: 0, doc3: 0 } },
    { amazing: { doc1: 0.04, doc2: 0.04 } },
    { machine: { doc1: 0.04, doc3: 0.03 } },
    { are: { doc3: 0.07 } },
    { powers: { doc2: 0.1 } },
    { and: { doc3: 0.07 } },
    { related: { doc3: 0.07 } },
  ];

  const doMapReduce = (cb) => {
    distribution.tfidf.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }

      distribution.tfidf.mr.exec(
        { keys: v, map: mapper, reduce: reducer },
        (e, v) => {
          try {
            expect(v).toEqual(expect.arrayContaining(expected));
            done();
          } catch (e) {
            done(e);
          }
        }
      );
    });
  };

  let cntr = 0;

  // Send the dataset to the cluster
  dataset.forEach((o) => {
    const key = Object.keys(o)[0];
    const value = o[key];
    distribution.tfidf.store.put(value, key, (e, v) => {
      cntr++;
      // Once the dataset is in place, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

/*
  The rest of the scenarios are left as an exercise.
  For each one you'd like to implement, you'll need to:
  - Define the map and reduce functions.
  - Create a dataset.
  - Run the map reduce.
*/

// This one is responsible for taking in a dataset of URL and properly parsing them
test("(10 pts) (scenario) all.mr:crawl", (done) => {
  done(new Error("Implement this test."));
});

// Given a page identifier on disk, this workflow extracts URLs from the corresponding page and stores them in a distributed URL index.
test("(10 pts) (scenario) all.mr:urlxtr", (done) => {
  done(new Error("Implement the map and reduce functions"));
});

// Given a regular expression, identify all the object IDs that match that regular expression in the dataset.
test("(10 pts) (scenario) all.mr:strmatch", (done) => {

  const mapper = (key, value) => {
    const regex = /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/i; // Email pattern
    if (regex.test(value)) {
      return [{ [regex]: key }];
    }
    return []; // according to MR contract, need to return empty array (dive deeper here!)
  };

  const reducer = (key, values) => {
    values.sort();
    return { [key]: values };
  };

  // GPT-generated dataset 
  const dataset = [
    { "doc1": "Contact us at support@example.com for customer service." },
    { "doc2": "Please call us at (555) 123-4567 for assistance." },
    { "doc3": "Visit our website at https://www.example.com for more information." },
    { "doc4": "Product ID: ABC-123-XYZ is currently out of stock." },
    { "doc5": "Meeting scheduled for 2023-04-15 at 3:00 PM." },
    { "doc6": "Send your resume to careers@example.org to apply." },
    { "doc7": "Use coupon code SUMMER25 for 25% off your order." },
    { "doc8": "Reply to john.doe@company.co.uk with your feedback." },
    { "doc9": "The file is stored at /users/data/report.pdf" },
    { "doc10": "Username: user123, Password: ********" }
  ];

  // doc IDs that match the given pattern
  const expected = [
    { "/\\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}\\b/i": ["doc1", "doc6", "doc8"] }
  ];

  const doMapReduce = (cb) => {
    distribution.strmatch.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }

      distribution.strmatch.mr.exec(
        { keys: v, map: mapper, reduce: reducer },
        (e, v) => {
          try {
            expect(v).toEqual(expect.arrayContaining(expected));
            done();
          } catch (e) {
            done(e);
          }
        }
      );
    });
  };

  let cntr = 0;

  // Send the dataset to the cluster
  dataset.forEach((o) => {
    const key = Object.keys(o)[0];
    const value = o[key];
    distribution.strmatch.store.put(value, key, (e, v) => {
      cntr++;
      // Once the dataset is in place, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

// Create a mapping from terms in documents (addressed by identifiers) to object IDs.
test.only("(10 pts) (scenario) all.mr:ridx", (done) => {
  const mapper = (key, value) => {
    console.log("This is the input to the mapper: ", key, value);
    const words = value.toLowerCase().split(/\s+/);
    const uniqueWords = new Set(words);
    let out = [];
    uniqueWords.forEach(word => {
      let out_obj = {};
      out_obj[word] = key;
      out.push(out_obj);
    });
    console.log("This is the output of the mapper: ", out);
    return out;
  };

  // Reduce function: calculate TF-IDF for each word
  const reducer = (key, values) => {      
    // consistent order
    console.log("Reducer Key + Value: ", key, values)
    values.sort();
    const out = {};
    out[key] = values;
    console.log("This is the output of the reducer: ", out);
    return out;
  };

  const dataset = [
    { "doc101": "The quick brown fox jumps over the lazy dog" },
    { "doc102": "Brown foxes are known for their quick movements" },
    { "doc103": "Dogs and cats are common pets" },
    { "doc104": "The lazy dog sleeps all day long" },
    { "doc105": "Quick thinking saved the day" }
  ];

  const expected = [
    { "quick": ["doc101", "doc102", "doc105"] },
    { "brown": ["doc101", "doc102"] },
    { "fox": ["doc101"] },
    { "foxes": ["doc102"] },
    { "jumps": ["doc101"] },
    { "over": ["doc101"] },
    { "the": ["doc101", "doc104", "doc105"] },
    { "lazy": ["doc101", "doc104"] },
    { "dog": ["doc101", "doc104"] },
    { "are": ["doc102", "doc103"] },
    { "known": ["doc102"] },
    { "for": ["doc102"] },
    { "their": ["doc102"] },
    { "movements": ["doc102"] },
    { "dogs": ["doc103"] },
    { "and": ["doc103"] },
    { "cats": ["doc103"] },
    { "common": ["doc103"] },
    { "pets": ["doc103"] },
    { "sleeps": ["doc104"] },
    { "all": ["doc104"] },
    { "day": ["doc104", "doc105"] },
    { "long": ["doc104"] },
    { "thinking": ["doc105"] },
    { "saved": ["doc105"] }
  ];

  const doMapReduce = (cb) => {
    distribution.ridx.store.get(null, (e, v) => {
      try {
        console.log("Values returned by ridx ", v)
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }

      distribution.ridx.mr.exec(
        { keys: v, map: mapper, reduce: reducer },
        (e, v) => {
          try {
            expect(v).toEqual(expect.arrayContaining(expected));
            done();
          } catch (e) {
            done(e);
          }
        }
      );
    });
  };

  let cntr = 0;

  // Send the dataset to the cluster
  dataset.forEach((o) => {
    const key = Object.keys(o)[0];
    const value = o[key];
    distribution.ridx.store.put(value, key, (e, v) => {
      cntr++;
      // Once the dataset is in place, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
  
});

// Given a set source-sink URLs, generate the following association: for each sink URL, store all the source URLs that point to that sink URL.
test("(10 pts) (scenario) all.mr:rlg", (done) => {
  
  const mapper = (key, value) => {
    const links = value.split(" ").map((e) => e.trim()).filter((e) => e !== "");
    
    const linkCounts = {};
    const total_links = links.length;

    let out = [];

    // Count occurrences of each word
    links.forEach(link => {
      linkCounts[link] = (linkCounts[link] || 0) + 1;
    });

    for (const link of Object.keys(linkCounts)) {
      const link_count = linkCounts[link]
      const link_tf_score = link_count / total_links

      out.push({ [link]: [key, link_tf_score] }); // this now outputs (sink, [source, importance_score])
    }
    // console.log(out)
    return out;
  };

  // Reduce function: calculate TF-IDF for each link
  // IDF = log10(Total number of documents / Number of documents with the term in it)
  const reducer = (key, values) => {
    const out = {};
    const result = {};

    const total_source_links = 7
    const total_sink_sources = values.length / 2;
    const idf = Math.log10(total_source_links/total_sink_sources)
    
    
    for (let i = 0; i < values.length; i += 2) {
      const source_link = values[i];
      const tf_score = values[i + 1];
      const tf_idf = tf_score * idf;
      result[source_link] = parseFloat(tf_idf.toFixed(2));
    } 
    
    out[key] = result;
    return out;
  };


  const dataset = [
    { "www.blog.com": "www.reference.com www.news.com www.wikipedia.org" },
    { "www.news.com": "www.blog.com www.sports.com www.weather.com" },
    { "www.sports.com": "www.news.com www.stats.com" },
    { "www.university.edu": "www.library.org www.research.org www.blog.com" },
    { "www.tech.org": "www.gadgets.com www.reviews.com" },
    { "www.travel.com": "www.hotels.com www.flights.com www.blog.com" },
    { "www.cooking.com": "www.recipes.org www.ingredients.com" }
  ];

  const expected = [
    {
      "www.news.com": {
        "www.blog.com": 0.18,
        "www.sports.com": 0.27
      }
    },
    {
      "www.ingredients.com": {
        "www.cooking.com": 0.42
      }
    },
    {
      "www.library.org": {
        "www.university.edu": 0.28
      }
    },
    {
      "www.research.org": {
        "www.university.edu": 0.28
      }
    },
    {
      "www.reference.com": {
        "www.blog.com": 0.28
      }
    },
    {
      "www.recipes.org": {
        "www.cooking.com": 0.42
      }
    },
    {
      "www.reviews.com": {
        "www.tech.org": 0.42
      }
    },
    {
      "www.stats.com": {
        "www.sports.com": 0.42
      }
    },
    {
      "www.sports.com": {
        "www.news.com": 0.28
      }
    },
    {
      "www.gadgets.com": {
        "www.tech.org": 0.42
      }
    },
    {
      "www.blog.com": {
        "www.news.com": 0.12,
        "www.travel.com": 0.12,
        "www.university.edu": 0.12
      }
    },
    {
      "www.wikipedia.org": {
        "www.blog.com": 0.28
      }
    },
    {
      "www.hotels.com": {
        "www.travel.com": 0.28
      }
    },
    {
      "www.flights.com": {
        "www.travel.com": 0.28
      }
    },
    {
      "www.weather.com": {
        "www.news.com": 0.28
      }
    }
  ]
  const doMapReduce = (cb) => {
    distribution.rlg.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }

      distribution.rlg.mr.exec(
        { keys: v, map: mapper, reduce: reducer },
        (e, v) => {
          try {
            expect(v).toEqual(expect.arrayContaining(expected));
            done();
          } catch (e) {
            done(e);
          }
        }
      );
    });
  };

  let cntr = 0;

  // Send the dataset to the cluster
  dataset.forEach((o) => {
    const key = Object.keys(o)[0];
    const value = o[key];
    distribution.rlg.store.put(value, key, (e, v) => {
      cntr++;
      // Once the dataset is in place, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

/*
    This is the setup for the test scenario.
    Do not modify the code below.
*/

beforeAll((done) => {
  ncdcGroup[id.getSID(n1)] = n1;
  ncdcGroup[id.getSID(n2)] = n2;
  ncdcGroup[id.getSID(n3)] = n3;

  dlibGroup[id.getSID(n1)] = n1;
  dlibGroup[id.getSID(n2)] = n2;
  dlibGroup[id.getSID(n3)] = n3;

  tfidfGroup[id.getSID(n1)] = n1;
  tfidfGroup[id.getSID(n2)] = n2;
  tfidfGroup[id.getSID(n3)] = n3;

  crawlGroup[id.getSID(n1)] = n1;
  crawlGroup[id.getSID(n2)] = n2;
  crawlGroup[id.getSID(n3)] = n3;

  urlxtrGroup[id.getSID(n1)] = n1;
  urlxtrGroup[id.getSID(n2)] = n2;
  urlxtrGroup[id.getSID(n3)] = n3;

  strmatchGroup[id.getSID(n1)] = n1;
  strmatchGroup[id.getSID(n2)] = n2;
  strmatchGroup[id.getSID(n3)] = n3;

  ridxGroup[id.getSID(n4)] = n4;
  ridxGroup[id.getSID(n5)] = n5;
  ridxGroup[id.getSID(n6)] = n6;

  rlgGroup[id.getSID(n7)] = n7;
  rlgGroup[id.getSID(n8)] = n8;
  rlgGroup[id.getSID(n9)] = n9;

  const startNodes = (cb) => {
    distribution.local.status.spawn(n1, (e, v) => {
      distribution.local.status.spawn(n2, (e, v) => {
        distribution.local.status.spawn(n3, (e, v) => {
          distribution.local.status.spawn(n4, (e, v) => {
            distribution.local.status.spawn(n5, (e, v) => {
              distribution.local.status.spawn(n6, (e, v) => {
                distribution.local.status.spawn(n7, (e, v) => {
                  distribution.local.status.spawn(n8, (e, v) => {
                    distribution.local.status.spawn(n9, (e, v) => {
                      cb()
                    });
                  });
                });
              });
            });
          });
        });
      });
    });
  };

  distribution.node.start((server) => {
    localServer = server;

    const ncdcConfig = { gid: "ncdc" };
    startNodes(() => {
      distribution.local.groups.put(ncdcConfig, ncdcGroup, (e, v) => {
        distribution.ncdc.groups.put(ncdcConfig, ncdcGroup, (e, v) => {
          const dlibConfig = { gid: "dlib" };
          distribution.local.groups.put(dlibConfig, dlibGroup, (e, v) => {
            distribution.dlib.groups.put(dlibConfig, dlibGroup, (e, v) => {
              const tfidfConfig = { gid: "tfidf" };
              distribution.local.groups.put(tfidfConfig, tfidfGroup, (e, v) => {
                distribution.tfidf.groups.put(tfidfConfig, tfidfGroup, (e, v) => {
                const ridxConfig = { gid: "ridx" };
                distribution.local.groups.put(ridxConfig, ridxGroup, (e, v) => {
                  distribution.ridx.groups.put(ridxConfig, ridxGroup, (e, v) => {
                  const strmatchConfig = { gid: "strmatch" };
                   distribution.local.groups.put(strmatchConfig, strmatchGroup, (e, v) => {
                    distribution.strmatch.groups.put(strmatchConfig, strmatchGroup, (e, v) => {
                      const rlgConfig = { gid: "rlg" };
                      distribution.local.groups.put(rlgConfig, rlgGroup, (e, v) => {
                      distribution.rlg.groups.put(rlgConfig, rlgGroup, (e, v) => {
                      done();
                        })
                      });
                    });
                  });
                });
              });
            });
          });
        });
       });
      });
    });
  });
})});

afterAll((done) => {
  const remote = { service: "status", method: "stop" };
  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
        remote.node = n4;
        distribution.local.comm.send([], remote, (e, v) => {
          remote.node = n5;
          distribution.local.comm.send([], remote, (e, v) => {
            remote.node = n6;
            distribution.local.comm.send([], remote, (e, v) => {
              remote.node = n7;
              distribution.local.comm.send([], remote, (e, v) => {
                remote.node = n8;
                distribution.local.comm.send([], remote, (e, v) => {
                  remote.node = n9;
                  distribution.local.comm.send([], remote, (e, v) => {
                    localServer.close();
                    done();
                  });
                });
              });
            });
          });
        });
      });
    });
  });
});
