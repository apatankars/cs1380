/*
    In this file, add your own test cases that correspond to functionality introduced for each milestone.
    You should fill out each test case so it adequately tests the functionality you implemented.
    You are left to decide what the complexity of each test case should be, but trivial test cases that abuse this flexibility might be subject to deductions.

    Imporant: Do not modify any of the test headers (i.e., the test('header', ...) part). Doing so will result in grading penalties.
*/

const distribution = require('../../config.js');
const id = distribution.util.id;

// Define node groups for each test
const wordcountGroup = {};
const tempmonthGroup = {};
const stockanalysisGroup = {};
const loganalysisGroup = {};
const ecommerceGroup = {};

/*
    The local node will be the orchestrator.
*/
let localServer = null;

const n1 = {ip: '127.0.0.1', port: 7110};
const n2 = {ip: '127.0.0.1', port: 7111};
const n3 = {ip: '127.0.0.1', port: 7112};


test('(1 pts) student test', (done) => {
  // Count occurrences of each word across documents
  const mapper = (key, value) => {
    const words = value.toLowerCase().split(/\s+/).filter(word => word.length > 0);
    const out = [];
    
    words.forEach(word => {
      // Remove punctuation
      const cleanWord = word.replace(/[^\w]/g, '');
      if (cleanWord.length > 0) {
        const o = {};
        o[cleanWord] = 1;
        out.push(o);
      }
    });
    
    return out;
  };

  const reducer = (key, values) => {
    const out = {};
    out[key] = values.reduce((sum, v) => sum + v, 0);
    return out;
  };

  const dataset = [
    {'doc1': 'The quick brown fox jumps over the lazy dog'},
    {'doc2': 'A fox is quick and brown, the dog is lazy'},
    {'doc3': 'Every fox is different, but that one was quick'},
  ];

  const expected = [
    {'the': 3}, {'quick': 3}, {'brown': 2}, {'fox': 3}, 
    {'jumps': 1}, {'over': 1}, {'lazy': 2}, {'dog': 2},
    {'a': 1}, {'is': 3}, {'and': 1}, {'every': 1},
    {'different': 1}, {'but': 1}, {'that': 1}, {'one': 1}, {'was': 1}
  ];

  const doMapReduce = (cb) => {
    distribution.wordcount.mr.exec({keys: getDatasetKeys(dataset), map: mapper, reduce: reducer}, (e, v) => {
      try {
        expect(v).toEqual(expect.arrayContaining(expected));
        done();
      } catch (e) {
        done(e);
      }
    });
  };

  let cntr = 0;

  dataset.forEach((o) => {
    const key = Object.keys(o)[0];
    const value = o[key];
    distribution.wordcount.store.put(value, key, (e, v) => {
      cntr++;
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});


test('(1 pts) student test', (done) => {
  // Find maximum temperature by month from weather data
  const mapper = (key, value) => {
    const fields = value.split(/\s+/);
    const year = fields[1];
    // Extract month from date code (positions 5-6 in field 2)
    const month = fields[2].substring(0, 2);
    // Extract temperature (remove leading + if present)
    const temp = parseInt(fields[3].replace('+', ''));
    
    const out = {};
    out[`${year}-${month}`] = temp;
    
    return [out];
  };

  const reducer = (key, values) => {
    const out = {};
    out[key] = values.reduce((max, temp) => Math.max(max, temp), -Infinity);
    return out;
  };

  const dataset = [
    {'001': '006701199099999 1950 0515070049999999N9 +0000 1+9999'},
    {'002': '004301199099999 1950 0515120049999999N9 +0022 1+9999'},
    {'003': '004301199099999 1950 0615180049999999N9 +0035 1+9999'},
    {'004': '004301265099999 1949 0324120040500001N9 +0111 1+9999'},
    {'005': '004301265099999 1949 0324180040500001N9 +0078 1+9999'},
    {'006': '004301265099999 1949 0724180040500001N9 +0128 1+9999'},
  ];

  const expected = [
    {'1950-05': 22}, {'1950-06': 35}, {'1949-03': 111}, {'1949-07': 128}
  ];

  const doMapReduce = (cb) => {
    distribution.tempmonth.mr.exec({keys: getDatasetKeys(dataset), map: mapper, reduce: reducer}, (e, v) => {
      try {
        expect(v).toEqual(expect.arrayContaining(expected));
        done();
      } catch (e) {
        done(e);
      }
    });
  };

  let cntr = 0;

  dataset.forEach((o) => {
    const key = Object.keys(o)[0];
    const value = o[key];
    distribution.tempmonth.store.put(value, key, (e, v) => {
      cntr++;
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});


test('(1 pts) student test', (done) => {
  // Calculate average stock price by company
  const mapper = (key, value) => {
    const data = JSON.parse(value);
    const out = {};
    out[data.company] = {
      sum: data.price,
      count: 1
    };
    return [out];
  };

  const reducer = (key, values) => {
    const totalSum = values.reduce((sum, value) => sum + value.sum, 0);
    const totalCount = values.reduce((count, value) => count + value.count, 0);
    const average = parseFloat((totalSum / totalCount).toFixed(2));
    
    const out = {};
    out[key] = average;
    return out;
  };

  const dataset = [
    {'stock1': '{"company":"AAPL","date":"2023-01-05","price":130.73}'},
    {'stock2': '{"company":"AAPL","date":"2023-01-06","price":129.62}'},
    {'stock3': '{"company":"MSFT","date":"2023-01-05","price":229.10}'},
    {'stock4': '{"company":"MSFT","date":"2023-01-06","price":224.93}'},
    {'stock5': '{"company":"GOOG","date":"2023-01-05","price":88.23}'},
    {'stock6': '{"company":"GOOG","date":"2023-01-06","price":89.87}'},
    {'stock7': '{"company":"AAPL","date":"2023-01-07","price":133.41}'},
  ];

  const expected = [
    {'AAPL': 131.25}, {'MSFT': 227.01}, {'GOOG': 89.05}
  ];

  const doMapReduce = (cb) => {
    distribution.stockanalysis.mr.exec({keys: getDatasetKeys(dataset), map: mapper, reduce: reducer}, (e, v) => {
      try {
        expect(v).toEqual(expect.arrayContaining(expected));
        done();
      } catch (e) {
        done(e);
      }
    });
  };

  let cntr = 0;

  dataset.forEach((o) => {
    const key = Object.keys(o)[0];
    const value = o[key];
    distribution.stockanalysis.store.put(value, key, (e, v) => {
      cntr++;
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});


test('(1 pts) student test', (done) => {
  // Count HTTP status codes from server logs
  const mapper = (key, value) => {
    // Parse log entry format: IP - - [timestamp] "METHOD /path HTTP/1.1" STATUS SIZE
    const statusCodeMatch = value.match(/"[^"]*"\s(\d{3})\s/);
    if (statusCodeMatch && statusCodeMatch[1]) {
      const statusCode = statusCodeMatch[1];
      const out = {};
      out[statusCode] = 1;
      return [out];
    }
    return [];
  };

  const reducer = (key, values) => {
    const out = {};
    out[key] = values.reduce((sum, v) => sum + v, 0);
    return out;
  };

  const dataset = [
    {'log1': '192.168.1.1 - - [01/Jan/2023:12:34:56 +0000] "GET /index.html HTTP/1.1" 200 2326'},
    {'log2': '192.168.1.2 - - [01/Jan/2023:12:35:21 +0000] "GET /images/logo.png HTTP/1.1" 200 4532'},
    {'log3': '192.168.1.3 - - [01/Jan/2023:12:36:45 +0000] "GET /nonexistent.html HTTP/1.1" 404 1024'},
    {'log4': '192.168.1.4 - - [01/Jan/2023:12:37:12 +0000] "POST /api/login HTTP/1.1" 401 543'},
    {'log5': '192.168.1.5 - - [01/Jan/2023:12:38:03 +0000] "GET /about.html HTTP/1.1" 200 3241'},
    {'log6': '192.168.1.6 - - [01/Jan/2023:12:39:27 +0000] "POST /api/data HTTP/1.1" 500 872'},
    {'log7': '192.168.1.7 - - [01/Jan/2023:12:40:59 +0000] "GET /contact.html HTTP/1.1" 200 1978'},
    {'log8': '192.168.1.8 - - [01/Jan/2023:12:41:33 +0000] "GET /old-page.html HTTP/1.1" 301 321'},
  ];

  const expected = [
    {'200': 4}, {'404': 1}, {'401': 1}, {'500': 1}, {'301': 1}
  ];

  const doMapReduce = (cb) => {
    distribution.loganalysis.mr.exec({keys: getDatasetKeys(dataset), map: mapper, reduce: reducer}, (e, v) => {
      try {
        expect(v).toEqual(expect.arrayContaining(expected));
        done();
      } catch (e) {
        done(e);
      }
    });
  };

  let cntr = 0;

  dataset.forEach((o) => {
    const key = Object.keys(o)[0];
    const value = o[key];
    distribution.loganalysis.store.put(value, key, (e, v) => {
      cntr++;
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});


test('(1 pts) student test', (done) => {
  // Calculate total sales by product category
  const mapper = (key, value) => {
    const sale = JSON.parse(value);
    const out = {};
    out[sale.category] = sale.price * sale.quantity;
    return [out];
  };

  const reducer = (key, values) => {
    const out = {};
    out[key] = parseFloat(values.reduce((sum, v) => sum + v, 0).toFixed(2));
    return out;
  };

  const dataset = [
    {'sale1': '{"id":"12345","category":"Electronics","product":"Smartphone","price":599.99,"quantity":1}'},
    {'sale2': '{"id":"12346","category":"Electronics","product":"Headphones","price":149.99,"quantity":2}'},
    {'sale3': '{"id":"12347","category":"Clothing","product":"T-Shirt","price":24.99,"quantity":3}'},
    {'sale4': '{"id":"12348","category":"Books","product":"Novel","price":15.99,"quantity":2}'},
    {'sale5': '{"id":"12349","category":"Electronics","product":"Tablet","price":349.99,"quantity":1}'},
    {'sale6': '{"id":"12350","category":"Clothing","product":"Jeans","price":49.99,"quantity":1}'},
    {'sale7': '{"id":"12351","category":"Books","product":"Cookbook","price":29.99,"quantity":1}'},
    {'sale8': '{"id":"12352","category":"Electronics","product":"Smartwatch","price":199.99,"quantity":1}'},
  ];

  const expected = [
    {'Electronics': 1449.95}, {'Clothing': 124.96}, {'Books': 61.97}
  ];

  const doMapReduce = (cb) => {
    distribution.ecommerce.mr.exec({keys: getDatasetKeys(dataset), map: mapper, reduce: reducer}, (e, v) => {
      try {
        expect(v).toEqual(expect.arrayContaining(expected));
        done();
      } catch (e) {
        done(e);
      }
    });
  };

  let cntr = 0;

  dataset.forEach((o) => {
    const key = Object.keys(o)[0];
    const value = o[key];
    distribution.ecommerce.store.put(value, key, (e, v) => {
      cntr++;
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

/*
    Test setup and teardown
*/

// Helper function to extract keys from dataset (in case the get(null) funnctionality has not been implemented)
function getDatasetKeys(dataset) {
  return dataset.map((o) => Object.keys(o)[0]);
}

beforeAll((done) => {
  
  // Set up new groups for our tests
  wordcountGroup[id.getSID(n1)] = n1;
  wordcountGroup[id.getSID(n2)] = n2;
  wordcountGroup[id.getSID(n3)] = n3;
  
  tempmonthGroup[id.getSID(n1)] = n1;
  tempmonthGroup[id.getSID(n2)] = n2;
  tempmonthGroup[id.getSID(n3)] = n3;
  
  stockanalysisGroup[id.getSID(n1)] = n1;
  stockanalysisGroup[id.getSID(n2)] = n2;
  stockanalysisGroup[id.getSID(n3)] = n3;
  
  loganalysisGroup[id.getSID(n1)] = n1;
  loganalysisGroup[id.getSID(n2)] = n2;
  loganalysisGroup[id.getSID(n3)] = n3;
  
  ecommerceGroup[id.getSID(n1)] = n1;
  ecommerceGroup[id.getSID(n2)] = n2;
  ecommerceGroup[id.getSID(n3)] = n3;

  const startNodes = (cb) => {
    distribution.local.status.spawn(n1, (e, v) => {
      distribution.local.status.spawn(n2, (e, v) => {
        distribution.local.status.spawn(n3, (e, v) => {
          cb();
        });
      });
    });
  };

  distribution.node.start((server) => {
    localServer = server;

    const wordcountConfig = {gid: 'wordcount'};
    startNodes(() => {
      // Set up new test configurations
      distribution.local.groups.put(wordcountConfig, wordcountGroup, (e, v) => {
        distribution.wordcount.groups.put(wordcountConfig, wordcountGroup, (e, v) => {
          const tempmonthConfig = {gid: 'tempmonth'};
          distribution.local.groups.put(tempmonthConfig, tempmonthGroup, (e, v) => {
            distribution.tempmonth.groups.put(tempmonthConfig, tempmonthGroup, (e, v) => {
              const stockanalysisConfig = {gid: 'stockanalysis'};
              distribution.local.groups.put(stockanalysisConfig, stockanalysisGroup, (e, v) => {
                distribution.stockanalysis.groups.put(stockanalysisConfig, stockanalysisGroup, (e, v) => {
                  const loganalysisConfig = {gid: 'loganalysis'};
                  distribution.local.groups.put(loganalysisConfig, loganalysisGroup, (e, v) => {
                    distribution.loganalysis.groups.put(loganalysisConfig, loganalysisGroup, (e, v) => {
                      const ecommerceConfig = {gid: 'ecommerce'};
                      distribution.local.groups.put(ecommerceConfig, ecommerceGroup, (e, v) => {
                        distribution.ecommerce.groups.put(ecommerceConfig, ecommerceGroup, (e, v) => {
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
    });
  });
});

afterAll((done) => {
  const remote = {service: 'status', method: 'stop'};
  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
        localServer.close();
        done();
      });
    });
  });
});