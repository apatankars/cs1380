const links = "www.reference.com www.news.com www.wikipedia.org";

const words = links.split("www.").map((e) => e.trim()).filter((e) => e !== "");

const mapper = (key, value) => {
    const links = value.split("www.").map((e) => e.trim()).filter((e) => e !== "");
    
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
    // console.log("Key + Value: ", key, values)
    const out = {};
    const result = {};

    const total_source_links = 7
    const total_sink_sources = values.length / 2;
    const idf = Math.log10(total_source_links/total_sink_sources)


    for (let i = 0; i < values.length; i += 2) {
        // const result = {}
        const source_link = values[i];
        const tf_score = values[i + 1];
        // console.log(result, source_link, tf_score)
        const tf_idf = tf_score * idf;
        result[source_link] = parseFloat(tf_idf.toFixed(2));
    } 

    // console.log(result)

    out[key] = result;
    return out;
};

const dataset = [
    { "www.blog.com": "www.tech.org www.reference.com www.news.com www.wikipedia.org" },
    { "www.news.com": "www.tech.org www.blog.com www.sports.com www.weather.com www.tech.org" },
    { "www.sports.com": "www.news.com www.stats.com" },
    { "www.university.edu": "www.tech.org www.library.org www.research.org www.blog.com" },
    { "www.tech.org": "www.gadgets.com www.reviews.com" },
    { "www.travel.com": "www.hotels.com www.flights.com www.blog.com" },
    { "www.cooking.com": "www.recipes.org www.ingredients.com" }
  ];

const key1 = "www.blog.com";
const key2 = "www.news.com";
const key3 = "www.university.edu";

let map1out = mapper(key1, dataset[0][key1]);
let map2out = mapper(key2, dataset[1][key2]);
let map3out = mapper(key3, dataset[3][key3]);

// console.log(map1out);
// console.log(map2out);
// console.log(map3out);

const redKey1 = "tech.org";
const redVal1 = ['www.blog.com', 0.25,'www.news.com', 0.4,'www.university.edu', 0.25]

let red1out = reducer(redKey1, redVal1);
console.log(red1out);


[{"flights.com": {"www.travel.com": 0.28}}, {"blog.com": {"www.news.com": 0.12, "www.travel.com": 0.12, "www.university.edu": 0.12}}, {"reference.com": {"www.blog.com": 0.28}}, {"recipes.org": {"www.cooking.com": 0.42}}, {"hotels.com": {"www.travel.com": 0.28}}, {"wikipedia.org": {"www.blog.com": 0.28}}, {"research.org": {"www.university.edu": 0.28}}, {"news.com": {"www.blog.com": 0.18, "www.sports.com": 0.27}}, {"ingredients.com": {"www.cooking.com": 0.42}}, {"gadgets.com": {"www.tech.org": 0.42}}, {"reviews.com": {"www.tech.org": 0.42}}, {"stats.com": {"www.sports.com": 0.42}}, {"sports.com": {"www.news.com": 0.28}}, {"weather.com": {"www.news.com": 0.28}}, {"library.org": {"www.university.edu": 0.28}}]