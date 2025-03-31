# M4: Distributed Storage

## Summary
In milestone 5, I implemented a custom map-reduce implementation. This is used for distirbuted execution and will be highly useful in our distributed search engine final project. This was an insnaely hard milestone for as I could not figure out where my errors were coming from. However, ultimately I think I have produced a good implementation of the map-reduce processing engine that is able to take an input map and reduce function for a dataset, and properly communicate with all worker nodes in the group to coordinate the reduction of the data. 

This assignment took me a total of 35-45 hours to complete. I took many late days on this project and found it to be incredibly challenging.

### Key Challenges:
- The role of notify in this coordination was easy to understand conceptually, but very hard to understand how to implement in code. I found myself really confused initally on how worker nodes could properly communicate back with the coordinator. I played around with ideas such as sending the coordinators ip and port with every notify, but it felt wrong and redundant. I ultiamtely realized I could use an RPC function to properly handle this which then allowed me to figure out how to properly handle cross-node communication.
- Another issue I had which was really dumb on me was I was first serializing the user provided functions and sending them to the map and reduce on each worker. However, I ultimately realzied I could just also place them on the service object I was putting on every node.
- A major issue I was facing was figuring out how to hold the user callback until the end. This was very hard because I honestly didn't really understand what was special about callbacks and what they really changed. I am honestly still a little confused, but I figured it out by just calling the user callback once in the entire function. I am still confused on the what is the scope of the variables within the service. 
- Another big issue I was having was with the append function I wrote. I kept running into serialization issues and it would result in the JSON being written incorredctly. This meant the reducer wouldn't read the values right or at all and I would get the wrong answer.
## Correctness & Performance Characterization

### Correctness

I wrote 5 comprehensive test cases testing different MapReduce workflows to ensure correctness across various data processing scenarios:

1. **Word Count**: Tests the ability to count occurrences of each word across multiple documents, validating basic mapping and aggregation.
2. **Temperature Analysis by Month**: Tests processing of structured weather data, extracting temporal information and finding maximum values per time period.
3. **Stock Price Analysis**: Tests the system's ability to handle JSON data and perform averaging calculations across multiple records for each entity.
4. **Server Log Analysis**: Tests pattern recognition within semi-structured text data, extracting and aggregating information using regular expressions.
5. **E-commerce Sales Analysis**: Tests handling of JSON data with numerical operations (multiplication and addition) and categorical grouping.

Each test validates both functional correctness (expected outputs match actual outputs) and the system's ability to handle different data formats and processing requirements.

### Performance

I characterized the performance of the WordCount workflow using a custom benchmark script that measures both latency and throughput across varying dataset sizes.

My WordCount workflow achieves an average throughput of approximately 38 documents per second with small datasets (5 documents), decreasing to 12 documents per second with larger datasets (50 documents). The average latency ranges from 131 milliseconds for small datasets to 4183 milliseconds for larger datasets.

The scalability analysis shows that latency increases roughly linearly with dataset size up to 20 documents, then increases super-linearly beyond that, indicating potential bottlenecks in the implementation when processing larger datasets. Throughput decreases by approximately 68% as dataset size increases from 5 to 50 documents, suggesting areas for optimization in handling larger workloads.

This performance characterization provides a baseline for future optimizations and allows for informed decisions about deployment constraints and capacity planning. The MapReduce implementation shows good performance for small to medium datasets but would benefit from optimization for larger data processing tasks.
