// latencyTest.js
const { performance } = require('perf_hooks');
const { serialize, deserialize } = require('../distribution/util/serialization');

// Function to measure execution time
function measureExecutionTime(fn, input) {
    const start = performance.now();
    const result = fn(input);
    const end = performance.now();
    return { result, time: end - start };
}

// Test function
function testSerializationPerformance() {
    const testData = {
        name: "John Doe",
        age: 30,
        email: "johndoe@example.com",
        address: {
            street: "123 Main St",
            city: "New York",
            state: "NY",
            zip: "10001"
        },
        hobbies: ["reading", "hiking", "coding"],
        metadata: Array.from({ length: 1000 }, (_, i) => `data-${i}`)
    };

    console.log("Measuring serialization latency...");
    const { result: serializedData, time: serializationTime } = measureExecutionTime(serialize, testData);
    console.log(`Serialization Time: ${serializationTime.toFixed(4)} ms`);

    console.log("Measuring deserialization latency...");
    const { result: _, time: deserializationTime } = measureExecutionTime(deserialize, serializedData);
    console.log(`Deserialization Time: ${deserializationTime.toFixed(4)} ms`);
}

// Run the test
testSerializationPerformance();