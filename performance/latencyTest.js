// latencyTest.js
const { performance } = require('perf_hooks');
const { serialize, deserialize } = require('../distribution/util/serialization');

const NUM_ITERATIONS = 1000;

// Function to measure execution time
function measureExecutionTime(fn, input) {
    const start = performance.now();
    const result = fn(input);
    const end = performance.now();
    return { result, time: end - start };
}

function averageLatency(data) {
    let totalSerializationTime = 0;
    let totalDeserializationTime = 0;

    for (let i = 0; i < NUM_ITERATIONS; i++) {
        const { result: serializedData, time: serializationTime } = measureExecutionTime(serialize, data);
        totalSerializationTime += serializationTime;

        const { time: deserializationTime } = measureExecutionTime(deserialize, serializedData);
        totalDeserializationTime += deserializationTime;
    }

    const averageSerializationTime = totalSerializationTime / NUM_ITERATIONS;
    const averageDeserializationTime = totalDeserializationTime / NUM_ITERATIONS;

    return {
        avgSerialization: averageSerializationTime,
        avgDeserialization: averageDeserializationTime
    }
}

// Test function
function testSerializationPerformance() {

    // First we will measure the latency of serializing and deserializing the base data types (T2)
    const smallBaseData = ["Armaan Patankar", 815, true, null, undefined];

    // Second we can measure the latency of serializing functions and more complex data structures (T3)
    function add(a, b) {
        return a + b;
    }

    function fibonacci(n) {
        if (n <= 1) return n;
        return fibonacci(n - 1) + fibonacci(n - 2);
    }

    const arrowFn = (x, y) => x * y;

    const mediumData = {
        add,
        fibonacci,
        arrowFn,
        nested: {
            nestedFn: (a, b) => a - b
        }
    }

    // Third we can measure the latency of serializing and deserializing large data structures (T4)
    const largeDataTest = {
        name: "Nikos Vasilakis",
        age: 30,
        email: "nikos_vasilakis@brown.edu",
        address: {
            street: "69 Brown St",
            city: "Providence",
            state: "RI",
            zip: "02912"
        },
        hobbies: ["distributing", "systems", "coding"],
        errors: [new Error("Test error"), new Error("Test error 2")],
        metadata: Array.from({ length: 1000 }, (_, i) => `data-${i}`)
    };
    let totalBaseDataSerializationTime = 0;
    let totalBaseDataDeserializationTime = 0;
    console.log("Measuring average latency for T2...");
    for (const data of smallBaseData) {
        const { avgSerialization, avgDeserialization } = averageLatency(data);
        totalBaseDataSerializationTime += avgSerialization;
        totalBaseDataDeserializationTime += avgDeserialization;
    }
    const avgBaseDataSerializationTime = totalBaseDataSerializationTime / smallBaseData.length;
    const avgBaseDataDeserializationTime = totalBaseDataDeserializationTime / smallBaseData.length;
    console.log(`Average Serialization Time for T2: ${avgBaseDataSerializationTime.toFixed(4)} ms`);
    console.log(`Average Deserialization Time for T2: ${avgBaseDataDeserializationTime.toFixed(4)} ms`);

    // I made it pretty too :)
    console.log("--------------------------------");

    console.log("Measuring average latency for T3...");
    const { avgSerialization: avgMediumDataSerializationTime, avgDeserialization: avgMediumDataDeserializationTime } = averageLatency(mediumData);
    console.log(`Average Serialization Time for T3: ${avgMediumDataSerializationTime.toFixed(4)} ms`);
    console.log(`Average Deserialization Time for T3: ${avgMediumDataDeserializationTime.toFixed(4)} ms`);

    console.log("--------------------------------");

    console.log("Measuring average latency for a large data structure T2-T4...");
    const { avgSerialization: avgLargeDataSerializationTime, avgDeserialization: avgLargeDataDeserializationTime } = averageLatency(largeDataTest);
    console.log(`Average Serialization Time for T2-T4: ${avgLargeDataSerializationTime.toFixed(4)} ms`);
    console.log(`Average Deserialization Time for T2-T4: ${avgLargeDataDeserializationTime.toFixed(4)} ms`);

    console.log("--------------------------------");   
}

// Run the test
testSerializationPerformance();