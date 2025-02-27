# M4: Distributed Storage

## Summary
In milestone 4, I implemented a distributed key-value storage system that uses consistent hashing and rendezvous hashing to efficiently distribute data across multiple nodes in our systems. The implementation includes both the local and distributed versions of in-memory (mem) and disc (store) storage services.

For the local services, I wrote implementations that store and retrieve data either in memory using a JavaScript object or on disk using the fs module to write custom files. The distributed services build upon these and use hashing algorithms to determine which node in a group should handle a specific object based on its key.

### Key Challenges:
- Implementing proper key management to distinguish between identical keys across different node groups. In order to solve this, the local memory services maps first to the `gid` and then to the key. This same schema is used in the `store` service. These always default to the `local` group.
- Initally, I was facing some challenges with actually trying to write to write the data to the disc. I was initally confused as to how to actually implement the same schema from `mem` in `store` since now I had to create directories and they had to be unique to that node. At first, I forgot we even had access to `global.nodeConfig`, but once I figured that out and the API specifications for the `fs` module, I was able to properly implement my desired disc storage schema.
- Setting up the AWS instances to test performance at scale was insanely hard. This may have been the longest part of the assignment for me. I found it really hard to conceptualize how to actually get the nodes to communicate with each other now. I understood how to do this on my local machine, but now these requests needed to actually happen. The first big issue was figuring out how to spawn the nodes. I had to create new AWS instances that allowed for HTTP connections and a custom TCP protocol. Using this, I thought I would be able to use their public IPs so I could properly set the node configurations. Initally, I had a singular `JS` file that I was deploying and calling on all nodes. This file would do different things depending on the `nodeIdx` I provided it (i.e properly set up the distributed group). However, my big issue was that I had no way of figuring out how to actually get the nodes to communicate since everything I was trying wasn't working (i.e. each node was trying to spawn itself using its public IP). Ultimately, it was after souring on Ed I realized how to set up the nodes to properly communicate where they are spawned from the `ip: 0.0.0.0.0/0`, but they can be contacted through their public IPs. Using this, I have a local script that connects to the nodes, creates the group, and properly measures the performance of the services.

## Correctness & Performance Characterization

### *Correctness*: 
I developed 5 custom test cases to validate the system's functionality. These cases are focused on the edge cases of my implementation:
1. Local memory service test - verifying that the local memory storage works and can properly distinguish between identical keys and objects in different groups
2. Local store service test - this is very similar to the local memory service test, as it also tests to make sure the service works and distinguishes between groups
3. Consistent hashing test - this test was aimed at the verifying the consistent hash function works by validing the correct node and route 
4. Rendezvous hashing test - this is similar as the above one by ensuring proper data distribution across group3
5. Key generation test - this is just a test to ensure that when no key is provided, the auto-generated key is the sha256 hash of the object.

All tests complete in 1198 milliseconds and validate the core functionality of both local and distributed storage services.

### *Performance*:
I deployed my custom distributed system on 3 AWS nodes with special TCP protocol and measured both put and get performance for both services. As mentioned above, this was a real struggle, but in order to measure performance, my `performance/distributedMem.js` file is home to everything. First, I wrote a function to generate random strings which given a length, will randomly sample the alphabet and generate a `key`. Then using this, I populate a dictionary with each key and its corresponding value (which is also an object). I then spawn a local node that then creates the group with the three AWS nodes. Finally, I call my measure functions which properly call the corresponding functions and aggregate their results. Here is what I produced:

**`MEM`**   
    - For put I was able to achieve a throughput of 2544.53 ops/second with an average latency of 249.82ms.
    - For get I was able to achieve a throughput of 4716.98 ops/second with an average latency of 115.15ms.

**`STORE`**   
    - For put I was able to achieve a throughput of 2403.85 ops/second with an average latency of 218.96ms.
    - For get I was able to achieve a throughput of 2816.90 ops/second with an average latency of 234.88ms.

## Key Feature
The `reconf` method designed to first identify all the keys to be relocated and then relocate individual objects instead of fetching all the objects immediately and then pushing them to their corresponding locations because that would be pretty silly. As we discovered from our use of hash functions beyond the basic modulus by the size of group, we do not need necessarily need to redistribute the data within all of our nodes. These alternative hash functions, such as consistent hashing, allow for a only a subset of nodes to be affected when a new node is added or removed. Therefore, in `reconf`, the approach to first identify the keys to be relocated and then move them avoids the overhead of processing keys that are already correctly located and do not need to moved which significantly reduces network load for large systems. It also prevents network congestion by processing data in manageable batches of nodes that needs changes, allowing the system to rebalance gradually rather than experiencing a sudden spike in a large reconfiguration. Another consider is likely that by relocating objects one at a time, the system is able to more effectively tolerate errors and failures since the entire system is not being overloaded. This implementation scales much better with increasing numbers of objects and nodes as it allocates computational and network resources only where needed. Furthermore, with these alternative hash functions, there is no need to reconfigure all of the data in the system at once.