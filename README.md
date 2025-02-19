# M1 : Serialization / Deserialization

## Summary

My implementation for M3 builds on the previous milestones to add support for node groups and distributed services, totaling approximately 300+ new lines of code across the core local and distributed components. The milestone focuses on creating abstractions for viewing and interacting with sets of nodes as unified objects. In this milestone, I implemented 4 new software components, and modified 3 old ones:
<li> `groups` which allow for nodes to maintain membership and record of different node groups across the system. The group is both a local service, where each node has its own local view of groups and their respective memberships, and a global service that allows node groups to manage their groups. 
<li> `routes` which is both a distributed and local service which allows nodes to maintain mapping to their local services, and their group services which can be used to access the distributed services
<li> `status` is now also a distributed and local service which allows nodes to report their own status, but now groups can also obtain their status, as well as spawn nodes in the group, and kill the group.
<li> `comm` is now also a distributed service which takes distributed calls and makes a local `comm` call to each node within the group

### Challenges
Some of the key challenges I faced in this milestone managing group membership across different nodes. This is because each node is supposed to maintain a local view of groups, and these are able to differ. This resulted in a lot of issues because it was understand how the distributed systems are meant to interact with the nodes within their respective group. However, the nodes within a group aren't necessarily aware of the group they are apart of. This logic was hard to wrap my head around at first, and I still think portions of my code are a little messy. As a fix to this, I added checks to make sure that the distributed services make the proper assumptions about its nodes, and that if they are ambiguous, they are properly handled.

Another major challenge was handling the code hanging. Often times, I found my methods hanging in an infinite loop which make it really hard to debug and find the issue. When the code got stuck in a loop, it was hard to find the source of the error and would often take up a few hours alone to really break down the code. 

Finally, the last major thing was implementing the dynamic service instantiation for new groups. It was hard to understand how and where to properly instantiate the new services so that they are accessible by the `distribution` object and the `routes` table. I fixed this within the `groups.put` method where I instantiate the distribution object.

## Correctness
I wrote 5 tests in `m3.student.test.js` which span the different services I implemented in this milestone. 
### Structure
The test suite consists of multiple test cases verifying different aspects of the system:
1.	Group Membership: Ensures nodes can be added, removed, and queried correctly within groups.
2.	Communication: Validates that messages and service requests can be sent across nodes.
3.	Routing: Checks that routes are properly registered, retrieved, and deleted within the distributed system.
4.	Node Lifecycle Management: Verifies that nodes can be started and stopped without affecting overall system stability.
### Setup
Automated Testing Setup
<li>Before Each Test: The system initializes a local server and spawns test nodes to simulate a network environment.
<li>After Each Test: The system cleans up by stopping all running nodes and closing connections.
<li>Assertions: The tests use Jest to validate expected behaviors and ensure proper error handling when conditions fail.

## Performance
For performance, I wrote two test files (on in shell and one in Javascript). These were to test out the different methods we are affored to spawn new nodes. I spawn some nodes in each file and measure the time it takes to spawn the node (the `onStart` function). This was actually a bit of struggle to do with the shell script, so I implemented a nifty technique to use a temporary file which is grepped to wait for the output of the `onStart` function since it was hard to serialize the personalized callbacks. For the Javascript file, it was fairly easy, but I kept running into issues where if I tried to spawn nodes in a loop that doesn't wait, they would all default to the final port in the group of nodes (very weird issue). 
For the JavaScript file, the average latency was **77.51ms** and the average throughput came out to **5.35** nodes per second (I launched 10 nodes which took a total of 1867.97ms).
For the shell script, I spawned only spawned 5 nodes. The average latency here ended up coming to **100ms** and throughput as **9.83** nodes per second.

## Key Feature
The key advantages we are afforded from gossip protocols are much easier to grasp when we move from our trivial examples with a few nodes to systems where there are thousands of nodes operating simultaneously. In this case without a gossip protocol, if we had a message or update that was required to be shared with all of the nodes in the system, it would require the node to send n-1 messages (n = total number of nodes in system). In systems with large numbers of nodes, this broadcasting would cause large network overhead as a massive number of requests are put into the system. Furthermore, now the broadcasting node has to manage each of these n-1 connections to make sure the message was delivered, and if not, manage the subsequent re-tries. This is a naive implementation is not scalable for the large distributed systems we require today.
Using a gossip protocol, each node can choose a subset of nodes at random to share information periodically. This allows us to spread the network load across nodes, as each node is now only handling a subsect instead of broadcasting to all nodes. We also reduce the total load on the network at once as the gossip protocol occurs in rounds which means the network won't receive n-1 calls at one (at least from trying to send information). Since each node is only managing a subset, the load on the each node stays relatively constant even as the system scales to include more nodes and it is easier to handle failures and retries. 