# M1 : Serialization / Deserialization

## Summary

In M2, I implemented basic node communication using the serialization and deserialization scheme we defined in the previous milestone. In this milestone, I added the ability to check on various status attributes about each node, find the pathways/service each node can route to, and finally wrote a function to easily facilitate this cross-node communication. This is all contained within the three files:
<ul>
<li> `local/status.js`
<li> `local/routes.js`
<li> `local/comm.js`

These three files outline the above functionality and be interacted with using the `distribution.local` path. It took me a total of 10 hours to complete M2. 

### Challenges
Some of the challenges I faced was solving was figuring how HTTP requests work and how to use the `http` library. This was hard, and I still am not sure I entirely get it. I understand that we set up a listener object, and how it should respond to each request. Within comm, we set up the functionality for each node to send a message to any other node using HTTP. However, something I am still struggling with is what happens if the node sends a message to a node that does not exist? Does it result in a 404 error? How do we properly handle this.

Another thing I had a bit of a struggle with, and this may come down to my lack of reading, but the `routes.js` file uses an implicit dicionary to map each service to its service attributes. However, from the handout, it was hard to tell that this dictionary was going to be populated for us with the necessary routes and such. I wish there was more of a breakdown of the infrastructure that is provided within the stencil code which would help alleviate my confusion.

The final challenge I had was writing the tests. I didn't realize there needed to be a server spawn and teardown function as I was kind of unaware of what infrastructure was going to be provided/what we needed to fill in. 

## Correctness
I wrote 5 extra tests located in the `m2.student.js` file. These tests cover two different edge cases for each of the service methods I implemented above. I tested different cases such as when an error should be thrown or when invalid inputs are provided. I made sure that the control flow of the program was not interrupted and that the errors were informative and gracefully handled. These takes take 2.71s to run.

## Performance
I characterized the performance of comm and RPC by sending 1000 service requests in a tight loop. Average throughput and latency is recorded in `package.json`

## Key Feature
`createRPC` is a very handy function that allows us as programmers to abstract away a lot of the complex networking that goes on behind node communication. To understand the use of `createRPC`, let's walk through an example. When you go to a restauraunt and you order a meal, you are able to order the meal without instructing the waiter on how to take your order to the kitchen, prepare it, and then they bring it out. We are able to avoid the abstraction of having to define this as we can already expect someone has set this process for us, and we can rely on it to work. This is similar to what `createRPC` does.
When a node has a service to offer, it can call `createRPC` which essentially creates a port, or mailbox, for other nodes (clients) to call this serivce. This RPC allows the client to simply call the service and not worry about detailing how to get the right answer.
As for the implementation, `createRPC` takes the client's agruments, your order, and serializes them, or writes your order down. The RPC then calls the service and deseralizes your argument,akin to delivering your order to the kitchen which prepares your food. Finally, the RPC then sends the result back, akin to the waiter returning your food. I hope that helps!