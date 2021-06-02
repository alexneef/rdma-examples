# rdma-examples
RDMA Programming Examples


### PubSub
A Sample of a Publisher/Subscriber uing RC connection and RDMA_WRITE Code

### PubSubMC
Multicast using UD Transport

### PubSub-MultiSub
This example has a single publisher that listens for connections from incoming subscribers. A new Reliable Connection
(RC) queue pair is established with each subcriber. On a regular interval the publisher will send a new message to
every subscriber in the list.