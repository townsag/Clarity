# Initial Thoughts:
- I think we should focus on just Real-Time collaborative text editing instead of building a fully featured jupyter notebook
    - This way we can focus on the user experience features like highly available realtime editing 
    - I am concerned that implementing a Jupyter notebook from scratch might be too much work for just the next ~2 months
    - I think the task of either syncing concurrent runtimes or managing a global runtime state would be very difficult
    - this is just my thoughts on it, I'm very willing to go with the groups decision
## Baseline:
- "The Art of the Fugue: Minimizing Interleaving in Collaborative Text Editing" This paper defines Conflict Free Replicated Data Types. These are a data structure that you can use to store a string of text for which concurrent updates to the string of text are concurrent and idempotent
    - this means that multiple users can work on documents in isolation and them send their edit history to each other. The datatype takes care of merging the two edit histories together in such a way that preserves the independent order of the concurrent edits
    - our message passing code can be pretty simple because the same edit processed twice will not corrupt the data by default
    - I am relatively confident that I can implement the List CRDT in typescript on the frontend so that in the browser the user can pull the edit history of other users and merge them with their own code
    - specifically for editing documents, this means that we don't need a "leader"
- how CRDTs work
    - List CRDT uses a tree structure underneath the hood to represent the document contents. 
    - Concurrent updates to the same "index" in the document text are actually added to the tree structure as sibling nodes. The user_id is used as the tie breaker for ordering of sibling nodes
    - we can get the human-readable document contents from the tree structure by DFS in order traversal of the tree
    - each local copy of a CRDT maintains a "version-vector" which allows them to specify to a central server or a peer which operations they are missing. 
        - This is a list of tuples like:
            - [(user_id), (operation_number)]
            - with each tuple representing the user_id of a user and the most recent sequential operation they have received from that user
        - when requesting recent operations from the server/peer the version-vector can be used to infer which operations the server/peer actually needs to send over the websocket
        - for each user_id that is editing the document, the server/peer sends all the operations with operation_numbers greater than the number in the version-vector
        - this is a lot like tcp acks but instead we send acks for multiple streams of messages
            - I wonder if this+CRDT would count as one of our three required algorithms


## Architecture Choices: Peer to Peer vs Centralized
### Peer to Peer:
- dataflow
    - when users what to start an editing session, they create a new document_id and register with a signalling server
        - the signalling server keeps track of the live documents and the IP addresses of the users editing each document
    - all future users that want to edit that document contact the signalling server and receive the IP addresses of the other users editing that document 
    - code running in the users browser maintains WebRTC connections with the other users editing a document
    - users send insert() and delete() messages to each other over the WebRTC connections
    - all edit conflicts are resolved by the CRDT objects commutativity and idempotence properties
- notable properties
    - No need to store any document history on the signalling server
        - very secure
    - cannot access documents that you have not edited before if none of the peers holding that document are online
    - susceptible to hard networking problems
        - lost connection retry
    - does not scale well to more that a few users because of overhead of maintaining many realtime connections
        - unless we implement more sophisticated peer network
        - I was missing for the peer-2-peer lecture so I don't know much about this
    - lots of client side code for maintaining peer connections
### Centralized Architecture
- dataflow:
    - when a user wants to create a document, they register with the connection server their user_id and document_id 
        - an entry for the document is created in the database
        - the connection server maintains a table of user_id's for each document, representing the users that are concurrently accessing a document
    - the application server maintains a number of web-socket connections, each to a different user that is accessing a document
    - when a user makes an edit to the local version of the document:
        - the edit is optimistically added to their local CRDT tree and the UI
        - they sends an operation message over the web-socket to the application server
    - the application server receives the operation message, it sends the message a message broker
    - worker processes dequeue new insert and delete operations from the message queue and process them:
        - write operations to the operation log in the database
        - update internal state of CRDT to be consistent with the new operations
        - add processed operations to the outgoing message broker
    - the application server polls operations from the outgoing message queue and sends them over the web socket connections to the relevant clients
    - clients incorporate operations from other clients sent by web sockets into their own state
- properties:
    - central state
        - less secure because site hosting company has access to user document state
        - potential single point of failure
        - network bound by connection between client and application server instead of network bound by connections between peers
        - any user always has access to the most recent version all the documents they have editor privileges on, even if the other clients that own that document are offline
            - document discovery is also much easier to implement
    - write more backend code than frontend code