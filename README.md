A robust distributed database system implementing Multi-Paxos consensus for intra-shard transactions and Two-Phase Commit (2PC) protocol for cross-shard transactions. The system features automatic leader election, fault tolerance, dynamic resharding, and comprehensive performance benchmarking.

Architecture
Cluster Organization

3 Clusters: Each managing a shard of the account space
9 Nodes Total: 3 nodes per cluster for fault tolerance
9000 Accounts: Initially partitioned equally across clusters (3000 accounts each)

Network Topology

Nodes within a cluster communicate via Multi-Paxos for consensus
Leaders from different clusters communicate via 2PC for cross-shard coordination
Clients can connect to any node, which forwards requests to the appropriate leader

Data Distribution

Cluster 1: Accounts 1-3000 (Nodes 1, 2, 3)
Cluster 2: Accounts 3001-6000 (Nodes 4, 5, 6)
Cluster 3: Accounts 6001-9000 (Nodes 7, 8, 9)

This distribution can be dynamically optimized through the resharding feature based on transaction patterns.

Key Features
1. Strong Consistency

Linearizable consistency for all committed transactions
No stale reads or lost updates
Atomic visibility of transaction effects across all replicas

2. High Availability

Tolerates up to 1 node failure per cluster (2 out of 3 quorum)
Automatic leader election on failure detection
System continues operating with degraded but functional service

3. Fault Tolerance

Persistent storage with SQLite and Write-Ahead Logging (WAL)
Checkpoint-based recovery for fast startup
Automatic log compaction to manage storage
Graceful handling of network partitions

4. Cross-Shard Transactions

Atomic transfers between accounts in different clusters
Two-Phase Commit protocol with proper timeout handling
Deadlock prevention through distributed lock management
Automatic rollback on failures

5. Dynamic Resharding

Analyzes transaction patterns to identify hotspots
Uses METIS graph partitioning for optimal account redistribution
Minimizes cross-shard transaction overhead
Zero-downtime migration with consistent snapshots

6. Performance Optimization

Lock-based concurrency control
Batched message processing
Efficient checkpoint and compaction strategies
Configurable consistency levels for different workloads

7. Comprehensive Monitoring

Real-time performance metrics (throughput, latency)
Transaction success/failure tracking
View change history for debugging
Per-node database inspection tools


System Components
Node (node.py)
The core component implementing distributed consensus and transaction coordination.
Responsibilities:

Leader election using Multi-Paxos
Transaction execution and replication
Cross-shard coordination as coordinator or participant
Heartbeat management for failure detection
Lock acquisition and release for concurrent transactions
Checkpoint creation and log compaction

State Management:

Current ballot number (for Paxos rounds)
Current leader identification
Sequence number tracking for ordered execution
Pending proposals awaiting consensus
Active 2PC transactions with timeout tracking

Database (database.py)
Persistent storage layer with strong durability guarantees.
Features:

SQLite with WAL mode for crash recovery
Account balance storage with efficient indexing
Paxos log with sequence numbers and ballot tracking
Checkpoint snapshots for fast recovery
Lock table for concurrency control
Write-Ahead Log for transaction rollback
View change history for debugging

Operations:

Balance queries and updates
Transfer execution with atomicity
Log entry management and compaction
Lock acquisition with timeout detection
WAL-based undo for aborted transactions

Client (client.py)
Interface for submitting transactions and administrative commands.
Capabilities:

Transaction submission with automatic routing
Balance queries with cluster detection
Node failure and recovery control
CSV-based test suite execution
Interactive command menu after each test set
Performance metric collection and reporting

Test Harness:

Parses CSV files with transaction sequences
Manages node availability configurations
Supports sequential or concurrent execution modes
Tracks success/failure rates and latencies
Provides detailed per-transaction reporting

Server (server.py)
Network communication layer handling TCP connections.
Functions:

Asynchronous message handling
Connection management and multiplexing
Message serialization and deserialization
Error handling and response formatting
Connection pool for peer communication

Messages (messages.py)
Protocol definition and message construction.
Message Types:

Client requests (TRANSACTION, BALANCE, PRINT commands)
Paxos messages (PREPARE, PROMISE, ACCEPT, ACCEPTED, COMMIT)
2PC messages (PREPARE, PREPARED, COMMIT, ABORT, ACK)
Administrative messages (FAIL, RECOVER, FLUSH)
Heartbeat messages for liveness detection

Configuration (config.py)
System-wide parameters and mappings.
Settings:

Cluster and node organization
Shard mapping (account to cluster assignment)
Timeout values for elections and transactions
Checkpoint and compaction intervals
Network addresses and ports
Performance tuning parameters

Resharding (resharding.py)
Dynamic workload optimization system.
Components:
Transaction Graph Builder:

Constructs weighted graph from transaction history
Nodes represent accounts, edges represent transfers
Edge weights reflect transaction frequency

METIS Partitioner:

Uses graph partitioning algorithms to minimize edge cuts
Balances partition sizes across clusters
Minimizes cross-shard transaction percentage

Data Mover:

Coordinates safe account migration between clusters
Ensures atomicity of the resharding operation
Updates shard mappings consistently across all nodes
Resets Paxos state for clean restart

Benchmark (benchmark.py)
Configurable workload generator for performance testing.
Workload Parameters:

Read/write ratio (percentage of read-only transactions)
Cross-shard ratio (percentage spanning multiple clusters)
Data skew (Zipfian distribution for hotspot simulation)
Target throughput (transactions per second)
Concurrency level (number of parallel clients)
Test duration

Metrics:

Throughput (successful TPS)
Latency percentiles (p50, p90, p95, p99)
Success and abort rates
Transaction type breakdown (read/write, intra/cross-shard)

Consensus Protocols
Multi-Paxos for Intra-Shard Transactions
Phase 1: Leader Election

Node times out waiting for heartbeat
Increments ballot number and sends PREPARE to peers
Collects PROMISE messages with previously accepted values
Achieves majority and becomes leader

Phase 2: Normal Operation

Leader receives client request
Assigns sequence number and broadcasts ACCEPT
Collects ACCEPTED from majority of replicas
Sends COMMIT to finalize and execute transaction
Continues with same ballot for subsequent transactions (Multi-Paxos optimization)

Failure Handling:

Followers detect leader failure via heartbeat timeout
New leader elected through Phase 1
New leader reconciles log gaps using collected promises
Fills holes with NO-OP entries to maintain sequential consistency

Two-Phase Commit for Cross-Shard Transactions
Phase 1: Prepare
Coordinator (sender's cluster):

Validates sender account and balance
Acquires exclusive lock on sender
Debits sender account and writes to WAL
Runs Paxos to replicate PREPARE phase
Sends 2PC_PREPARE to participant cluster

Participant (receiver's cluster):

Validates receiver account
Acquires exclusive lock on receiver
Credits receiver account and writes to WAL
Runs Paxos to replicate PREPARE phase
Sends 2PC_PREPARED back to coordinator

Phase 2: Commit or Abort
Commit Path (both prepared successfully):

Coordinator runs Paxos for COMMIT phase
Sends 2PC_COMMIT to participant
Both sides release locks and clear WAL
Participant sends ACK
Transaction completes successfully

Abort Path (any failure):

Failed side runs Paxos for ABORT phase
Sends 2PC_ABORT to other side
Both sides undo changes using WAL
Both sides release locks and clear WAL
Transaction rolls back atomically

Timeout Handling:

Coordinator times out waiting for PREPARED: Aborts and notifies participant
Participant times out during prepare: Aborts unilaterally
Coordinator retries COMMIT messages if ACK not received
All timeouts trigger proper cleanup and lock release

Future Enhancements
Protocol Extensions

Read-only transactions without consensus
Multi-partition transactions (>2 clusters)
Snapshot isolation for better concurrency
Optimistic concurrency control

Performance Improvements

Batched Paxos for higher throughput
Parallel 2PC for independent cross-shard transactions
Speculative execution with rollback
Adaptive timeouts based on observed latency

Operational Features

Online cluster membership changes
Rolling upgrades without downtime
Backup and restore procedures
Monitoring dashboards and alerting
Automatic failure injection for testing

Workload Optimization

Predictive resharding based on forecast
Cost-based query optimization
Data placement hints from application
Multi-tier storage (hot/cold accounts)
