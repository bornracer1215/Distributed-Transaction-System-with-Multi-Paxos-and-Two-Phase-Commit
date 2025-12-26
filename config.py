#Configuration constants for the distributed transaction system


# CLUSTER CONFIGURATION
# Number of clusters
NUM_CLUSTERS = 3

# Nodes per cluster
NODES_PER_CLUSTER = 3

# Total nodes in system
TOTAL_NODES = NUM_CLUSTERS * NODES_PER_CLUSTER  # 9

# Shard mapping: cluster_id -> (start_account, end_account)
SHARD_MAPPING = {
    1: (1, 3000),
    2: (3001, 6000),
    3: (6001, 9000),
}

def reload_shard_mapping():
    
    global SHARD_MAPPING
    
    import importlib
    import config as config_module
    importlib.reload(config_module)
    
    SHARD_MAPPING = config_module.SHARD_MAPPING
    
    return SHARD_MAPPING

# Initial balance for all accounts
INITIAL_BALANCE = 10

# NETWORK CONFIGURATION

# Base port for nodes
BASE_PORT = 5000

# Host for all nodes (localhost for testing)
HOST = 'localhost'

# Node port mapping: node_id -> port
# Node 1-3: Cluster 1, Node 4-6: Cluster 2, Node 7-9: Cluster 3
def get_node_port(node_id):
    return BASE_PORT + node_id

def get_node_address(node_id):
    return (HOST, get_node_port(node_id))

# Cluster membership: cluster_id -> [node_ids]
CLUSTER_NODES = {
    1: [1, 2, 3],
    2: [4, 5, 6],
    3: [7, 8, 9]
}

def get_cluster_for_node(node_id):
    return ((node_id - 1) // NODES_PER_CLUSTER) + 1

def get_cluster_for_account(account_id):
    for cluster_id, shard_range in SHARD_MAPPING.items():
        if isinstance(shard_range, tuple):
            # Old format: (start, end)
            start, end = shard_range
            if start <= account_id <= end:
                return cluster_id
        elif isinstance(shard_range, list):
            # New format: [list of account_ids]
            if account_id in shard_range:
                return cluster_id
    
    if account_id <= 3000:
        return 1
    elif account_id <= 6000:
        return 2
    else:
        return 3
    
# PAXOS CONFIGURATION

# Heartbeat interval (seconds)
HEARTBEAT_INTERVAL = 0.1

# Election timeout (seconds) - miss 4 heartbeats
ELECTION_TIMEOUT = 2.0

# Client timeout for transactions (seconds)
CLIENT_TIMEOUT = 3.0

# Majority quorum size
def get_majority(cluster_size=NODES_PER_CLUSTER):
    return (cluster_size // 2) + 1

MAJORITY = get_majority()  # 2 for 3 nodes


# CHECKPOINTING CONFIGURATION

# Create checkpoint every N executed transactions
CHECKPOINT_INTERVAL = 100

# Number of log entries to keep after checkpointing
LOG_RETENTION_SIZE = 200

# Maximum number of checkpoints to keep in database
MAX_CHECKPOINTS = 5

# Enable/disable automatic log compaction after checkpoint
AUTO_COMPACT_LOG = True


# DATABASE CONFIGURATION

# Database directory
DB_DIR = 'data'

# Database file pattern
def get_db_path(node_id):
    return f"{DB_DIR}/node_{node_id}.db"

# WAL mode for SQLite
SQLITE_WAL_MODE = True


# LOGGING CONFIGURATION

# Log directory
LOG_DIR = 'logs'

# Log file pattern
def get_log_path(node_id):
    return f"{LOG_DIR}/node_{node_id}.log"

# Log level (DEBUG, INFO, WARNING, ERROR)
LOG_LEVEL = 'INFO'


# MESSAGE TYPES

# Client messages
MSG_TRANSACTION = 'TRANSACTION'
MSG_BALANCE = 'BALANCE'
MSG_FAIL = 'FAIL'
MSG_RECOVER = 'RECOVER'
MSG_PRINT_BALANCE = 'PRINT_BALANCE' 
MSG_PRINT_DB = 'PRINT_DB'
MSG_PRINT_VIEW = 'PRINT_VIEW'
MSG_PERFORMANCE = 'PERFORMANCE'

# Paxos messages
MSG_PREPARE = 'PREPARE'
MSG_PROMISE = 'PROMISE'
MSG_ACCEPT = 'ACCEPT'
MSG_ACCEPTED = 'ACCEPTED'
MSG_COMMIT = 'COMMIT'
MSG_HEARTBEAT = 'HEARTBEAT'

# 2PC messages (for later)
MSG_2PC_PREPARE = '2PC_PREPARE'
MSG_2PC_PREPARED = '2PC_PREPARED'
MSG_2PC_COMMIT = '2PC_COMMIT'
MSG_2PC_ABORT = '2PC_ABORT'
MSG_2PC_ACK = '2PC_ACK'

# Response messages
MSG_RESPONSE = 'RESPONSE'
MSG_ERROR = 'ERROR'


# CONSISTENCY LEVELS (for bonus)

CONSISTENCY_LINEARIZABLE = 'linearizable'
CONSISTENCY_SEQUENTIAL = 'sequential'
CONSISTENCY_EVENTUAL = 'eventual'

DEFAULT_CONSISTENCY = CONSISTENCY_LINEARIZABLE


# PERFORMANCE CONFIGURATION

# Target throughput per cluster (TPS)
TARGET_THROUGHPUT = 1000

# Benchmark parameters
DEFAULT_READ_WRITE_RATIO = 0.8  
DEFAULT_CROSS_SHARD_RATIO = 0.3  
DEFAULT_SKEW = 0.0  

# UTILITY FUNCTIONS

def validate_node_id(node_id):
    if not (1 <= node_id <= TOTAL_NODES):
        raise ValueError(f"Invalid node_id: {node_id}. Must be 1-{TOTAL_NODES}")
    return True

def validate_account_id(account_id):
    if not (1 <= account_id <= 9000):
        raise ValueError(f"Invalid account_id: {account_id}. Must be 1-9000")
    return True

def validate_cluster_id(cluster_id):
    if not (1 <= cluster_id <= NUM_CLUSTERS):
        raise ValueError(f"Invalid cluster_id: {cluster_id}. Must be 1-{NUM_CLUSTERS}")
    return True

def get_peers_in_cluster(node_id):
    cluster_id = get_cluster_for_node(node_id)
    peers = CLUSTER_NODES[cluster_id].copy()
    peers.remove(node_id)
    return peers


# DEBUG / TESTING

DEBUG_MODE = False

def enable_debug():
    global DEBUG_MODE, LOG_LEVEL
    DEBUG_MODE = True
    LOG_LEVEL = 'DEBUG'

def disable_debug():
    global DEBUG_MODE, LOG_LEVEL
    DEBUG_MODE = False
    LOG_LEVEL = 'INFO'