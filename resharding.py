#Shard Redistribution System using Graph Partitioning

import asyncio
import time
import sqlite3
import shutil
import os
from typing import Dict, List, Optional, Tuple, Set, Any
from collections import defaultdict
import networkx as nx


try:
    import metis
    METIS_AVAILABLE = True
except (ImportError, RuntimeError) as e:
    METIS_AVAILABLE = False
    metis = None  
    #print(f"WARNING: METIS not available. Resharding will use greedy algorithm.")
    #print(f"  Reason: {e}")

from config import *


class TransactionGraph:
    
    def __init__(self):
        self.graph = nx.Graph()
        self.transaction_count = 0
        self.account_set = set()
    
    def add_transaction(self, sender: int, receiver: int):
        self.transaction_count += 1
        self.account_set.add(sender)
        self.account_set.add(receiver)
        
        if not self.graph.has_node(sender):
            self.graph.add_node(sender)
        if not self.graph.has_node(receiver):
            self.graph.add_node(receiver)
        
        if self.graph.has_edge(sender, receiver):
            self.graph[sender][receiver]['weight'] += 1
        else:
            self.graph.add_edge(sender, receiver, weight=1)
    
    def get_statistics(self) -> Dict[str, Any]:
        return {
            'num_accounts': len(self.account_set),
            'num_transactions': self.transaction_count,
            'num_edges': self.graph.number_of_edges(),
            'avg_degree': sum(dict(self.graph.degree()).values()) / max(1, len(self.account_set))
        }


class GraphPartitioner:
    
    def __init__(self, num_partitions: int = NUM_CLUSTERS):
        self.num_partitions = num_partitions
    
    def partition_with_metis(self, graph: nx.Graph) -> Dict[int, int]:
        if not METIS_AVAILABLE or metis is None:
            #print("METIS not available, falling back to greedy algorithm...")
            return self.partition_greedy(graph)
        
        if len(graph.nodes()) == 0:
            return {}
        
        node_list = list(graph.nodes())
        node_to_idx = {node: idx for idx, node in enumerate(node_list)}
        
        adjacency = []
        edge_weights = []
        
        for node in node_list:
            neighbors = []
            weights = []
            
            for neighbor in graph.neighbors(node):
                neighbors.append(node_to_idx[neighbor])
                weights.append(graph[node][neighbor].get('weight', 1))
            
            adjacency.append(neighbors)
            edge_weights.append(weights)
        
        try:
            edgecuts, partition_array = metis.part_graph(
                adjacency,
                nparts=self.num_partitions,
                eweights=edge_weights,
                recursive=True
            )
            
            partitions = {node_list[idx]: partition_id 
                         for idx, partition_id in enumerate(partition_array)}
            
            return partitions
        
        except Exception as e:
            #print(f"METIS partitioning failed: {e}")
            #print("Falling back to greedy algorithm...")
            return self.partition_greedy(graph)
    
    def partition_greedy(self, graph: nx.Graph) -> Dict[int, int]:
        nodes = list(graph.nodes())
        if not nodes:
            return {}
        
        #print(f"  Using minimal-movement algorithm with {len(nodes)} hot accounts")
        
        components = list(nx.connected_components(graph))
        
        #print(f"  Found {len(components)} connected components")
        for i, comp in enumerate(components[:5]):  # Show first 5
            print(f"    Component {i+1}: {len(comp)} nodes")
        
        components = sorted(components, key=len, reverse=True)
        
        partitions = {}
        
        for comp_idx, component in enumerate(components):
            component_list = list(component)
            
            current_distribution = {0: 0, 1: 0, 2: 0}  
            
            for account_id in component_list:
                if account_id <= 3000:
                    current_cluster = 1
                elif account_id <= 6000:
                    current_cluster = 2
                else:
                    current_cluster = 3
                
                partition_id = current_cluster - 1
                current_distribution[partition_id] += 1
            
            best_partition = max(current_distribution, key=current_distribution.get)
            
            for account_id in component_list:
                partitions[account_id] = best_partition
            
            moved = len(component_list) - current_distribution[best_partition]
            print(f"    Component {comp_idx+1}: {len(component_list)} accounts -> Partition {best_partition} (moving {moved} accounts)")
        
        print(f"\n  Total hot accounts to reassign: {len(partitions)}")
        
        return partitions
    
    def _calculate_node_cost(self, graph: nx.Graph, node: int, 
                           partitions: Dict[int, int]) -> int:
        node_partition = partitions[node]
        cost = 0
        
        for neighbor in graph.neighbors(node):
            if partitions[neighbor] != node_partition:
                cost += graph[node][neighbor].get('weight', 1)
        
        return cost


class ShardMapper:
    def __init__(self, current_mapping: Dict[int, Tuple[int, int]]):
        self.current_mapping = current_mapping
        self.current_account_to_cluster = self._build_account_map(current_mapping)
    
    def _build_account_map(self, mapping: Dict[int, Tuple[int, int]]) -> Dict[int, int]:
        account_map = {}
        for cluster_id, (start, end) in mapping.items():
            for account_id in range(start, end + 1):
                account_map[account_id] = cluster_id
        return account_map
    
    def create_new_mapping(self, partitions: Dict[int, int]) -> Dict[int, List[int]]:
        new_mapping = {cluster_id: [] for cluster_id in range(1, NUM_CLUSTERS + 1)}
        
        for account_id in range(1, 9001):
            if account_id in partitions:
                # Hot account - use NEW partition assignment
                partition_id = partitions[account_id]  
                cluster_id = partition_id + 1  
            else:
                # Cold account - keep in CURRENT cluster (no movement!)
                cluster_id = self.current_account_to_cluster.get(account_id, 1)
            
            new_mapping[cluster_id].append(account_id)
        
        # Sort for cleaner output
        for cluster_id in new_mapping:
            new_mapping[cluster_id].sort()
        
        # Verify all accounts are present
        total_accounts = sum(len(accounts) for accounts in new_mapping.values())
        if total_accounts != 9000:
            print(f"WARNING: Only mapped {total_accounts}/9000 accounts!")
        
        print(f"\n  New distribution:")
        for cluster_id in sorted(new_mapping.keys()):
            print(f"    Cluster {cluster_id}: {len(new_mapping[cluster_id])} accounts")
        
        return new_mapping
    
    def calculate_movements(self, new_mapping: Dict[int, List[int]]) -> Dict[int, Tuple[int, int]]:
        movements = {}
        
        for to_cluster, accounts in new_mapping.items():
            for account_id in accounts:
                from_cluster = self.current_account_to_cluster[account_id]
                
                if from_cluster != to_cluster:
                    movements[account_id] = (from_cluster, to_cluster)
        
        return movements
    
    def calculate_cross_shard_percentage(self, transactions: List[Tuple[int, int]], 
                                        mapping: Dict[int, List[int]]) -> float:
        if not transactions:
            return 0.0
        
        account_to_cluster = {}
        for cluster_id, accounts in mapping.items():
            for account_id in accounts:
                account_to_cluster[account_id] = cluster_id
        
        cross_shard = 0
        for sender, receiver in transactions:
            sender_cluster = account_to_cluster.get(sender, 1)
            receiver_cluster = account_to_cluster.get(receiver, 1)
            
            if sender_cluster != receiver_cluster:
                cross_shard += 1
        
        return cross_shard / len(transactions) * 100


class DataMover:
    
    def __init__(self):
        self.moved_count = 0
    
    def read_account_from_db(self, cluster_id: int, account_id: int) -> Optional[int]:
        node_id = CLUSTER_NODES[cluster_id][0]
        db_path = get_db_path(node_id)
        
        if not os.path.exists(db_path):
            return None
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT balance FROM accounts WHERE id = ?', (account_id,))
        row = cursor.fetchone()
        
        conn.close()
        
        return row[0] if row else None
    
    def write_account_to_db(self, cluster_id: int, account_id: int, balance: int):
        for node_id in CLUSTER_NODES[cluster_id]:
            db_path = get_db_path(node_id)
            
            if not os.path.exists(db_path):
                continue
            
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO accounts (id, balance)
                VALUES (?, ?)
            ''', (account_id, balance))
            
            conn.commit()
            conn.close()
    
    def delete_account_from_db(self, cluster_id: int, account_id: int):
        for node_id in CLUSTER_NODES[cluster_id]:
            db_path = get_db_path(node_id)
            
            if not os.path.exists(db_path):
                continue
            
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            cursor.execute('DELETE FROM accounts WHERE id = ?', (account_id,))
            
            conn.commit()
            conn.close()
    
    def move_accounts(self, movements: Dict[int, Tuple[int, int]]) -> int:
        self.moved_count = 0
        
        for account_id, (from_cluster, to_cluster) in movements.items():
            balance = self.read_account_from_db(from_cluster, account_id)
            
            if balance is None:
                print(f"WARNING: Account {account_id} not found in cluster {from_cluster}")
                balance = INITIAL_BALANCE  
            self.write_account_to_db(to_cluster, account_id, balance)
            
            self.delete_account_from_db(from_cluster, account_id)
            
            self.moved_count += 1
            
            if self.moved_count % 100 == 0:
                print(f"  Moved {self.moved_count}/{len(movements)} accounts...")
        
        return self.moved_count


class ReshardingController:
    
    def __init__(self):
        self.graph = TransactionGraph()
        self.partitioner = GraphPartitioner()
        self.mapper = ShardMapper(SHARD_MAPPING)
        self.mover = DataMover()
    
    def analyze_transactions(self, transactions: List[Dict[str, Any]]):
        for txn in transactions:
            if txn['type'] == 'transfer':
                self.graph.add_transaction(txn['sender'], txn['receiver'])
    
    def partition_and_analyze(self) -> Tuple[Dict[int, List[int]], Dict[str, Any]]:
        print("\n" + "="*60)
        print("Graph Partitioning Analysis")
        print("="*60)
        
        # Get graph statistics
        stats = self.graph.get_statistics()
        print(f"\nTransaction Graph Statistics:")
        print(f"  Unique accounts: {stats['num_accounts']}")
        print(f"  Total transactions: {stats['num_transactions']}")
        print(f"  Graph edges: {stats['num_edges']}")
        print(f"  Avg degree: {stats['avg_degree']:.2f}")
        
        if stats['num_accounts'] == 0:
            print("\nNo accounts in transaction graph - nothing to partition!")
            return {}, {}
        
        # Run partitioning
        print(f"\nRunning METIS graph partitioning...")
        print(f"  Target: {NUM_CLUSTERS} balanced partitions")
        print(f"  Objective: Minimize cross-partition edges")
        
        partitions = self.partitioner.partition_with_metis(self.graph.graph)
        
        new_mapping = self.mapper.create_new_mapping(partitions)
        
        partition_sizes = {cluster_id: len(accounts) 
                          for cluster_id, accounts in new_mapping.items()}
        
        print(f"\nPartition Sizes:")
        for cluster_id in sorted(partition_sizes.keys()):
            print(f"  Cluster {cluster_id}: {partition_sizes[cluster_id]} accounts")

        movements = self.mapper.calculate_movements(new_mapping)
        
        movement_summary = defaultdict(int)
        for _, (from_c, to_c) in movements.items():
            movement_summary[f"C{from_c}→C{to_c}"] += 1
        
        print(f"\nAccount Movements Required:")
        if movements:
            for route, count in sorted(movement_summary.items()):
                print(f"  {route}: {count} accounts")
            print(f"  Total: {len(movements)} accounts to move")
        else:
            print(f"  No movements needed!")
        
        transactions_list = [
            (s, r) for s, r in 
            [(edge[0], edge[1]) for edge in self.graph.graph.edges()]
        ]
    
        current_mapping_list = {
            cluster_id: list(range(start, end + 1))
            for cluster_id, (start, end) in SHARD_MAPPING.items()
        }
        
        old_pct = self.mapper.calculate_cross_shard_percentage(
            transactions_list, current_mapping_list
        )
        new_pct = self.mapper.calculate_cross_shard_percentage(
            transactions_list, new_mapping
        )
        
        improvement = old_pct - new_pct
        
        print(f"\nCross-Shard Transaction Analysis:")
        print(f"  Current mapping: {old_pct:.1f}% cross-shard")
        print(f"  New mapping:     {new_pct:.1f}% cross-shard")
        
        if improvement > 0:
            print(f"  Improvement:     ↓ {improvement:.1f}% reduction 🎉")
        elif improvement < 0:
            print(f"  Change:          ↑ {abs(improvement):.1f}% increase ⚠️")
        else:
            print(f"  No change in cross-shard percentage")
        
        analysis = {
            'partition_sizes': partition_sizes,
            'movements': movements,
            'movement_count': len(movements),
            'old_cross_shard_pct': old_pct,
            'new_cross_shard_pct': new_pct,
            'improvement_pct': improvement
        }
        
        return new_mapping, analysis
    
    async def execute_resharding(self, new_mapping: Dict[int, List[int]], 
                                movements: Dict[int, Tuple[int, int]],
                                node_stop_callback=None,
                                node_start_callback=None):
        print("\n" + "="*60)
        print("Executing Resharding")
        print("="*60)
        
        print("\n[1/6] Stopping all nodes...")
        if node_stop_callback:
            await node_stop_callback()
        else:
            print("  (No stop callback provided - assuming nodes are stopped)")
        
        await asyncio.sleep(2)
        print("All nodes stopped")
        
        print("\n[2/6] Backing up databases...")
        backup_dir = f"data_backup_{int(time.time())}"
        os.makedirs(backup_dir, exist_ok=True)
        
        for node_id in range(1, TOTAL_NODES + 1):
            db_path = get_db_path(node_id)
            if os.path.exists(db_path):
                backup_path = f"{backup_dir}/node_{node_id}.db"
                shutil.copy2(db_path, backup_path)
        
        print(f"Databases backed up to {backup_dir}/")
        
        print(f"\n[3/6] Moving {len(movements)} accounts between clusters...")
        moved = self.mover.move_accounts(movements)
        print(f"Moved {moved} accounts successfully")
        print("\n[4/6] Resetting Paxos state in all databases...")
        self._reset_all_databases()
        print("Paxos state reset in all nodes")
        
        print("\n[5/6] Updating shard mappings in config.py...")
        self._update_config_file(new_mapping)
        print("config.py updated")
        
        print("\n[6/6] Restarting all nodes...")
        if node_start_callback:
            await node_start_callback()
        else:
            print("  (No start callback provided - please restart nodes manually)")
        
        await asyncio.sleep(3)
        print("All nodes restarted")
        
        print("\n" + "="*60)
        print("Resharding Complete! ✓")
        print("="*60)
        print(f"\nNew shard distribution:")
        for cluster_id in sorted(new_mapping.keys()):
            num_accounts = len(new_mapping[cluster_id])
            print(f"  Cluster {cluster_id}: {num_accounts} accounts")

    def _reset_all_databases(self):
        for node_id in range(1, TOTAL_NODES + 1):
            db_path = get_db_path(node_id)
            
            if not os.path.exists(db_path):
                print(f"Node{node_id} database not found, skipping")
                continue
            
            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                # Clear Paxos log
                cursor.execute('DELETE FROM paxos_log')
                
                # Clear view changes
                cursor.execute('DELETE FROM view_changes')
                
                # Clear checkpoints
                cursor.execute('DELETE FROM checkpoints')
                
                # Clear locks
                cursor.execute('DELETE FROM locks')
                
                # Clear WAL
                cursor.execute('DELETE FROM write_ahead_log')
                
                cursor.execute("UPDATE node_state SET value = '1' WHERE key = 'current_ballot'")
                cursor.execute("UPDATE node_state SET value = 'none' WHERE key = 'current_leader'")
                cursor.execute("UPDATE node_state SET value = '0' WHERE key = 'last_executed_seq'")
                cursor.execute("UPDATE node_state SET value = '1' WHERE key = 'current_view'")
                cursor.execute("UPDATE node_state SET value = '1' WHERE key = 'next_sequence_number'")
                
                conn.commit()
                conn.close()
                
                print(f"Reset Node{node_id} database")
            
            except Exception as e:
                print(f"Error resetting Node{node_id}: {e}")
                raise
    
    def _update_config_file(self, new_mapping: Dict[int, List[int]]):
        with open('config.py', 'r') as f:
            lines = f.readlines()
        
        start_idx = None
        end_idx = None
        
        for i, line in enumerate(lines):
            if 'SHARD_MAPPING = {' in line:
                start_idx = i
            if start_idx is not None and '}' in line and 'SHARD_MAPPING' not in line:
                end_idx = i
                break
        
        if start_idx is None:
            print("WARNING: Could not find SHARD_MAPPING in config.py")
            return
        
        new_lines = ['SHARD_MAPPING = {\n']
        
        for cluster_id in sorted(new_mapping.keys()):
            accounts = sorted(new_mapping[cluster_id])
            if accounts:
                min_account = min(accounts)
                max_account = max(accounts)
                new_lines.append(f'    {cluster_id}: ({min_account}, {max_account}),\n')
        
        new_lines.append('}\n')
        
        lines = lines[:start_idx] + new_lines + lines[end_idx+1:]
        
        with open('config.py', 'w') as f:
            f.writelines(lines)
        
        with open('shard_mapping_detailed.txt', 'w') as f:
            f.write("Detailed Shard Mapping\n")
            f.write("="*60 + "\n\n")
            
            for cluster_id in sorted(new_mapping.keys()):
                accounts = sorted(new_mapping[cluster_id])
                f.write(f"Cluster {cluster_id}: {len(accounts)} accounts\n")
                f.write(f"  Range: {min(accounts)}-{max(accounts)}\n")
                f.write(f"  Accounts: {accounts[:10]}...")
                if len(accounts) > 10:
                    f.write(f" ... {accounts[-10:]}\n")
                f.write("\n")


async def analyze_and_reshard_prompt(transactions: List[Dict[str, Any]],
                                    node_stop_callback=None,
                                    node_start_callback=None) -> bool:
    controller = ReshardingController()
    
    print("\nAnalyzing transaction patterns...")
    controller.analyze_transactions(transactions)
    
    new_mapping, analysis = controller.partition_and_analyze()
    
    if not new_mapping or analysis.get('movement_count', 0) == 0:
        print("\nNo beneficial resharding found. Continuing with current mapping.")
        return False
    
    print("\n" + "="*60)
    
    improvement = analysis.get('improvement_pct', 0)
    if improvement > 5:
        recommendation = "RECOMMENDED - Significant improvement"
    elif improvement > 0:
        recommendation = "OPTIONAL - Minor improvement"
    else:
        recommendation = "NOT RECOMMENDED - No benefit"
    
    print(f"Resharding Recommendation: {recommendation}")
    print("="*60)
    
    answer = input("\nProceed with resharding? (y/n): ").strip().lower()
    
    if answer == 'y':
        await controller.execute_resharding(
            new_mapping,
            analysis['movements'],
            node_stop_callback,
            node_start_callback
        )
        return True
    else:
        print("\nResharding skipped. Continuing with current shard mapping.")
        return False


async def run_all_tests(self):
    print("\n" + "#"*60)
    print("# CSV Test Harness")
    print(f"# Mode: {'SEQUENTIAL' if self.max_concurrent == 1 else f'{self.max_concurrent} concurrent'}")
    print("#"*60)
    
    from resharding import reset_shard_mapping_to_default
    reset_shard_mapping_to_default()
    
    from config import reload_shard_mapping
    reload_shard_mapping()
    print("Shard mapping initialized to default\n")
    
    for idx, (set_num, transactions, live_nodes) in enumerate(self.test_sets):
        resharded = False
        
        if idx + 1 < len(self.test_sets):
            next_set_num, next_transactions, next_live_nodes = self.test_sets[idx + 1]
            
            print("="*60)
            print(f"About to run Test Set {set_num}")
            print(f"Next set will be: Set {next_set_num}")
            print("="*60)
            
            answer = input(f"\nAnalyze and optimize for Set {next_set_num}? (y/n): ").strip().lower()
            
            if answer == 'y':
                parsed_transactions = []
                for item in next_transactions:
                    txn_str = item['transaction']
                    parsed_txn = self.parse_transaction(txn_str)
                    
                    if parsed_txn['type'] == 'transfer':
                        parsed_transactions.append(parsed_txn)
                
                if parsed_transactions:
                    resharded = await analyze_and_reshard_prompt(
                        parsed_transactions,
                        node_stop_callback=self.stop_all_nodes,
                        node_start_callback=self.start_all_nodes
                    )
                    
                    if resharded:
                        reload_shard_mapping()
                        #print("\nWaiting 3 seconds for system to stabilize with new mapping...")
                        await asyncio.sleep(3)
                else:
                    print("\nNo transfer transactions - skipping resharding.")
    
        results = await self.run_test_set(set_num, transactions, live_nodes)
        self.results.append({
            'set_num': set_num,
            'results': results
        })
        
        if resharded:
            print("\n" + "="*60)
            print("Restoring Default Mapping")
            print("="*60)
            reset_shard_mapping_to_default()
            reload_shard_mapping()
            print("Mapping restored to default\n")
            
            await asyncio.sleep(2)
        
        
        await asyncio.sleep(0.5)
    
    await self.print_summary()
    await self.client.close()
    
def save_current_mapping() -> Dict[int, Tuple[int, int]]:
    from config import SHARD_MAPPING
    
    saved = {}
    for cluster_id, shard_range in SHARD_MAPPING.items():
        if isinstance(shard_range, tuple):
            saved[cluster_id] = shard_range
        elif isinstance(shard_range, list):
            if shard_range:
                saved[cluster_id] = (min(shard_range), max(shard_range))
            else:
                saved[cluster_id] = (1, 1)
    
    print(f"Saved current mapping:")
    for cid in sorted(saved.keys()):
        start, end = saved[cid]
        print(f"  Cluster {cid}: {start}-{end}")
    
    return saved    

def restore_mapping(original_mapping: Dict[int, Tuple[int, int]]):
    print("\nRestoring original shard mapping...")
    
    with open('config.py', 'r') as f:
        lines = f.readlines()
    
    start_idx = None
    end_idx = None
    
    for i, line in enumerate(lines):
        if 'SHARD_MAPPING = {' in line:
            start_idx = i
        if start_idx is not None and '}' in line and 'SHARD_MAPPING' not in line:
            end_idx = i
            break
    
    if start_idx is None:
        print("WARNING: Could not find SHARD_MAPPING in config.py")
        return False
    
    new_lines = ['SHARD_MAPPING = {\n']
    for cluster_id in sorted(original_mapping.keys()):
        start, end = original_mapping[cluster_id]
        new_lines.append(f'    {cluster_id}: ({start}, {end}),\n')
    new_lines.append('}\n')
    
    lines = lines[:start_idx] + new_lines + lines[end_idx+1:]
    
    with open('config.py', 'w') as f:
        f.writelines(lines)
    
    print("Shard mapping restored:")
    for cluster_id in sorted(original_mapping.keys()):
        start, end = original_mapping[cluster_id]
        print(f"  Cluster {cluster_id}: {start}-{end}")
    
    return True

def reset_shard_mapping_to_default():
    default_mapping = {
        1: (1, 3000),
        2: (3001, 6000),
        3: (6001, 9000)
    }
    
    print("Resetting shard mapping to default...")
    restore_mapping(default_mapping)
    print()


if __name__ == '__main__':
    print("Shard Redistribution System - Test Mode")
    print("="*60)
    
    sample_transactions = [
        {'type': 'transfer', 'sender': 1001, 'receiver': 1002},
        {'type': 'transfer', 'sender': 1001, 'receiver': 3001},
        {'type': 'transfer', 'sender': 3001, 'receiver': 3002},
        {'type': 'transfer', 'sender': 3001, 'receiver': 1001},
        {'type': 'transfer', 'sender': 6001, 'receiver': 6002},
        {'type': 'transfer', 'sender': 1002, 'receiver': 3001},
        {'type': 'balance', 'account_id': 1001},
    ]
    
    async def test():
        controller = ReshardingController()
        controller.analyze_transactions(sample_transactions)
        new_mapping, analysis = controller.partition_and_analyze()
    
    asyncio.run(test())