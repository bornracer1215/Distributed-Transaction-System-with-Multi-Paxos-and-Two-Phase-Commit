#Client Interface - FIXED VERSION

import asyncio
import time
from typing import Optional, Dict, Any, List, Tuple
import random
import csv

from config import *
from messages import Message, MessageSerializer
from resharding import analyze_and_reshard_prompt

class Client:
    # Client that can send requests to the distributed system
    
    def __init__(self, client_id: int = 0):
        self.client_id = client_id
        self.timeout = CLIENT_TIMEOUT
    
    async def _send_request(self, node_id: int, message: Dict[str, Any], 
                       connect_timeout: float = 0.5) -> Dict[str, Any]:
        # Send request and wait for response.
       
        host, port = get_node_address(node_id)
        reader = None
        writer = None
        
        try:
            # Open connection with shorter timeout
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=connect_timeout
            )
            
            # Send request
            data = MessageSerializer.serialize(message)
            writer.write(data)
            await writer.drain()
            
            # Wait for response (connection stays open)
            response_data = await asyncio.wait_for(
                reader.readline(),
                timeout=self.timeout  
            )
            
            if not response_data:
                raise ConnectionError(f"Connection to node {node_id} closed without response")
            
            response = MessageSerializer.deserialize(response_data)
            return response
        
        except asyncio.TimeoutError:
            raise TimeoutError(f"Request to node {node_id} timed out")
        
        except Exception as e:
            raise ConnectionError(f"Failed to communicate with node {node_id}: {e}")
        
        finally:
            # Always close connection
            if writer:
                try:
                    writer.close()
                    await writer.wait_closed()
                except:
                    pass
    
    async def _send_to_cluster(self, cluster_id: int, message: Dict[str, Any],
                          try_all_nodes: bool = True, max_retries: int = 2) -> Dict[str, Any]:
        # Send request to a cluster (tries leader first, then others)
        cluster_nodes = CLUSTER_NODES[cluster_id]
        
        for retry in range(max_retries):
            for node_id in cluster_nodes:
                try:
                    # Use shorter connection timeout (0.5s for failed nodes)
                    response = await self._send_request(node_id, message, connect_timeout=0.5)
                    
                    # Retry on "Not leader" error
                    if response.get('type') == 'ERROR':
                        error_msg = response.get('error', '').lower()
                        if 'not leader' in error_msg or 'not a leader' in error_msg:
                            continue  
                    
                    return response
                
                except (TimeoutError, ConnectionError) as e:
                    if not try_all_nodes:
                        raise
                    # Log and continue to next node quickly
                    continue
            
            # If we get here, all nodes failed or aren't leader yet
            # Wait a bit longer before retry (leader election may be in progress)
            if retry < max_retries - 1:
                backoff = 0.5 * (retry + 1)
                await asyncio.sleep(backoff)  
        
        raise ConnectionError(f"All nodes in cluster {cluster_id} failed after {max_retries} retries")
    
    async def transfer(self, sender: int, receiver: int, amount: int,
                      consistency: str = DEFAULT_CONSISTENCY) -> Dict[str, Any]:
        validate_account_id(sender)
        validate_account_id(receiver)
        
        sender_cluster = get_cluster_for_account(sender)
        receiver_cluster = get_cluster_for_account(receiver)
        
        txn_msg = Message.transaction(sender, receiver, amount, consistency)
        
        start_time = time.time()
        try:
            response = await self._send_to_cluster(sender_cluster, txn_msg, max_retries=4)
            latency = time.time() - start_time
            
            return {
                'success': response.get('status') == 'success',
                'response': response,
                'latency': latency,
                'cross_shard': sender_cluster != receiver_cluster,
                'sender_cluster': sender_cluster,
                'receiver_cluster': receiver_cluster
            }
        except Exception as e:
            latency = time.time() - start_time
            return {
                'success': False,
                'error': str(e),
                'latency': latency,
                'cross_shard': sender_cluster != receiver_cluster,
                'sender_cluster': sender_cluster,
                'receiver_cluster': receiver_cluster
            }
    
    async def get_balance(self, account_id: int,
                         consistency: str = DEFAULT_CONSISTENCY) -> Dict[str, Any]:
        # Get account balance
        validate_account_id(account_id)
        
        cluster_id = get_cluster_for_account(account_id)
        bal_msg = Message.balance(account_id, consistency)
        
        start_time = time.time()
        try:
            response = await self._send_to_cluster(cluster_id, bal_msg, max_retries=4)
            latency = time.time() - start_time
            
            return {
                'success': response.get('status') == 'success',
                'balance': response.get('data', {}).get('balance'),
                'response': response,
                'latency': latency,
                'cluster': cluster_id
            }
        except Exception as e:
            latency = time.time() - start_time
            return {
                'success': False,
                'error': str(e),
                'latency': latency,
                'cluster': cluster_id
            }
    
    async def print_balance_query(self, node_id: int, account_id: int) -> Dict[str, Any]:
        # Query a specific node for an account balance
        from messages import MSG_PRINT_BALANCE
        
        query_msg = {
            'type': MSG_PRINT_BALANCE,
            'account_id': account_id,
            'timestamp': time.time()
        }
        
        try:
            response = await self._send_request(node_id, query_msg, connect_timeout=1.0)
            return {
                'success': True,
                'node_id': response.get('node_id'),
                'balance': response.get('balance')
            }
        except Exception as e:
            return {
                'success': False,
                'node_id': node_id,
                'balance': None,
                'error': str(e)
            }
    
    async def print_db_query(self, node_id: int) -> Dict[str, Any]:
        # Query a specific node for modified accounts
        from messages import MSG_PRINT_DB
        
        query_msg = {
            'type': MSG_PRINT_DB,
            'timestamp': time.time()
        }
        
        try:
            response = await self._send_request(node_id, query_msg, connect_timeout=1.0)
            return {
                'success': True,
                'node_id': response.get('node_id'),
                'modified_accounts': response.get('modified_accounts', {})
            }
        except Exception as e:
            return {
                'success': False,
                'node_id': node_id,
                'modified_accounts': {},
                'error': str(e)
            }
    
    async def print_view_query(self, node_id: int) -> Dict[str, Any]:
        # Query a specific node for view change history AND current state
        from messages import MSG_PRINT_VIEW
        
        query_msg = {
            'type': MSG_PRINT_VIEW,
            'timestamp': time.time()
        }
        
        try:
            response = await self._send_request(node_id, query_msg, connect_timeout=1.0)
            return {
                'success': True,
                'node_id': response.get('node_id'),
                'cluster_id': response.get('cluster_id'),
                'view_changes': response.get('view_changes', []),
                'current_leader': response.get('current_leader'),  # NEW
                'current_ballot': response.get('current_ballot'),  # NEW
                'is_failed': response.get('is_failed', False)      # NEW
            }
        except Exception as e:
            return {
                'success': False,
                'node_id': node_id,
                'cluster_id': None,
                'view_changes': [],
                'error': str(e)
            }
            
    async def performance_query(self, node_id: int = 1) -> Dict[str, Any]:
        # Query performance statistics from the system
        
        from messages import MSG_PERFORMANCE
        
        query_msg = {
            'type': MSG_PERFORMANCE,
            'timestamp': time.time()
        }
        
        try:
            response = await self._send_request(node_id, query_msg, connect_timeout=2.0)
            return {
                'success': True,
                'aggregated_stats': response.get('aggregated_stats', {}),
                'per_node_stats': response.get('per_node_stats', {})
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    async def fail_node(self, node_id: int) -> Dict[str, Any]:
        validate_node_id(node_id)
        
        fail_msg = Message.fail(node_id)
        try:
            # For FAIL command, don't wait for response - node will fail immediately
            host, port = get_node_address(node_id)
            reader, writer = await asyncio.open_connection(host, port)
            
            data = MessageSerializer.serialize(fail_msg)
            writer.write(data)
            await writer.drain()
            
            # Close immediately without waiting for response
            writer.close()
            await writer.wait_closed()
            
            return {'success': True, 'message': f'Node {node_id} failed'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    async def recover_node(self, node_id: int) -> Dict[str, Any]:
        # Send recovery command to a node
        validate_node_id(node_id)
        
        recover_msg = Message.recover(node_id)
        try:
            # For RECOVER command, also don't wait for response
            host, port = get_node_address(node_id)
            reader, writer = await asyncio.open_connection(host, port)
            
            data = MessageSerializer.serialize(recover_msg)
            writer.write(data)
            await writer.drain()
            
            writer.close()
            await writer.wait_closed()
            
            return {'success': True, 'message': f'Node {node_id} recovered'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    async def close(self):
        # Close all connections (no-op now since we don't cache)
        pass


class CSVTestRunner:
    def __init__(self, csv_file: str, max_concurrent: int = 1):
        # Initialize test runner with rate limiting
        self.csv_file = csv_file
        self.client = Client()
        self.test_sets = []
        self.results = []
        self.max_concurrent = max_concurrent

        self.current_set_metrics = {
            'start_time': None,
            'end_time': None,
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'latencies': []  
    }
        
    def parse_csv(self):
        with open(self.csv_file, 'r') as f:
            reader = csv.reader(f)
            next(reader)  
            
            current_set = []
            current_set_num = None
            current_live_nodes = None
            
            for row in reader:
                if not row or len(row) < 2:
                    continue
                
                set_num = row[0].strip().strip('"') if row[0].strip() else None
                transaction = row[1].strip().strip('"')
                live_nodes = row[2].strip().strip('"') if len(row) > 2 and row[2].strip() else None
                
                if not transaction:
                    continue
                
                # New set detected
                if set_num and set_num != current_set_num:
                    # Save previous set if exists
                    if current_set:
                        self.test_sets.append((current_set_num, current_set, current_live_nodes))
                    # Start new set
                    current_set = []
                    current_set_num = set_num
                    if live_nodes:
                        current_live_nodes = live_nodes
                
                # Add transaction to current set
                current_set.append({
                    'transaction': transaction,
                    'live_nodes': current_live_nodes  
                })
            
            # Don't forget last set
            if current_set:
                self.test_sets.append((current_set_num, current_set, current_live_nodes))
        
        print(f"Parsed {len(self.test_sets)} test sets")
        for set_num, txns, live_nodes in self.test_sets:
            print(f"  Set {set_num}: {len(txns)} transactions, Live nodes: {live_nodes}")
    
    def parse_live_nodes(self, live_nodes_str: str) -> list:
        # Parse live nodes string like '[n1, n2, n4, n5, n7, n8]' into list of node IDs
        import re
        matches = re.findall(r'n(\d+)', live_nodes_str)
        return [int(n) for n in matches]
    
    async def setup_initial_nodes(self, live_nodes: list):
        # Fail all nodes that are NOT in the live_nodes list
        all_nodes = list(range(1, 10))  # nodes 1-9
        nodes_to_fail = [n for n in all_nodes if n not in live_nodes]
        
        print(f"Live nodes: {live_nodes}")
        print(f"Failing nodes: {nodes_to_fail}")
        
        for node_id in nodes_to_fail:
            result = await self.client.fail_node(node_id)
            if result['success']:
                print(f"Node {node_id} failed")
            else:
                print(f"Failed to fail node {node_id}: {result.get('error', 'Unknown')}")
            await asyncio.sleep(0.1)  # Small delay between failures
    
    def parse_transaction(self, txn_str: str):
        txn_str = txn_str.strip()
        
        # Balance query with negative sign: -1001 means balance query for 1001
        if txn_str.startswith('-') and txn_str[1:].isdigit():
            account_id = int(txn_str[1:])  # Remove negative sign
            return {'type': 'balance', 'account_id': account_id}
        
        # Fail command: F(n1) or F(1)
        if txn_str.startswith('F(') and txn_str.endswith(')'):
            node_str = txn_str[2:-1]
            node_id = int(node_str[1:]) if node_str.startswith('n') else int(node_str)
            return {'type': 'fail', 'node_id': node_id}
        
        # Recover command: R(n1) or R(1)
        if txn_str.startswith('R(') and txn_str.endswith(')'):
            node_str = txn_str[2:-1]
            node_id = int(node_str[1:]) if node_str.startswith('n') else int(node_str)
            return {'type': 'recover', 'node_id': node_id}
        
        # Transaction or balance
        if txn_str.startswith('(') and txn_str.endswith(')'):
            inner = txn_str[1:-1]
            parts = [p.strip() for p in inner.split(',')]
            
            if len(parts) == 3:
                # Transfer: (sender, receiver, amount)
                sender = int(parts[0])
                receiver = int(parts[1])
                amount = int(parts[2])
                return {
                    'type': 'transfer',
                    'sender': sender,
                    'receiver': receiver,
                    'amount': amount
                }
            elif len(parts) == 1:
                # Balance: (account_id)
                account_id = int(parts[0])
                return {'type': 'balance', 'account_id': account_id}
        
        return {'type': 'unknown', 'raw': txn_str}
    
    async def execute_transaction(self, txn):
        txn_type = txn['type']
        
        if txn_type == 'transfer':
            result = await self.client.transfer(
                txn['sender'],
                txn['receiver'],
                txn['amount']
            )
            return result
        
        elif txn_type == 'balance':
            result = await self.client.get_balance(txn['account_id'])
            return result
        
        elif txn_type == 'fail':
            result = await self.client.fail_node(txn['node_id'])
            return result
        
        elif txn_type == 'recover':
            result = await self.client.recover_node(txn['node_id'])
            return result
        
        else:
            return {'success': False, 'error': 'Unknown transaction type'}
    
    async def run_test_set(self, set_num: str, transactions, live_nodes_str):
        print("\n" + "="*60)
        print(f"Running Test Set {set_num} ({len(transactions)} transactions)")
        print(f"Live Nodes: {live_nodes_str}")
        print("="*60)
        
        self.current_set_metrics = {
            'start_time': time.time(),
            'end_time': None,
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'latencies': []
    }

        #print(f"Recovering all nodes first...")
        all_nodes = list(range(1, 10))
        for node_id in all_nodes:
            try:
                result = await self.client.recover_node(node_id)
                await asyncio.sleep(0.1)
            except:
                pass
        
        await asyncio.sleep(2)  
        #print(f"All nodes recovered")
        if live_nodes_str:
            live_nodes = self.parse_live_nodes(live_nodes_str)
            #print(f"Setting up node states for this test set...")
            
            all_nodes = set(range(1, 10))
            live_nodes_set = set(live_nodes)
            nodes_to_fail = all_nodes - live_nodes_set 
            
            await self.setup_initial_nodes(live_nodes)
            await asyncio.sleep(2)  
        
        if set_num != '1':
            await self.flush_all_databases()
            await asyncio.sleep(1)
        
        set_results = []
        
        # Process transactions with rate limiting
        i = 0
        while i < len(transactions):
            item = transactions[i]
            txn_str = item['transaction']
            txn = self.parse_transaction(txn_str)
            
            # Fail/Recover commands must be sequential
            if txn['type'] in ['fail', 'recover']:
                if txn['type'] == 'fail':
                    print(f"[{i+1}/{len(transactions)}] F(n{txn['node_id']}) - Failing node {txn['node_id']}...")
                    result = await self.execute_transaction(txn)
                    status = "✓" if result['success'] else "✗"
                    msg = result.get('message', result.get('error', 'Unknown'))
                    print(f"    {status} {msg}")
                    await asyncio.sleep(0.3)
                else:  
                    print(f"[{i+1}/{len(transactions)}] R(n{txn['node_id']}) - Recovering node {txn['node_id']}...")
                    result = await self.execute_transaction(txn)
                    status = "✓" if result['success'] else "✗"
                    msg = result.get('message', result.get('error', 'Unknown'))
                    print(f"    {status} {msg}")
                    
                    await asyncio.sleep(2.0)
                
                set_results.append({
                    'transaction': txn_str,
                    'parsed': txn,
                    'result': result
                })
                i += 1
            
            else:
                batch = []
                batch_start = i
                while i < len(transactions) and len(batch) < self.max_concurrent:
                    item = transactions[i]
                    txn_str = item['transaction']
                    txn = self.parse_transaction(txn_str)
                    
                    if txn['type'] in ['fail', 'recover']:
                        break
                    
                    batch.append((i, txn_str, txn))
                    i += 1
                
                tasks = [self.execute_transaction(txn) for _, _, txn in batch]
                batch_results = await asyncio.gather(*tasks)
                
                # Report results
                for (idx, txn_str, txn), result in zip(batch, batch_results):
                    status = "✓" if result['success'] else "✗"

                    self.current_set_metrics['total_requests'] += 1
                    if result['success']:
                        self.current_set_metrics['successful_requests'] += 1
                        if 'latency' in result:
                            self.current_set_metrics['latencies'].append(result['latency'])
                    else:
                        self.current_set_metrics['failed_requests'] += 1

                    if result['success']:
                        if txn['type'] == 'transfer':
                            print(f"[{idx+1}/{len(transactions)}] {txn_str} {status} [C{result['sender_cluster']}→C{result['receiver_cluster']}] {result['latency']*1000:.1f}ms")
                        elif txn['type'] == 'balance':
                            print(f"[{idx+1}/{len(transactions)}] {txn_str} {status} Balance={result['balance']} [C{result['cluster']}] {result['latency']*1000:.1f}ms")
                    else:
                        error_msg = result.get('error', 'Unknown')
                        # Shorten error message
                        if len(error_msg) > 50:
                            error_msg = error_msg[:47] + "..."
                        print(f"[{idx+1}/{len(transactions)}] {txn_str} {status} FAILED: {error_msg}")
                    
                    set_results.append({
                        'transaction': txn_str,
                        'parsed': txn,
                        'result': result
                    })
                
                # Small delay between batches (only if not last batch)
                if i < len(transactions) and self.max_concurrent > 1:
                    await asyncio.sleep(0.1)
        
        print(f"\nTest Set {set_num} Complete")
        print("="*60)
        
        self.current_set_metrics['end_time'] = time.time()
        return set_results
    
    async def flush_all_databases(self):
        flush_msg = {'type': 'FLUSH', 'timestamp': time.time()}
        
        for cluster_id in [1, 2, 3]:
            for node_id in CLUSTER_NODES[cluster_id]:
                try:
                    reader, writer = await asyncio.open_connection(
                        HOST, 
                        get_node_port(node_id)
                    )
                    data = MessageSerializer.serialize(flush_msg)
                    writer.write(data)
                    await writer.drain()
                    writer.close()
                    await writer.wait_closed()
                except:
                    pass
        
        #print("All databases flushed")
    
    async def run_all_tests(self):
        print("\n" + "#"*60)
        print("# CSV Test Harness")
        #print(f"# Mode: {'SEQUENTIAL' if self.max_concurrent == 1 else f'{self.max_concurrent} concurrent'}")
        print("#"*60)
        
        for idx, (set_num, transactions, live_nodes) in enumerate(self.test_sets):
            # Run current test set
            results = await self.run_test_set(set_num, transactions, live_nodes)
            self.results.append({
                'set_num': set_num,
                'results': results
            })

            await self.show_interactive_menu(set_num)
            
            # AFTER test completes, check if there's a next set for resharding
            if idx + 1 < len(self.test_sets):
                next_set_num, next_transactions, next_live_nodes = self.test_sets[idx + 1]
                
                print("\n" + "="*60)
                print(f"Set {set_num} Complete")
                print(f"Next: Set {next_set_num}")
                print("="*60)
                
                answer = input(f"\nAnalyze Set {next_set_num} for resharding? (y/n): ").strip().lower()
                
                if answer == 'y':
                    # Parse next set's transactions
                    parsed_transactions = []
                    for item in next_transactions:
                        txn_str = item['transaction']
                        parsed_txn = self.parse_transaction(txn_str)
                        
                        if parsed_txn['type'] == 'transfer':
                            parsed_transactions.append(parsed_txn)
                    
                    if parsed_transactions:
                        # Call resharding with node control callbacks
                        resharded = await analyze_and_reshard_prompt(
                            parsed_transactions,
                            node_stop_callback=self.stop_all_nodes,
                            node_start_callback=self.start_all_nodes
                        )
                        
                        if resharded:
                            print("\nWaiting 5 seconds")
                            await asyncio.sleep(5)
                    else:
                        print("\nNo transfer transactions in next set - skipping resharding.")
                
                await asyncio.sleep(0.5)
        
        await self.print_summary()
        await self.client.close()
    
    async def stop_all_nodes(self):
        # Stop all nodes for resharding
        print("Stopping all nodes...")
        
        for node_id in range(1, 10):
            try:
                result = await self.client.fail_node(node_id)
                await asyncio.sleep(0.1)
            except:
                pass
        
        await asyncio.sleep(2)
        print("All nodes stopped")

    async def start_all_nodes(self):
        # Start/recover all nodes after resharding
        print("Starting all nodes...")
        
        for node_id in range(1, 10):
            try:
                result = await self.client.recover_node(node_id)
                await asyncio.sleep(0.1)
            except:
                pass
        
        await asyncio.sleep(3)
        print("All nodes started")
    
    async def print_summary(self):
        print("\n" + "#"*60)
        print("# Test Summary")
        print("#"*60)
        
        for test_set in self.results:
            set_num = test_set['set_num']
            results = test_set['results']
            
            # Count only actual transactions (not fail/recover commands)
            actual_txns = [r for r in results if r['parsed']['type'] not in ['fail', 'recover']]
            total = len(actual_txns)
            successful = sum(1 for r in actual_txns if r['result'].get('success', False))
            
            print(f"\nSet {set_num}: {successful}/{total} transactions successful")
            
            failures = [r for r in actual_txns if not r['result'].get('success', False)]
            if failures:
                print(f"  Failures:")
                for r in failures[:10]:  # Show first 10 failures
                    print(f" {r['transaction']}: {r['result'].get('error', 'Unknown')}")
                if len(failures) > 10:
                    print(f"    ... and {len(failures) - 10} more")
        
        print("\n" + "#"*60)

    async def show_interactive_menu(self, set_num: str):
        # interactive menu after completing a test set
        while True:
            print("\n" + "="*60)
            print(f"Test Set {set_num} - Interactive Menu")
            print("="*60)
            print("1. PrintBalance - Query balance across cluster nodes")
            print("2. PrintDB - Show modified accounts on all nodes")
            print("3. PrintView - Display view change history")
            print("4. Performance - Show throughput and latency metrics")
            print("5. Continue to next test set")
            print("="*60)
            
            choice = input("\nEnter your choice (1-5): ").strip()
            
            if choice == '1':
                await self.handle_print_balance()
            elif choice == '2':
                await self.handle_print_db()
            elif choice == '3':
                await self.handle_print_view()
            elif choice == '4':
                await self.handle_performance()
            elif choice == '5':
                print("\nContinuing to next test set...\n")
                break
            else:
                print("\nInvalid choice. Please enter 1-5.")

    async def handle_print_balance(self):
        # Handle PrintBalance command
        print("\n" + "="*60)
        print("PrintBalance - Query Account Balance Across Cluster")
        print("="*60)
        
        # Get account ID from user
        account_input = input("\nEnter account ID (1-9000): ").strip()
        
        try:
            account_id = int(account_input)
            
            # Validate account ID
            if not (1 <= account_id <= 9000):
                print(f"Error: Account ID must be between 1 and 9000")
                input("\nPress Enter to continue...")
                return
            
            # Determine which cluster this account belongs to
            cluster_id = get_cluster_for_account(account_id)
            cluster_nodes = CLUSTER_NODES[cluster_id]
            
            print(f"\nQuerying account {account_id} from Cluster {cluster_id} (nodes {cluster_nodes})...")
            print(f"Input: PrintBalance({account_id});")
            
            # Query all nodes in the cluster
            tasks = []
            for node_id in cluster_nodes:
                tasks.append(self.client.print_balance_query(node_id, account_id))
            
            results = await asyncio.gather(*tasks)
            
            output_parts = []
            for result in results:
                node_id = result['node_id']
                balance = result['balance']
                
                if balance is not None:
                    output_parts.append(f"n{node_id} : {balance}")
                else:
                    output_parts.append(f"n{node_id} : N/A")
            
            output = ", ".join(output_parts)
            print(f"Output: {output}")
            
        except ValueError:
            print(f"Error: Invalid account ID format")
        except Exception as e:
            print(f"Error: {e}")
        
        input("\nPress Enter to continue...")

    async def handle_print_db(self):
        # Handle PrintDB command - show modified accounts on all nodes
        print("\n" + "="*60)
        print("PrintDB - Modified Accounts on All Nodes")
        print("="*60)
        print("\nQuerying all 9 nodes for modified accounts (balance != 10)...\n")
        
        all_nodes = list(range(1, 10))
        tasks = []
        for node_id in all_nodes:
            tasks.append(self.client.print_db_query(node_id))
        
        results = await asyncio.gather(*tasks)
        
        for result in results:
            node_id = result['node_id']
            
            if not result['success']:
                print(f"n{node_id}: [Error: {result.get('error', 'Unknown')}]")
                continue
            
            modified_accounts = result['modified_accounts']
            
            if not modified_accounts:
                print(f"n{node_id}: [No modified accounts]")
            else:
                sorted_accounts = dict(sorted(modified_accounts.items()))
                accounts_str = ", ".join([f"{acc_id}: {balance}" 
                                        for acc_id, balance in sorted_accounts.items()])
                print(f"n{node_id}: {{{accounts_str}}}")
        
        print("\n" + "="*60)
        
        total_modified = sum(len(r['modified_accounts']) for r in results if r['success'])
        print(f"Total modified accounts across all nodes: {total_modified}")
        
        input("\nPress Enter to continue...")

    async def handle_print_view(self):
        #Handle PrintView command - show current cluster leadership and inferred view changes
        print("\n" + "="*60)
        print("PrintView - Cluster Leadership & View Changes")
        print("="*60)
        print("\nQuerying cluster status...\n")
        
        print("Current Cluster Leadership:")
        print("-" * 60)
        
        cluster_info = {} 
        
        for cluster_id in [1, 2, 3]:
            cluster_nodes = CLUSTER_NODES[cluster_id]
            
            print(f"\nCluster {cluster_id} (Nodes {cluster_nodes[0]}, {cluster_nodes[1]}, {cluster_nodes[2]}):")
            
            # Query each node in the cluster
            node_states = []
            for node_id in cluster_nodes:
                try:
                    result = await self.client.print_view_query(node_id)
                    
                    if result['success']:
                        current_leader = result.get('current_leader', 'Unknown')
                        current_ballot = result.get('current_ballot', 'Unknown')
                        is_failed = result.get('is_failed', False)
                        
                        status = "FAILED" if is_failed else "ACTIVE"
                        leader_info = f"sees leader=n{current_leader}" if current_leader != 'Unknown' else "no leader"
                        
                        node_states.append({
                            'node_id': node_id,
                            'status': status,
                            'leader': current_leader,
                            'ballot': current_ballot
                        })
                        
                        print(f"  n{node_id}: {status:8} | {leader_info}, ballot={current_ballot}")
                    else:
                        print(f"  n{node_id}: OFFLINE  | (no response)")
                        
                except Exception as e:
                    print(f"  n{node_id}: ERROR    | {str(e)[:40]}")
            
            # Determine cluster leader (consensus)
            if node_states:
                leader_votes = {}
                for state in node_states:
                    if state['status'] == 'ACTIVE':
                        leader = state['leader']
                        leader_votes[leader] = leader_votes.get(leader, 0) + 1
                
                if leader_votes:
                    consensus_leader = max(leader_votes, key=leader_votes.get)
                    consensus_ballot = node_states[0]['ballot']  # They should all agree on ballot
                    print(f" Cluster Leader: n{consensus_leader} (consensus from {leader_votes[consensus_leader]} nodes)")
                    
                    cluster_info[cluster_id] = {
                        'current_leader': consensus_leader,
                        'current_ballot': consensus_ballot,
                        'node_states': node_states
                    }
                else:
                    print(f" Cluster Leader: Unknown (no active nodes)")
                    cluster_info[cluster_id] = None
        
        print("\n" + "-" * 60)
        print("Leader Election Timeline:")
        print("-" * 60)
        
        all_events = []
        
        for cluster_id in [1, 2, 3]:
            cluster_nodes = CLUSTER_NODES[cluster_id]
            
            for node_id in cluster_nodes:
                try:
                    result = await self.client.print_view_query(node_id)
                    
                    if result['success']:
                        view_changes = result.get('view_changes', [])
                        
                        # Add to timeline
                        for vc in view_changes:
                            all_events.append({
                                'cluster_id': cluster_id,
                                'type': 'recorded_election',
                                'old_leader': vc['old_leader_id'],
                                'new_leader': vc['new_leader_id'],
                                'ballot': vc['ballot_number'],
                                'timestamp': vc['timestamp']
                            })
                        
                        # Infer if there was another election after last recorded one
                        if cluster_info.get(cluster_id):
                            current_leader = cluster_info[cluster_id]['current_leader']
                            current_ballot = cluster_info[cluster_id]['current_ballot']
                            
                            if view_changes:
                                last_recorded_leader = view_changes[-1]['new_leader_id']
                                last_recorded_ballot = view_changes[-1]['ballot_number']
                                
                                # If current leader/ballot differs, there was an unrecorded election
                                if (current_leader != last_recorded_leader or 
                                    current_ballot > last_recorded_ballot):
                                    all_events.append({
                                        'cluster_id': cluster_id,
                                        'type': 'inferred_election',
                                        'old_leader': last_recorded_leader,
                                        'new_leader': current_leader,
                                        'ballot': current_ballot,
                                        'timestamp': None
                                    })
                        
                        break  
                        
                except Exception:
                    continue
        
        if not all_events:
            print("\nNo leader elections detected.")
            print("Initial leaders: n1 (Cluster 1), n4 (Cluster 2), n7 (Cluster 3)")
        else:
            all_events.sort(key=lambda x: x['timestamp'] if x['timestamp'] else float('inf'))
            
            print(f"\nDetected {len(all_events)} leader election(s):\n")
            
            for i, event in enumerate(all_events, 1):
                from datetime import datetime
                
                old_leader_str = f"n{event['old_leader']}" if event['old_leader'] else "Initial"
                
                if event['type'] == 'recorded_election':
                    ts_str = datetime.fromtimestamp(event['timestamp']).strftime('%H:%M:%S.%f')[:-3]
                    print(f"  #{i} [{ts_str}] Cluster {event['cluster_id']}: "
                        f"{old_leader_str} → n{event['new_leader']} (ballot={event['ballot']})")
                else:
                    print(f"  #{i} [after above] Cluster {event['cluster_id']}: "
                        f"{old_leader_str} → n{event['new_leader']} (ballot={event['ballot']}) *inferred*")
            
            #print("\n  *inferred = detected from current state but not logged in view_changes table")
        
        print("\n" + "-" * 60)
        print("Summary:")
        print("-" * 60)
        
        for cluster_id in [1, 2, 3]:
            if cluster_info.get(cluster_id):
                info = cluster_info[cluster_id]
                failed_nodes = [s['node_id'] for s in info['node_states'] if s['status'] == 'FAILED']
                offline_count = 3 - len(info['node_states'])
                
                if failed_nodes or offline_count > 0:
                    failed_str = f"n{', n'.join(map(str, failed_nodes))}" if failed_nodes else "none"
                    print(f"\nCluster {cluster_id}:")
                    print(f"  Failed/Offline nodes: {failed_str}")
                    print(f"  Current leader: n{info['current_leader']} (ballot={info['current_ballot']})")
                    
                    # Infer what happened
                    initial_leaders = {1: 1, 2: 4, 3: 7}
                    if info['current_leader'] != initial_leaders[cluster_id]:
                        print(f" Leader changed from n{initial_leaders[cluster_id]} due to failure")
        
        print("\n" + "="*60)
        input("\nPress Enter to continue...")

    async def handle_performance(self):
        # Handle Performance command - show throughput and latency metrics
        print("\n" + "="*60)
        print("Performance Metrics")
        print("="*60)
        
        # Show metrics from current test set
        if self.current_set_metrics['end_time']:
            duration = self.current_set_metrics['end_time'] - self.current_set_metrics['start_time']
            total = self.current_set_metrics['total_requests']
            successful = self.current_set_metrics['successful_requests']
            failed = self.current_set_metrics['failed_requests']
            latencies = self.current_set_metrics['latencies']
            
            print(f"\nCurrent Test Set Metrics:")
            print(f"  Duration: {duration:.2f}s")
            print(f"  Total Requests: {total}")
            print(f"  Successful: {successful}")
            print(f"  Failed: {failed}")
            print(f"  Success Rate: {(successful/max(1, total)*100):.2f}%")
            
            if successful > 0:
                throughput = successful / duration
                print(f"\nThroughput:")
                print(f"  {throughput:.2f} transactions/second")
            
            if latencies:
                import numpy as np
                latencies_sorted = sorted(latencies)
                n = len(latencies_sorted)
                
                print(f"\nLatency (milliseconds):")
                print(f"  Mean:   {np.mean(latencies)*1000:.2f} ms")
                print(f"  Median: {np.median(latencies)*1000:.2f} ms")
                print(f"  p50:    {latencies_sorted[int(n*0.50)]*1000:.2f} ms")
                print(f"  p90:    {latencies_sorted[int(n*0.90)]*1000:.2f} ms")
                print(f"  p95:    {latencies_sorted[int(n*0.95)]*1000:.2f} ms")
                print(f"  p99:    {latencies_sorted[int(n*0.99)]*1000:.2f} ms")
                print(f"  Min:    {min(latencies)*1000:.2f} ms")
                print(f"  Max:    {max(latencies)*1000:.2f} ms")
        else:
            print("\nNo metrics available yet (test set not completed)")
        
        print(f"\n" + "-"*60)
        print("Querying system-wide performance from all nodes...")
        print("-"*60)
        
        result = await self.client.performance_query(node_id=1)
        
        if result['success']:
            agg_stats = result['aggregated_stats']
            
            print(f"\nSystem-Wide Aggregated Statistics:")
            print(f"  Total Transactions: {agg_stats.get('total_transactions', 0)}")
            print(f"  Successful: {agg_stats.get('successful_transactions', 0)}")
            print(f"  Failed: {agg_stats.get('failed_transactions', 0)}")
            print(f"  Success Rate: {agg_stats.get('success_rate', 0):.2f}%")
            
            latency = agg_stats.get('latency', {})
            if latency:
                print(f"\nSystem-Wide Latency (milliseconds):")
                print(f"  Mean:   {latency.get('mean_ms', 0):.2f} ms")
                print(f"  Median: {latency.get('median_ms', 0):.2f} ms")
                print(f"  p50:    {latency.get('p50_ms', 0):.2f} ms")
                print(f"  p90:    {latency.get('p90_ms', 0):.2f} ms")
                print(f"  p95:    {latency.get('p95_ms', 0):.2f} ms")
                print(f"  p99:    {latency.get('p99_ms', 0):.2f} ms")
                print(f"  Min:    {latency.get('min_ms', 0):.2f} ms")
                print(f"  Max:    {latency.get('max_ms', 0):.2f} ms")
            
            show_details = input("\nShow per-node breakdown? (y/n): ").strip().lower()
            if show_details == 'y':
                per_node = result['per_node_stats']
                print(f"\nPer-Node Statistics:")
                for node_id in sorted(per_node.keys()):
                    stats = per_node[node_id]
                    if stats:
                        print(f"\n  Node {node_id}:")
                        print(f"    Total: {stats.get('total_transactions', 0)}")
                        print(f"    Successful: {stats.get('successful_transactions', 0)}")
                        print(f"    Failed: {stats.get('failed_transactions', 0)}")
                    else:
                        print(f"\n  Node {node_id}: [No data]")
        else:
            print(f"\nFailed to query system performance: {result.get('error', 'Unknown')}")
        
        print("\n" + "="*60)
        input("\nPress Enter to continue...")


async def run_csv_main():
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python client.py <csv_file> [max_concurrent]")
        print("Example: python client.py test.csv 1")
        print("         python client.py test.csv 3  (for concurrent)")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    max_concurrent = int(sys.argv[2]) if len(sys.argv) > 2 else 1  
    
    print(f"Mode: {'SEQUENTIAL' if max_concurrent == 1 else f'{max_concurrent} concurrent transactions'}")
    print("Waiting 3 seconds for nodes to start and elect leaders...")
    await asyncio.sleep(3)
    
    runner = CSVTestRunner(csv_file, max_concurrent=max_concurrent)
    runner.parse_csv()
    await runner.run_all_tests()

if __name__ == '__main__':
    asyncio.run(run_csv_main())