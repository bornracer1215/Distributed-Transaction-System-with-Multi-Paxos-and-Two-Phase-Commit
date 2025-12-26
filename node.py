#Node Implementation - Multi-Paxos with Two-Phase Commit

import asyncio
import time
import sys
import random 
import uuid
from typing import Dict, Any, Optional, Set
from collections import defaultdict
from datetime import datetime
from config import reload_shard_mapping

from config import *
from messages import Message, MessageSerializer
from database import NodeDatabase
from server import NodeServer

MSG_FLUSH = 'FLUSH'

def log_info(node_id: int, message: str):
    timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    print(f"[{timestamp}] Node{node_id} [INFO] {message}")

def log_debug(node_id: int, message: str):
    timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    print(f"[{timestamp}] Node{node_id} [DEBUG] {message}")

def log_error(node_id: int, message: str):
    timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    print(f"[{timestamp}] Node{node_id} [ERROR] {message}")

def log_warning(node_id: int, message: str):
    timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    print(f"[{timestamp}] Node{node_id} [WARN] {message}")

class Node:
    def __init__(self, node_id: int):
        validate_node_id(node_id)
        
        self.node_id = node_id
        self.cluster_id = get_cluster_for_node(node_id)
        self.peers = get_peers_in_cluster(node_id)
        
        self.server = NodeServer(node_id)
        self.db = self.server.db
        
        # Paxos state
        self.current_ballot = 0
        self.current_leader = None
        self.last_heartbeat_time = time.time() + 10.0
        self.is_candidate = False
        
        self.promises_received = set()
        self.accepts_received = defaultdict(set)
        self.pending_proposals = {}
        
        self.next_seq = 1
        self.last_executed_seq = 0
        self.seq_lock = asyncio.Lock()
        
        self.peer_connections = {}
        
        self.heartbeat_task = None
        self.election_timer_task = None
        self.in_view_change = False
        
        # 2PC state tracking
        self.pending_2pc = {}  
        self.cluster_leader_connections = {}  
        
        # 2PC timeout tasks
        self.twopc_timeout_tasks = {}  
        self.lock_cleanup_task = None
        
        #Benchmark hooks
        self.benchmark_mode = False
        self.benchmark_stats = {
            'transactions_processed': 0,
            'cross_shard_transactions': 0,
            'intra_shard_transactions': 0,
            'aborted_transactions': 0,
            'committed_transactions': 0,
            'prepare_latencies': [],
            'commit_latencies': []
        }

    async def start(self):
        self.server.set_message_handler(self.route_message)
        await self.server.start()
        
        state = await self.db.get_current_ballot()
        if state:
            self.current_ballot = state
        
        state = await self.db.get_current_leader()
        if state:
            self.current_leader = state
        
        state = await self.db.get_next_sequence_number()
        if state:
            self.next_seq = state
        
        state = await self.db.get_last_executed_seq()
        if state:
            self.last_executed_seq = state
        
        initial_leaders = {1: 1, 2: 4, 3: 7}
        expected_leader = initial_leaders[self.cluster_id]
        
        if self.node_id == expected_leader:
            existing_view = await self.db.get_state('current_view')
            if not existing_view or existing_view == '0':
                self.current_ballot = 1
                await self.db.set_current_ballot(1)
                self.current_leader = self.node_id
                await self.db.set_current_leader(self.node_id)
                
                view_number = 1
                await self.db.set_state('current_view', str(view_number))
                await self.db.set_state('old_leader', str(self.node_id))
                
                try:
                    await self.db.log_view_change(view_number, None, self.node_id, 1)
                except:
                    pass
                
                log_info(self.node_id, f"*** INITIAL LEADER for cluster {self.cluster_id} ***")
            else:
                self.current_ballot = await self.db.get_current_ballot()
                self.current_leader = self.node_id
                #log_info(self.node_id, f"*** RESUMING as LEADER for cluster {self.cluster_id} ***")
            
            if not self.heartbeat_task or self.heartbeat_task.done():
                self.heartbeat_task = asyncio.create_task(self.heartbeat_loop())
                #log_info(self.node_id, f"Started heartbeat task")
        else:
            self.current_ballot = 1
            self.current_leader = expected_leader
            await self.db.set_current_ballot(1)
            await self.db.set_current_leader(expected_leader)
            self.last_heartbeat_time = time.time() + 10.0
            
            #log_info(self.node_id, f"Follower - expecting leader Node{expected_leader}")
        
        self.election_timer_task = asyncio.create_task(self.election_timer_loop())

        self.lock_cleanup_task = asyncio.create_task(self.periodic_lock_cleanup())
        
        #log_info(self.node_id, f"Node started in cluster {self.cluster_id}")
    
    async def stop(self):
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        if self.election_timer_task:
            self.election_timer_task.cancel()
        
        for task in self.twopc_timeout_tasks.values():
            task.cancel()

        if self.lock_cleanup_task:
            self.lock_cleanup_task.cancel()
        
        for writer in self.peer_connections.values():
            writer.close()
            await writer.wait_closed()
        
        for writer in self.cluster_leader_connections.values():
            writer.close()
            await writer.wait_closed()
        
        await self.server.stop()
        #log_info(self.node_id, "Node stopped")
    
    async def route_message(self, message: Dict[str, Any], writer):
        msg_type = message['type']
        
        if self.server.is_failed() and msg_type != MSG_RECOVER:
            return
        
        handlers = {
            MSG_TRANSACTION: self.handle_transaction,
            MSG_BALANCE: self.handle_balance,
            MSG_FAIL: self.handle_fail,
            MSG_RECOVER: self.handle_recover,
            MSG_FLUSH: self.handle_flush,
            MSG_PREPARE: self.handle_prepare,
            MSG_PROMISE: self.handle_promise,
            MSG_ACCEPT: self.handle_accept,
            MSG_ACCEPTED: self.handle_accepted,
            MSG_COMMIT: self.handle_commit,
            MSG_HEARTBEAT: self.handle_heartbeat,
            # 2PC message handlers
            MSG_2PC_PREPARE: self.handle_2pc_prepare,
            MSG_2PC_PREPARED: self.handle_2pc_prepared,
            MSG_2PC_COMMIT: self.handle_2pc_commit,
            MSG_2PC_ABORT: self.handle_2pc_abort_message,
            MSG_2PC_ACK: self.handle_2pc_ack,
            MSG_PRINT_BALANCE: self.handle_print_balance,
            MSG_PRINT_DB: self.handle_print_db,
            MSG_PRINT_VIEW: self.handle_print_view,
            MSG_PERFORMANCE: self.handle_performance,
            'PERFORMANCE_QUERY': self.handle_performance_query,
            'BENCHMARK_START': self.handle_benchmark_start,
            'BENCHMARK_STOP': self.handle_benchmark_stop,
            'BENCHMARK_STATS': self.handle_benchmark_stats,
            'BENCHMARK_RESET': self.handle_benchmark_reset,
        }
        
        handler = handlers.get(msg_type)
        if handler:
            await handler(message, writer)
        else:
            log_warning(self.node_id, f"Unknown message type: {msg_type}")
    
    # ============================================================
    # PAXOS: LEADER ELECTION
    # ============================================================
    
    async def election_timer_loop(self):
        while True:
            await asyncio.sleep(0.1)
            
            if self.server.is_failed():
                continue
            
            if self.current_leader == self.node_id:
                continue
            
            elapsed = time.time() - self.last_heartbeat_time
            if elapsed > ELECTION_TIMEOUT:
                log_warning(self.node_id, f"Election timeout ({elapsed:.1f}s)")
                await self.start_election()
    
    async def start_election(self):
        if self.is_candidate:
            return
        
        if self.current_leader and self.current_leader != self.node_id:
            elapsed = time.time() - self.last_heartbeat_time
            if elapsed < ELECTION_TIMEOUT:
                #log_debug(self.node_id, f"Leader Node{self.current_leader} still active, not starting election")
                return

        backoff = random.uniform(0.1, 0.8)
        await asyncio.sleep(backoff)
        
        self.is_candidate = True
        self.in_view_change = True
        self.current_ballot += 1
        await self.db.set_current_ballot(self.current_ballot)
        
        #log_info(self.node_id, f"Starting election with ballot {self.current_ballot}")
        
        self.promises_received = {self.node_id}
        self.collected_proposals = {}
        
        prepare_msg = Message.prepare(self.node_id, self.current_ballot, self.cluster_id)
        await self.broadcast_to_peers(prepare_msg)
        
        await asyncio.sleep(1.0)
        
        if len(self.promises_received) >= MAJORITY:
            await self.become_leader()
        else:
            log_warning(self.node_id, f"Election failed, only {len(self.promises_received)} promises")
            self.is_candidate = False
            self.in_view_change = False
    
    async def handle_prepare(self, message: Dict[str, Any], writer):
        ballot = message['ballot_number']
        from_node = message['from']
        
        log_debug(self.node_id, f"Received PREPARE from Node{from_node} with ballot {ballot}")
        
        if ballot > self.current_ballot:
            self.current_ballot = ballot
            await self.db.set_current_ballot(ballot)
            
            self.current_leader = None
            await self.db.set_current_leader(None)
            self.last_heartbeat_time = time.time()
            
            if self.heartbeat_task:
                self.heartbeat_task.cancel()
                self.heartbeat_task = None
            
            #log_info(self.node_id, f"Accepted PREPARE from Node{from_node}, ballot now {ballot}")
            
            accepted_proposals = await self.db.get_all_log_entries()
            
            promise_msg = Message.promise(self.node_id, ballot, accepted_proposals)
            await self.send_to_peer(from_node, promise_msg)
    
    async def handle_promise(self, message: Dict[str, Any], writer):
        if not self.is_candidate:
            return
        
        ballot = message['ballot_number']
        from_node = message['from']
        
        if ballot != self.current_ballot:
            return
        
        log_debug(self.node_id, f"Received PROMISE from Node{from_node}")
        
        self.promises_received.add(from_node)
        
        accepted_proposals = message.get('accepted_proposals', [])
        for proposal in accepted_proposals:
            seq = proposal['sequence_number']
            if seq not in self.collected_proposals:
                self.collected_proposals[seq] = proposal
            else:
                if proposal['ballot_number'] > self.collected_proposals[seq]['ballot_number']:
                    self.collected_proposals[seq] = proposal
        
        if (len(self.promises_received) >= MAJORITY and 
            self.current_leader != self.node_id and 
            self.is_candidate):  
            await self.become_leader()
    
    async def become_leader(self):

        if not self.is_candidate:
            #log_warning(self.node_id, "become_leader called but not a candidate, ignoring")
            return

        self.is_candidate = False
        self.current_leader = self.node_id
        await self.db.set_current_leader(self.node_id)
        
        old_leader = await self.db.get_state('old_leader')
        old_leader_id = int(old_leader) if old_leader and old_leader != 'none' else None
        
        view_number = await self.db.get_state('current_view')
        view_number = int(view_number) if view_number else 0
        view_number += 1
        await self.db.set_state('current_view', str(view_number))
        await self.db.set_state('old_leader', str(self.node_id))
        
        await self.db.log_view_change(view_number, old_leader_id, self.node_id, self.current_ballot)
        
        #log_info(self.node_id, f"*** BECAME LEADER (view={view_number}) with ballot {self.current_ballot} ***")
        
        await self.fill_log_gaps()
        
        self.in_view_change = False
        
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass
    
        self.heartbeat_task = asyncio.create_task(self.heartbeat_loop())
        #log_info(self.node_id, f"Started heartbeat task as new leader")
    
    async def fill_log_gaps(self):
        if not self.collected_proposals:
           # log_info(self.node_id, "No proposals to reconcile")
            return
        
        max_seq = max(self.collected_proposals.keys())
        
        #log_info(self.node_id, f"Reconciling log up to seq={max_seq}")
        
        for seq in range(self.last_executed_seq + 1, max_seq + 1):
            if seq in self.collected_proposals:
                proposal = self.collected_proposals[seq]
                #log_info(self.node_id, f"Re-proposing seq={seq} from view change")
                
                await self.db.log_accepted(
                    seq,
                    self.current_ballot,
                    proposal['transaction_id'],
                    proposal['transaction_type'],
                    proposal['transaction_data'],
                    proposal.get('phase'),
                    proposal.get('role')
                )
                
                self.pending_proposals[seq] = {
                    'request_id': proposal['transaction_id'],
                    'writer': None,
                    'transaction': proposal['transaction_data'],
                    'timestamp': time.time(),
                    'committed': False,
                    'already_executed': False
                }
                
                self.accepts_received[seq] = {self.node_id}
                
                accept_msg = Message.accept(
                    self.node_id,
                    self.current_ballot,
                    seq,
                    proposal['transaction_data'],
                    proposal.get('phase'),
                    proposal.get('role')
                )
                #log_info(self.node_id, f"Broadcasting ACCEPT for recovered seq={seq}")
                await self.broadcast_to_peers(accept_msg)
                
                await asyncio.sleep(0.5)
                
                if len(self.accepts_received[seq]) >= MAJORITY:
                    #log_info(self.node_id, f"Got majority for recovered seq={seq}, committing")
                    commit_msg = Message.commit(self.node_id, seq)
                    await self.broadcast_to_peers(commit_msg)
                    await self.mark_as_committed(seq)
                else:
                    log_warning(self.node_id, f"Failed to get majority for seq={seq}, using NO-OP")
                    noop_txn = {
                        'type': 'NO-OP',
                        'reason': 'failed_to_commit_in_view_change'
                    }
                    
                    await self.db.log_accepted(
                        seq,
                        self.current_ballot,
                        f'noop-{seq}',
                        'noop',
                        noop_txn,
                        None,
                        None
                    )
                    
                    self.pending_proposals[seq] = {
                        'request_id': f'noop-{seq}',
                        'writer': None,
                        'transaction': noop_txn,
                        'timestamp': time.time(),
                        'committed': True,
                        'already_executed': False
                    }
                    
                    accept_msg = Message.accept(
                        self.node_id,
                        self.current_ballot,
                        seq,
                        noop_txn,
                        None,
                        None
                    )
                    await self.broadcast_to_peers(accept_msg)
            else:
                log_warning(self.node_id, f"Gap detected at seq={seq}, inserting NO-OP")
                
                noop_txn = {
                    'type': 'NO-OP',
                    'reason': 'gap_fill'
                }
                
                await self.db.log_accepted(
                    seq,
                    self.current_ballot,
                    f'noop-{seq}',
                    'noop',
                    noop_txn,
                    None,
                    None
                )
                
                self.pending_proposals[seq] = {
                    'request_id': f'noop-{seq}',
                    'writer': None,
                    'transaction': noop_txn,
                    'timestamp': time.time(),
                    'committed': True,
                    'already_executed': False
                }
                
                accept_msg = Message.accept(
                    self.node_id,
                    self.current_ballot,
                    seq,
                    noop_txn,
                    None,
                    None
                )
                await self.broadcast_to_peers(accept_msg)
        
        if max_seq >= self.next_seq:
            self.next_seq = max_seq + 1
            await self.db.set_state('next_sequence_number', str(self.next_seq))
        
        #log_info(self.node_id, f"Log reconciliation complete, next_seq={self.next_seq}")
        
        await self.try_execute_next_in_order()
    
    async def heartbeat_loop(self):
        #log_info(self.node_id, "Heartbeat loop started")
        
        while True:
            if self.current_leader != self.node_id:
                #log_info(self.node_id, "Heartbeat stopped: no longer leader")
                break
            
            if self.server.is_failed():
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                continue
            
            heartbeat_msg = Message.heartbeat(
                self.node_id,
                self.current_ballot,
                self.cluster_id
            )
            
            await self.broadcast_to_peers(heartbeat_msg)
            
            await asyncio.sleep(HEARTBEAT_INTERVAL)
        
        #log_info(self.node_id, "Heartbeat loop exited")

    async def periodic_lock_cleanup(self):
        #log_info(self.node_id, "Lock cleanup task started")
        
        while True:
            try:
                await asyncio.sleep(10)  
                
                if self.server.is_failed():
                    continue
                
                count = await self.db.cleanup_expired_locks(timeout=10.0)
                
                if count > 0:
                    #log_warning(self.node_id, f"[CLEANUP] Released {count} stale locks")
                    
                    current_time = time.time()
                    stale_txns = []
                    
                    for txn_id, state in self.pending_2pc.items():
                        age = current_time - state.get('start_time', current_time)
                        if age > 15.0:  
                            stale_txns.append(txn_id)
                    
                    for txn_id in stale_txns:
                        #log_warning(self.node_id, f"[CLEANUP] Removing stale 2PC transaction {txn_id[:8]}")
                        del self.pending_2pc[txn_id]
            
            except asyncio.CancelledError:
                #log_info(self.node_id, "Lock cleanup task cancelled")
                break
            except Exception as e:
                log_error(self.node_id, f"Error in lock cleanup: {e}")

    

    def enable_benchmark_mode(self):
        self.benchmark_mode = True
        self.benchmark_stats = {
            'transactions_processed': 0,
            'cross_shard_transactions': 0,
            'intra_shard_transactions': 0,
            'aborted_transactions': 0,
            'committed_transactions': 0,
            'prepare_latencies': [],
            'commit_latencies': []
        }
        #log_info(self.node_id, "Benchmark mode enabled")

    def disable_benchmark_mode(self):
        self.benchmark_mode = False
        #log_info(self.node_id, "Benchmark mode disabled")

    def get_benchmark_stats(self) -> Dict[str, Any]:
        return self.benchmark_stats.copy()

    def reset_benchmark_stats(self):
        self.benchmark_stats = {
            'transactions_processed': 0,
            'cross_shard_transactions': 0,
            'intra_shard_transactions': 0,
            'aborted_transactions': 0,
            'committed_transactions': 0,
            'prepare_latencies': [],
            'commit_latencies': []
        }
        #log_info(self.node_id, "Benchmark stats reset")
    
    async def handle_heartbeat(self, message: Dict[str, Any], writer):
        from_node = message['from']
        ballot = message['ballot_number']

        if ballot >= self.current_ballot:
            self.current_ballot = ballot
            self.current_leader = from_node
            self.last_heartbeat_time = time.time()
            
            await self.db.set_current_ballot(ballot)
            await self.db.set_current_leader(from_node)
            
            if self.heartbeat_task:
                self.heartbeat_task.cancel()
                self.heartbeat_task = None
    
    # ============================================================
    # TRANSACTION HANDLING: INTRA-SHARD vs CROSS-SHARD
    # ============================================================
    
    async def handle_transaction(self, message: Dict[str, Any], writer):
        txn = message['transaction']
        request_id = message['request_id']
        
        sender = txn['sender']
        receiver = txn['receiver']
        amount = txn['amount']

        if self.benchmark_mode:
            self.benchmark_stats['transactions_processed'] += 1
            if sender_cluster != receiver_cluster:
                self.benchmark_stats['cross_shard_transactions'] += 1
            else:
                self.benchmark_stats['intra_shard_transactions'] += 1
        
        #log_info(self.node_id, f"Transaction request: {sender}->{receiver} (${amount})")
        
        sender_cluster = get_cluster_for_account(sender)
        receiver_cluster = get_cluster_for_account(receiver)
        
        if sender_cluster != self.cluster_id:
            await self.server.send_error(writer, request_id, "Wrong cluster for sender")
            return
        
        if self.current_leader != self.node_id:
            await self.server.send_error(writer, request_id, "Not leader")
            return
        
        if self.in_view_change:
            await self.server.send_error(writer, request_id, "View change in progress")
            return
        
        if sender_cluster != receiver_cluster:
            #log_info(self.node_id, f"[2PC] Cross-shard transaction detected: C{sender_cluster}->C{receiver_cluster}")
            await self.handle_cross_shard_transaction(message, writer)
            return
        
        await self.handle_intra_shard_transaction(message, writer)
    
    async def handle_intra_shard_transaction(self, message: Dict[str, Any], writer):
        txn = message['transaction']
        request_id = message['request_id']
        
        sender = txn['sender']
        receiver = txn['receiver']
        amount = txn['amount']
        
        sender_balance = await self.db.get_balance(sender)
        if sender_balance is None or sender_balance < amount:
            log_warning(self.node_id, f"Rejecting: insufficient balance (sender {sender} has {sender_balance})")
            await self.server.send_response(writer, request_id, 'failed', 
                                           message='Insufficient balance')
            return
        
        receiver_balance = await self.db.get_balance(receiver)
        if receiver_balance is None:
            await self.server.send_error(writer, request_id, "Receiver account not found")
            return
        
        async with self.seq_lock:
            seq = self.next_seq
            self.next_seq += 1
            await self.db.set_state('next_sequence_number', str(self.next_seq))
        
        self.pending_proposals[seq] = {
            'request_id': request_id,
            'writer': writer,
            'transaction': txn,
            'timestamp': time.time(),
            'committed': False,
            'already_executed': False
        }
        
        await self.db.log_accepted(
            seq,
            self.current_ballot,
            request_id,
            'transfer',
            txn,
            None,
            None
        )
        
        accept_msg = Message.accept(
            self.node_id,
            self.current_ballot,
            seq,
            txn,
            None,
            None
        )
        
        self.accepts_received[seq] = {self.node_id}
        
        #log_info(self.node_id, f"[PAXOS] Broadcasting ACCEPT for seq={seq}")
        await self.broadcast_to_peers(accept_msg)
    
    # ============================================================
    # 2PC: CROSS-SHARD TRANSACTIONS - COORDINATOR ROLE
    # ============================================================
    
    async def handle_cross_shard_transaction(self, message: Dict[str, Any], writer):
        txn = message['transaction']
        request_id = message['request_id']
        
        sender = txn['sender']
        receiver = txn['receiver']
        amount = txn['amount']
        
        sender_cluster = get_cluster_for_account(sender)
        receiver_cluster = get_cluster_for_account(receiver)
        
        transaction_id = str(uuid.uuid4())
        
        #log_info(self.node_id, f"[2PC-COORD] Starting 2PC for txn {transaction_id[:8]}")

        if self.benchmark_mode:
            prepare_start_time = time.time()
        
        is_locked = await self.db.is_locked(sender)
        if is_locked:
            log_warning(self.node_id, f"[2PC-COORD] Sender {sender} is locked, rejecting")
            await self.server.send_response(writer, request_id, 'failed',
                                           message='Sender account is locked')
            return
        
        sender_balance = await self.db.get_balance(sender)
        if sender_balance is None or sender_balance < amount:
            log_warning(self.node_id, f"[2PC-COORD] Insufficient balance: {sender_balance} < {amount}")
            await self.server.send_response(writer, request_id, 'failed',
                                           message='Insufficient balance')
            return
        
        lock_acquired = await self.db.acquire_lock(sender, transaction_id, 'exclusive')
        if not lock_acquired:
            log_warning(self.node_id, f"[2PC-COORD] Failed to acquire lock on sender {sender}")
            await self.server.send_response(writer, request_id, 'failed',
                                           message='Failed to acquire lock')
            return
        
        #log_info(self.node_id, f"[2PC-COORD] Locked sender {sender}")
        
        async with self.seq_lock:
            seq = self.next_seq
            self.next_seq += 1
            await self.db.set_state('next_sequence_number', str(self.next_seq))
        
        self.pending_2pc[transaction_id] = {
            'request_id': request_id,
            'writer': writer,
            'transaction': txn,
            'sequence_number': seq,
            'phase': 'prepare',
            'role': 'coordinator',
            'participant_cluster': receiver_cluster,
            'start_time': time.time(),
            'prepared_received': False,
            'commit_sent': False,
            'ack_received': False
        }
        
        #log_info(self.node_id, f"[2PC-COORD] Running Paxos PREPARE phase at seq={seq}")
        
        await self.db.log_accepted(
            seq,
            self.current_ballot,
            transaction_id,
            'cross_shard_transfer',
            txn,
            'P',  
            'coordinator'
        )
        
        accept_msg = Message.accept(
            self.node_id,
            self.current_ballot,
            seq,
            txn,
            'P',
            'coordinator'
        )
        
        self.accepts_received[seq] = {self.node_id}
        await self.broadcast_to_peers(accept_msg)
        
        await asyncio.sleep(0.5)  
        
        if len(self.accepts_received[seq]) < MAJORITY:
            log_error(self.node_id, f"[2PC-COORD] Failed to get majority for prepare phase")
            await self.abort_2pc_coordinator(transaction_id, "Consensus failed")
            return
        
        old_balance = sender_balance
        new_balance = sender_balance - amount
        
        await self.db.write_wal_entry(
            transaction_id,
            sender,
            old_balance,
            new_balance,
            'debit'
        )
        
        await self.db.update_balance(sender, new_balance)
        await self.db.mark_executed(seq)
        
        #log_info(self.node_id, f"[2PC-COORD] Executed prepare: {sender} balance {old_balance}->{new_balance}")

        if self.benchmark_mode:
            prepare_latency = time.time() - prepare_start_time
            self.benchmark_stats['prepare_latencies'].append(prepare_latency)
        
        prepare_msg = Message.two_pc_prepare(
            self.node_id,
            transaction_id,
            txn,
            request_id,
            self.cluster_id
        )
        
        #log_info(self.node_id, f"[2PC-COORD] Sending PREPARE to cluster {receiver_cluster}")
        
        success = await self.send_to_cluster_leader(receiver_cluster, prepare_msg)
        
        if not success:
            log_error(self.node_id, f"[2PC-COORD] Failed to send PREPARE to participant")
            await self.abort_2pc_coordinator(transaction_id, "Failed to contact participant")
            return
        
        timeout_task = asyncio.create_task(
            self.coordinator_timeout_handler(transaction_id, 5.0)
        )
        self.twopc_timeout_tasks[transaction_id] = timeout_task
    
    async def coordinator_timeout_handler(self, transaction_id: str, timeout: float):
        await asyncio.sleep(timeout)
        
        if transaction_id not in self.pending_2pc:
            return
        
        state = self.pending_2pc[transaction_id]
        
        if not state.get('prepared_received', False):
            log_warning(self.node_id, f"[2PC-COORD] Timeout waiting for PREPARED from participant")
            await self.abort_2pc_coordinator(transaction_id, "Participant timeout")
    
    async def handle_2pc_prepared(self, message: Dict[str, Any], writer):
        transaction_id = message['transaction_id']
        from_cluster = message['from_cluster']
        from_node = message['from']
        
        #log_info(self.node_id, f"[2PC-COORD] Received PREPARED from Node{from_node} (C{from_cluster})")
        
        if transaction_id not in self.pending_2pc:
            log_warning(self.node_id, f"[2PC-COORD] Unknown transaction {transaction_id[:8]}")
            return
        
        state = self.pending_2pc[transaction_id]
        state['prepared_received'] = True
        
        if transaction_id in self.twopc_timeout_tasks:
            self.twopc_timeout_tasks[transaction_id].cancel()
            del self.twopc_timeout_tasks[transaction_id]
        
        await self.commit_2pc_coordinator(transaction_id)
    
    async def handle_2pc_abort_message(self, message: Dict[str, Any], writer):
        transaction_id = message['transaction_id']
        from_cluster = message['from_cluster']
        from_node = message['from']
        reason = message.get('reason', 'Unknown')
        
        log_warning(self.node_id, f"[2PC] Received ABORT from Node{from_node}: {reason}")
        
        if transaction_id not in self.pending_2pc:
            log_debug(self.node_id, f"[2PC] Transaction {transaction_id[:8]} not tracked, ignoring")
            return
        
        state = self.pending_2pc[transaction_id]
        
        if state['role'] == 'coordinator':
            #log_info(self.node_id, f"[2PC-COORD] Participant aborted, aborting coordinator too")
            await self.abort_2pc_coordinator(transaction_id, f"Participant aborted: {reason}")
        
        elif state['role'] == 'participant':
            #log_info(self.node_id, f"[2PC-PART] Received ABORT decision from coordinator")
            await self.abort_2pc_participant(transaction_id)
    
    async def commit_2pc_coordinator(self, transaction_id: str):
        if self.benchmark_mode:
            commit_start_time = time.time()

        if transaction_id not in self.pending_2pc:
            return
        
        state = self.pending_2pc[transaction_id]
        seq = state['sequence_number']
        txn = state['transaction']
        participant_cluster = state['participant_cluster']
        
        #log_info(self.node_id, f"[2PC-COORD] Starting COMMIT phase for seq={seq}")
        
        await self.db.log_accepted(
            seq,
            self.current_ballot,
            transaction_id,
            'cross_shard_transfer',
            txn,
            'C',  
            'coordinator'
        )
        
        accept_msg = Message.accept(
            self.node_id,
            self.current_ballot,
            seq,
            txn,
            'C',
            'coordinator'
        )
        
        self.accepts_received[seq] = {self.node_id}
        await self.broadcast_to_peers(accept_msg)
        
        await asyncio.sleep(0.5)
        
        if len(self.accepts_received[seq]) < MAJORITY:
            log_error(self.node_id, f"[2PC-COORD] Failed to get majority for commit phase")
            return
        
        
        #log_info(self.node_id, f"[2PC-COORD] Commit phase consensus achieved")

        if self.benchmark_mode:
            commit_latency = time.time() - commit_start_time
            self.benchmark_stats['commit_latencies'].append(commit_latency)
            self.benchmark_stats['committed_transactions'] += 1
        
        commit_msg = Message.two_pc_commit(
            self.node_id,
            transaction_id,
            self.cluster_id
        )
        
        state['commit_sent'] = True
        
        #log_info(self.node_id, f"[2PC-COORD] Sending COMMIT to cluster {participant_cluster}")
        await self.send_to_cluster_leader(participant_cluster, commit_msg)
        
        sender = txn['sender']
        await self.db.release_lock(sender, transaction_id)
        
        await self.db.delete_wal_entries(transaction_id)
        
        #log_info(self.node_id, f"[2PC-COORD] Released lock on {sender}, deleted WAL")
        
        writer = state.get('writer')
        request_id = state['request_id']
        
        if writer:
            await self.server.send_response(
                writer,
                request_id,
                'success',
                message='Cross-shard transaction committed'
            )
        
        asyncio.create_task(self.wait_for_ack_with_retry(transaction_id, participant_cluster))
    
    async def wait_for_ack_with_retry(self, transaction_id: str, participant_cluster: int):
        max_retries = 5
        retry_interval = 1.0
        
        for i in range(max_retries):
            await asyncio.sleep(retry_interval)
            
            if transaction_id not in self.pending_2pc:
                return
            
            state = self.pending_2pc[transaction_id]
            
            if state.get('ack_received', False):
                #log_info(self.node_id, f"[2PC-COORD] ACK received, cleaning up")
                del self.pending_2pc[transaction_id]
                return
            
            #log_info(self.node_id, f"[2PC-COORD] Retry {i+1}/{max_retries}: Re-sending COMMIT")
            commit_msg = Message.two_pc_commit(
                self.node_id,
                transaction_id,
                self.cluster_id
            )
            await self.send_to_cluster_leader(participant_cluster, commit_msg)
        
        #log_warning(self.node_id, f"[2PC-COORD] No ACK after {max_retries} retries, giving up")
        if transaction_id in self.pending_2pc:
            del self.pending_2pc[transaction_id]
    
    async def abort_2pc_coordinator(self, transaction_id: str, reason: str):
        if self.benchmark_mode:
            self.benchmark_stats['aborted_transactions'] += 1

        if transaction_id not in self.pending_2pc:
            return
        
        state = self.pending_2pc[transaction_id]
        seq = state['sequence_number']
        txn = state['transaction']
        participant_cluster = state['participant_cluster']
        sender = txn['sender']
        
        log_warning(self.node_id, f"[2PC-COORD] Aborting transaction: {reason}")
        
        await self.db.log_accepted(
            seq,
            self.current_ballot,
            transaction_id,
            'cross_shard_transfer',
            txn,
            'A', 
            'coordinator'
        )
        
        accept_msg = Message.accept(
            self.node_id,
            self.current_ballot,
            seq,
            txn,
            'A',
            'coordinator'
        )
        
        self.accepts_received[seq] = {self.node_id}
        await self.broadcast_to_peers(accept_msg)
        
        await asyncio.sleep(0.5)
        
        undone = await self.db.undo_from_wal(transaction_id)
        #log_info(self.node_id, f"[2PC-COORD] Undone {undone} operations from WAL")
        
        released = await self.db.release_lock(sender, transaction_id)
        if not released:
            log_error(self.node_id, f"[2PC-COORD] Failed to release lock on {sender}")
        else:
            log_info(self.node_id, f"[2PC-COORD] Successfully released lock on {sender}")

        deleted = await self.db.delete_wal_entries(transaction_id)
        #log_info(self.node_id, f"[2PC-COORD] Deleted {deleted} WAL entries")

        log_info(self.node_id, f"[2PC-COORD] Cleanup complete for transaction {transaction_id[:8]}")
        
        if state.get('prepared_received', False):
            abort_msg = Message.two_pc_abort(
                self.node_id,
                transaction_id,
                self.cluster_id,
                reason
            )
            await self.send_to_cluster_leader(participant_cluster, abort_msg)
        
        writer = state.get('writer')
        request_id = state['request_id']
        
        if writer:
            await self.server.send_response(
                writer,
                request_id,
                'failed',
                message=f'Transaction aborted: {reason}'
            )
        
        del self.pending_2pc[transaction_id]
        
        if transaction_id in self.twopc_timeout_tasks:
            self.twopc_timeout_tasks[transaction_id].cancel()
            del self.twopc_timeout_tasks[transaction_id]
    
    async def handle_2pc_ack(self, message: Dict[str, Any], writer):
        transaction_id = message['transaction_id']
        from_node = message['from']
        phase = message['phase']
        
        log_info(self.node_id, f"[2PC-COORD] Received ACK from Node{from_node} for {phase} phase")
        
        if transaction_id in self.pending_2pc:
            self.pending_2pc[transaction_id]['ack_received'] = True
    
    # ============================================================
    # 2PC: CROSS-SHARD TRANSACTIONS - PARTICIPANT ROLE
    # ============================================================
    
    async def handle_2pc_prepare(self, message: Dict[str, Any], writer):
        transaction_id = message['transaction_id']
        txn = message['transaction']
        request_id = message['request_id']
        from_node = message['from']
        from_cluster = message['from_cluster']
        
        sender = txn['sender']
        receiver = txn['receiver']
        amount = txn['amount']
        
        log_info(self.node_id, f"[2PC-PART] Received PREPARE from Node{from_node} (C{from_cluster})")
        log_info(self.node_id, f"[2PC-PART] Transaction: {sender}->{receiver} (${amount})")
        
        if self.current_leader != self.node_id:
            log_warning(self.node_id, f"[2PC-PART] Not leader (leader is Node{self.current_leader}), forwarding")
            
            if self.current_leader:
                await self.send_to_peer(self.current_leader, message)
                log_info(self.node_id, f"[2PC-PART] Forwarded PREPARE to leader Node{self.current_leader}")
            else:
                abort_msg = Message.two_pc_abort(
                    self.node_id,
                    transaction_id,
                    self.cluster_id,
                    "No leader available"
                )
                await self.send_to_cluster_leader(from_cluster, abort_msg)
            return
        
        is_locked = await self.db.is_locked(receiver)
        if is_locked:
            log_warning(self.node_id, f"[2PC-PART] Receiver {receiver} is locked, aborting")
            
            async with self.seq_lock:
                seq = self.next_seq
                self.next_seq += 1
                await self.db.set_state('next_sequence_number', str(self.next_seq))
            
            await self.db.log_accepted(
                seq,
                self.current_ballot,
                transaction_id,
                'cross_shard_transfer',
                txn,
                'A',
                'participant'
            )
            
            accept_msg = Message.accept(
                self.node_id,
                self.current_ballot,
                seq,
                txn,
                'A',
                'participant'
            )
            
            self.accepts_received[seq] = {self.node_id}
            await self.broadcast_to_peers(accept_msg)
            
            abort_msg = Message.two_pc_abort(
                self.node_id,
                transaction_id,
                self.cluster_id,
                "Receiver is locked"
            )
            await self.send_to_cluster_leader(from_cluster, abort_msg)
            return
        
        lock_acquired = await self.db.acquire_lock(receiver, transaction_id, 'exclusive')
        if not lock_acquired:
            log_error(self.node_id, f"[2PC-PART] Failed to acquire lock on {receiver}")
            
            abort_msg = Message.two_pc_abort(
                self.node_id,
                transaction_id,
                self.cluster_id,
                "Failed to acquire lock"
            )
            await self.send_to_cluster_leader(from_cluster, abort_msg)
            return
        
        log_info(self.node_id, f"[2PC-PART] Locked receiver {receiver}")
        
        async with self.seq_lock:
            seq = self.next_seq
            self.next_seq += 1
            await self.db.set_state('next_sequence_number', str(self.next_seq))
        
        self.pending_2pc[transaction_id] = {
            'request_id': request_id,
            'transaction': txn,
            'sequence_number': seq,
            'phase': 'prepare',
            'role': 'participant',
            'coordinator_cluster': from_cluster,
            'coordinator_node': from_node,
            'start_time': time.time()
        }
        
        log_info(self.node_id, f"[2PC-PART] Running Paxos PREPARE phase at seq={seq}")
        
        await self.db.log_accepted(
            seq,
            self.current_ballot,
            transaction_id,
            'cross_shard_transfer',
            txn,
            'P',
            'participant'
        )
        
        accept_msg = Message.accept(
            self.node_id,
            self.current_ballot,
            seq,
            txn,
            'P',
            'participant'
        )
        
        self.accepts_received[seq] = {self.node_id}
        await self.broadcast_to_peers(accept_msg)
        
        for attempt in range(3):
            await asyncio.sleep(0.3)
            if len(self.accepts_received[seq]) >= MAJORITY:
                break

        if len(self.accepts_received[seq]) < MAJORITY:
            log_error(self.node_id, f"[2PC-PART] Failed to get majority for prepare phase (got {len(self.accepts_received[seq])}/{NODES_PER_CLUSTER})")
            
            await self.db.log_accepted(
                seq,
                self.current_ballot,
                transaction_id,
                'cross_shard_transfer',
                txn,
                'A',
                'participant'
            )
            
            accept_msg = Message.accept(
                self.node_id,
                self.current_ballot,
                seq,
                txn,
                'A',
                'participant'
            )
            
            self.accepts_received[seq] = {self.node_id}
            await self.broadcast_to_peers(accept_msg)
            
            await asyncio.sleep(0.5)  
            
            released = await self.db.release_lock(receiver, transaction_id)
            log_info(self.node_id, f"[2PC-PART] Released lock on {receiver} (success={released})")
            
            await self.db.delete_wal_entries(transaction_id)
            
            abort_msg = Message.two_pc_abort(
                self.node_id,
                transaction_id,
                self.cluster_id,
                "Consensus failed"
            )
            await self.send_to_cluster_leader(from_cluster, abort_msg)
            
            del self.pending_2pc[transaction_id]
            return
        
        old_balance = await self.db.get_balance(receiver)
        new_balance = old_balance + amount
        
        await self.db.write_wal_entry(
            transaction_id,
            receiver,
            old_balance,
            new_balance,
            'credit'
        )
        
        await self.db.update_balance(receiver, new_balance)
        await self.db.mark_executed(seq)
        
        log_info(self.node_id, f"[2PC-PART] Executed prepare: {receiver} balance {old_balance}->{new_balance}")
        
        prepared_msg = Message.two_pc_prepared(
            self.node_id,
            transaction_id,
            self.cluster_id
        )

        coordinator_node = self.pending_2pc[transaction_id]['coordinator_node']

        log_info(self.node_id, f"[2PC-PART] Sending PREPARED to coordinator Node{coordinator_node} (C{from_cluster})")

        try:
            await self.send_to_peer(coordinator_node, prepared_msg)
            log_debug(self.node_id, f"[2PC-PART] Successfully sent PREPARED to Node{coordinator_node}")
        except Exception as e:
            log_warning(self.node_id, f"[2PC-PART] Failed to send to Node{coordinator_node}, trying cluster")
            await self.send_to_cluster_leader(from_cluster, prepared_msg)
    
    async def handle_2pc_commit(self, message: Dict[str, Any], writer):
        transaction_id = message['transaction_id']
        from_node = message['from']
        from_cluster = message['from_cluster']
        
        log_info(self.node_id, f"[2PC-PART] Received COMMIT from Node{from_node} (C{from_cluster})")
        
        if transaction_id not in self.pending_2pc:
            log_warning(self.node_id, f"[2PC-PART] Unknown transaction {transaction_id[:8]}, ignoring")
           
            ack_msg = Message.two_pc_ack(
                self.node_id,
                transaction_id,
                self.cluster_id,
                'commit'
            )
            await self.send_to_cluster_leader(from_cluster, ack_msg)
            return
        
        state = self.pending_2pc[transaction_id]
        seq = state['sequence_number']
        txn = state['transaction']
        receiver = txn['receiver']
        
        log_info(self.node_id, f"[2PC-PART] Running Paxos COMMIT phase at seq={seq}")
        
        await self.db.log_accepted(
            seq,
            self.current_ballot,
            transaction_id,
            'cross_shard_transfer',
            txn,
            'C',
            'participant'
        )
        
        accept_msg = Message.accept(
            self.node_id,
            self.current_ballot,
            seq,
            txn,
            'C',
            'participant'
        )
        
        self.accepts_received[seq] = {self.node_id}
        await self.broadcast_to_peers(accept_msg)
        
        await self.db.release_lock(receiver, transaction_id)

        await asyncio.sleep(0.5)
        
        await self.db.delete_wal_entries(transaction_id)
        
        log_info(self.node_id, f"[2PC-PART] Released lock on {receiver}, deleted WAL")
        
        ack_msg = Message.two_pc_ack(
            self.node_id,
            transaction_id,
            self.cluster_id,
            'commit'
        )
        
        log_info(self.node_id, f"[2PC-PART] Sending ACK to coordinator")
        await self.send_to_cluster_leader(from_cluster, ack_msg)
        
        del self.pending_2pc[transaction_id]
    
    async def abort_2pc_participant(self, transaction_id: str):
        if transaction_id not in self.pending_2pc:
            return
        
        state = self.pending_2pc[transaction_id]
        seq = state['sequence_number']
        txn = state['transaction']
        receiver = txn['receiver']
        coordinator_cluster = state['coordinator_cluster']
        
        log_warning(self.node_id, f"[2PC-PART] Aborting transaction")
        
        await self.db.log_accepted(
            seq,
            self.current_ballot,
            transaction_id,
            'cross_shard_transfer',
            txn,
            'A',
            'participant'
        )
        
        accept_msg = Message.accept(
            self.node_id,
            self.current_ballot,
            seq,
            txn,
            'A',
            'participant'
        )
        
        self.accepts_received[seq] = {self.node_id}
        await self.broadcast_to_peers(accept_msg)
        
        await asyncio.sleep(0.5)
        
        undone = await self.db.undo_from_wal(transaction_id)
        log_info(self.node_id, f"[2PC-PART] Undone {undone} operations from WAL")

        released = await self.db.release_lock(receiver, transaction_id)
        if not released:
            log_error(self.node_id, f"[2PC-PART] Failed to release lock on {receiver}")
        else:
            log_info(self.node_id, f"[2PC-PART] Released lock on {receiver}")

        await self.db.delete_wal_entries(transaction_id)

        log_info(self.node_id, f"[2PC-PART] Released lock on {receiver}, deleted WAL")
        
        ack_msg = Message.two_pc_ack(
            self.node_id,
            transaction_id,
            self.cluster_id,
            'abort'
        )
        await self.send_to_cluster_leader(coordinator_cluster, ack_msg)
        
        del self.pending_2pc[transaction_id]
    
    # ============================================================
    # INTER-CLUSTER COMMUNICATION
    # ============================================================
    
    async def send_to_cluster_leader(self, cluster_id: int, message: Dict[str, Any]) -> bool:
        if cluster_id == self.cluster_id:
            log_error(self.node_id, "Cannot send to own cluster")
            return False
        
        target_nodes = CLUSTER_NODES[cluster_id]
        
        for node_id in target_nodes:
            try:
                host, port = get_node_address(node_id)
                reader, writer = await asyncio.open_connection(host, port)
                
                data = MessageSerializer.serialize(message)
                writer.write(data)
                await writer.drain()
                
                writer.close()
                await writer.wait_closed()
                
                log_debug(self.node_id, f"Sent {message['type']} to Node{node_id} (C{cluster_id})")
                return True
            
            except Exception as e:
                log_debug(self.node_id, f"Failed to send to Node{node_id}: {e}")
                continue
        
        log_error(self.node_id, f"Failed to send to any node in cluster {cluster_id}")
        return False
    
    # ============================================================
    # PAXOS: CONSENSUS
    # ============================================================
    
    async def handle_accept(self, message: Dict[str, Any], writer):
        from_node = message['from']
        ballot = message['ballot_number']
        seq = message['sequence_number']
        txn = message['transaction']
        phase = message.get('phase')
        role = message.get('role')
        
        log_info(self.node_id, f"[PAXOS] Received ACCEPT from Node{from_node} seq={seq}, ballot={ballot}, phase={phase}")
        
        if ballot < self.current_ballot:
            log_warning(self.node_id, f"Rejected ACCEPT: old ballot {ballot} < {self.current_ballot}")
            return
        
        if ballot > self.current_ballot:
            self.current_ballot = ballot
            self.current_leader = from_node
            await self.db.set_current_ballot(ballot)
            await self.db.set_current_leader(from_node)
        
        request_id = txn.get('request_id', 'unknown')
        txn_type = txn.get('type', 'transfer')
        
        await self.db.log_accepted(
            seq,
            ballot,
            request_id,
            txn_type,
            txn,
            phase,
            role
        )
        
        accepted_msg = Message.accepted(self.node_id, ballot, seq)
        await self.send_to_peer(from_node, accepted_msg)
        
        log_info(self.node_id, f"[PAXOS] Sent ACCEPTED to Node{from_node} for seq={seq}")
    
    async def handle_accepted(self, message: Dict[str, Any], writer):
        if self.current_leader != self.node_id:
            return
        
        from_node = message['from']
        ballot = message['ballot_number']
        seq = message['sequence_number']
        
        if ballot != self.current_ballot:
            return
        
        log_info(self.node_id, f"[PAXOS] Received ACCEPTED from Node{from_node} for seq={seq}")
        
        self.accepts_received[seq].add(from_node)
        
        log_info(self.node_id, f"[PAXOS] Accepts for seq={seq}: {len(self.accepts_received[seq])}/{NODES_PER_CLUSTER}")
        
        if len(self.accepts_received[seq]) == MAJORITY:
            log_info(self.node_id, f"[PAXOS] MAJORITY REACHED for seq={seq}, committing")
            
            commit_msg = Message.commit(self.node_id, seq)
            await self.broadcast_to_peers(commit_msg)
            
            await self.mark_as_committed(seq)
            await self.try_execute_next_in_order()
    
    async def handle_commit(self, message: Dict[str, Any], writer):
        from_node = message['from']
        seq = message['sequence_number']
        
        log_info(self.node_id, f"[PAXOS] Received COMMIT from Node{from_node} for seq={seq}")
        
        if seq not in self.pending_proposals:
            entry = await self.db.get_log_entry(seq)
            if entry:
                self.pending_proposals[seq] = {
                    'request_id': entry['transaction_id'],
                    'writer': None,
                    'transaction': entry['transaction_data'],
                    'timestamp': time.time(),
                    'committed': True,
                    'already_executed': False
                }
                log_info(self.node_id, f"Created pending proposal from log for seq={seq}")
            else:
                log_warning(self.node_id, f"Received COMMIT for seq={seq} but no log entry found")
                return
        else:
            await self.mark_as_committed(seq)
        
        await self.try_execute_next_in_order()
    
    async def mark_as_committed(self, seq: int):
        if seq in self.pending_proposals:
            proposal = self.pending_proposals[seq]
            proposal['committed'] = True
            log_info(self.node_id, f"Marked seq={seq} as committed")
    
    async def try_execute_next_in_order(self):
        while True:
            next_seq = self.last_executed_seq + 1
            
            if next_seq not in self.pending_proposals:
                entry = await self.db.get_log_entry(next_seq)
                if entry and not entry['executed']:
                    self.pending_proposals[next_seq] = {
                        'request_id': entry['transaction_id'],
                        'writer': None,
                        'transaction': entry['transaction_data'],
                        'timestamp': time.time(),
                        'committed': True,
                        'already_executed': False
                    }
                    log_info(self.node_id, f"Added seq={next_seq} to pending from log")
                else:
                    break
            
            proposal = self.pending_proposals[next_seq]
            
            if not proposal.get('committed', False):
                break
            
            if proposal.get('already_executed', False):
                log_warning(self.node_id, f"seq={next_seq} already executed, skipping")
                del self.pending_proposals[next_seq]
                continue
            
            proposal['already_executed'] = True
            
            await self.execute_transaction(next_seq)

            if next_seq % CHECKPOINT_INTERVAL == 0:
                await self.create_checkpoint(next_seq)
            
            if self.current_leader == self.node_id:
                writer = proposal.get('writer')
                if writer is not None:
                    try:
                        if writer.is_closing():
                            log_debug(self.node_id, f"Writer already closed for seq={next_seq}, skipping response")
                        else:
                            await self.server.send_response(
                                writer,
                                proposal['request_id'],
                                'success',
                                message='Transaction committed'
                            )
                    except Exception as e:
                        log_debug(self.node_id, f"Response send failed for seq={next_seq}: {e}")
            
            del self.pending_proposals[next_seq]
            log_info(self.node_id, f"Completed seq={next_seq} in order")
    
    async def execute_transaction(self, seq: int):
        if seq <= self.last_executed_seq:
            return
        
        entry = await self.db.get_log_entry(seq)
        if not entry:
            log_error(self.node_id, f"Cannot execute seq={seq}: not in log")
            return
        
        if entry['executed']:
            return
        
        txn = entry['transaction_data']
        phase = entry.get('phase')
        
        if txn.get('type') == 'NO-OP':
            log_info(self.node_id, f"Executing NO-OP at seq={seq}")
            await self.db.mark_executed(seq)
            self.last_executed_seq = seq
            await self.db.set_last_executed_seq(seq)
            return
        
        if phase == 'C' or phase == 'A':
            log_info(self.node_id, f"Seq={seq} is {phase} phase (no execution needed)")
            await self.db.mark_executed(seq)
            self.last_executed_seq = seq
            await self.db.set_last_executed_seq(seq)
            return
        
        if phase == 'P':
            log_info(self.node_id, f"Seq={seq} is PREPARE phase (already executed)")
            if not entry['executed']:
                await self.db.mark_executed(seq)
            self.last_executed_seq = seq
            await self.db.set_last_executed_seq(seq)
            return
        
        sender = txn['sender']
        receiver = txn['receiver']
        amount = txn['amount']
        
        try:
            current_balance = await self.db.get_balance(sender)
            if current_balance is None or current_balance < amount:
                log_warning(self.node_id, f"Skipping seq={seq}: insufficient balance")
                await self.db.mark_executed(seq)
                self.last_executed_seq = seq
                await self.db.set_last_executed_seq(seq)
                return
            
            await self.db.execute_transfer(sender, receiver, amount)
            await self.db.mark_executed(seq)
            
            self.last_executed_seq = seq
            await self.db.set_last_executed_seq(seq)
            
            sender_bal = await self.db.get_balance(sender)
            receiver_bal = await self.db.get_balance(receiver)
            
            log_info(self.node_id, f"Executed seq={seq}: {sender}->{receiver} (${amount}) | Balances: {sender}={sender_bal}, {receiver}={receiver_bal}")
        
        except Exception as e:
            log_error(self.node_id, f"Failed to execute seq={seq}: {e}")
            import traceback
            traceback.print_exc()
    
    # ============================================================
    # CLIENT REQUESTS: BALANCE QUERY
    # ============================================================
    
    async def handle_balance(self, message: Dict[str, Any], writer):
        account_id = message['account_id']
        request_id = message['request_id']
        
        cluster = get_cluster_for_account(account_id)
        if cluster != self.cluster_id:
            await self.server.send_error(writer, request_id, "Wrong cluster")
            return
        
        balance = await self.db.get_balance(account_id)
        
        if balance is None:
            await self.server.send_error(writer, request_id, "Account not found")
        else:
            await self.server.send_response(writer, request_id, 'success', {'balance': balance})

    async def handle_print_balance(self, message: Dict[str, Any], writer):
        account_id = message['account_id']
        
        balance = await self.db.get_balance(account_id)
        
        response = {
            'type': 'PRINT_BALANCE_RESPONSE',
            'node_id': self.node_id,
            'account_id': account_id,
            'balance': balance,
            'timestamp': time.time()
        }
        
        data = MessageSerializer.serialize(response)
        writer.write(data)
        await writer.drain()
    

    async def handle_print_db(self, message: Dict[str, Any], writer):
        
        modified_accounts = await self.db.get_modified_accounts()
        
        response = {
            'type': 'PRINT_DB_RESPONSE',
            'node_id': self.node_id,
            'modified_accounts': modified_accounts,  
            'timestamp': time.time()
        }
        
        data = MessageSerializer.serialize(response)
        writer.write(data)
        await writer.drain()

    async def handle_print_view(self, message: Dict[str, Any], writer):
        
        view_changes = await self.db.get_all_view_changes()
        
        response = {
            'type': 'PRINT_VIEW_RESPONSE',
            'node_id': self.node_id,
            'cluster_id': self.cluster_id,
            'view_changes': view_changes,
            'current_leader': self.current_leader,     
            'current_ballot': self.current_ballot,       
            'is_failed': self.server.is_failed()         
        }
        
        data = MessageSerializer.serialize(response)
        writer.write(data)
        await writer.drain()

    async def handle_performance(self, message: Dict[str, Any], writer):
        log_info(self.node_id, "[PERFORMANCE] Aggregating performance metrics from all nodes")
        
        all_node_ids = list(range(1, TOTAL_NODES + 1))
        tasks = []
        
        for node_id in all_node_ids:
            tasks.append(self._query_node_performance(node_id))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        total_transactions = 0
        total_successful = 0
        total_failed = 0
        all_latencies = []
        
        node_stats = {}
        
        for i, result in enumerate(results):
            node_id = all_node_ids[i]
            
            if isinstance(result, Exception):
                log_warning(self.node_id, f"[PERFORMANCE] Failed to get stats from Node{node_id}: {result}")
                node_stats[node_id] = None
                continue
            
            if result and result.get('success'):
                stats = result.get('stats', {})
                node_stats[node_id] = stats
                
                total_transactions += stats.get('total_transactions', 0)
                total_successful += stats.get('successful_transactions', 0)
                total_failed += stats.get('failed_transactions', 0)
                all_latencies.extend(stats.get('latencies', []))
        
        if all_latencies:
            import numpy as np
            all_latencies.sort()
            n = len(all_latencies)
            
            latency_stats = {
                'mean_ms': float(np.mean(all_latencies) * 1000),
                'median_ms': float(np.median(all_latencies) * 1000),
                'p50_ms': float(all_latencies[int(n * 0.50)] * 1000),
                'p90_ms': float(all_latencies[int(n * 0.90)] * 1000),
                'p95_ms': float(all_latencies[int(n * 0.95)] * 1000),
                'p99_ms': float(all_latencies[int(n * 0.99)] * 1000),
                'min_ms': float(min(all_latencies) * 1000),
                'max_ms': float(max(all_latencies) * 1000)
            }
        else:
            latency_stats = {}
        
        response = {
            'type': 'PERFORMANCE_RESPONSE',
            'aggregated_stats': {
                'total_transactions': total_transactions,
                'successful_transactions': total_successful,
                'failed_transactions': total_failed,
                'success_rate': total_successful / max(1, total_transactions) * 100,
                'latency': latency_stats
            },
            'per_node_stats': node_stats,
            'timestamp': time.time()
        }
        
        data = MessageSerializer.serialize(response)
        writer.write(data)
        await writer.drain()

    async def _query_node_performance(self, node_id: int) -> Dict[str, Any]:
        try:
            host, port = get_node_address(node_id)
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=1.0
            )
            
            query_msg = {
                'type': 'PERFORMANCE_QUERY',
                'timestamp': time.time()
            }
            
            data = MessageSerializer.serialize(query_msg)
            writer.write(data)
            await writer.drain()
            
            response_data = await asyncio.wait_for(
                reader.readline(),
                timeout=2.0
            )
            
            writer.close()
            await writer.wait_closed()
            
            if not response_data:
                return {'success': False, 'error': 'No response'}
            
            response = MessageSerializer.deserialize(response_data)
            return {
                'success': True,
                'stats': response.get('stats', {})
            }
        
        except Exception as e:
            return {'success': False, 'error': str(e)}
        

    async def handle_performance_query(self, message: Dict[str, Any], writer):
        stats = {
            'total_transactions': len(self.pending_proposals) + self.last_executed_seq,
            'successful_transactions': self.last_executed_seq,
            'failed_transactions': 0,  
            'latencies': []  
        }
        
        if self.benchmark_mode:
            stats = {
                'total_transactions': self.benchmark_stats.get('transactions_processed', 0),
                'successful_transactions': self.benchmark_stats.get('committed_transactions', 0),
                'failed_transactions': self.benchmark_stats.get('aborted_transactions', 0),
                'latencies': self.benchmark_stats.get('prepare_latencies', []) + 
                            self.benchmark_stats.get('commit_latencies', [])
            }
        
        response = {
            'type': 'PERFORMANCE_QUERY_RESPONSE',
            'node_id': self.node_id,
            'stats': stats,
            'timestamp': time.time()
        }
        
        data = MessageSerializer.serialize(response)
        writer.write(data)
        await writer.drain()

    # ============================================================
    # CONTROL COMMANDS: FAIL/RECOVER/FLUSH
    # ============================================================
    
    async def handle_fail(self, message: Dict[str, Any], writer):
        log_warning(self.node_id, "*** FAILING NODE ***")
    
        if self.current_leader == self.node_id:
            log_info(self.node_id, "Leader failing - stopping heartbeats immediately")
            if self.heartbeat_task:
                self.heartbeat_task.cancel()
                self.heartbeat_task = None
        
        self.server.fail()
    
    async def handle_recover(self, message: Dict[str, Any], writer):
        log_info(self.node_id, "*** RECOVERING NODE ***")
        try:
            reload_shard_mapping()
            log_info(self.node_id, "Reloaded shard mapping after recovery")
        except Exception as e:
            log_warning(self.node_id, f"Could not reload shard mapping: {e}")
        self.server.recover()
        
        initial_leaders = {1: 1, 2: 4, 3: 7}
        expected_leader = initial_leaders[self.cluster_id]
        
        if self.node_id == expected_leader:
            self.current_leader = self.node_id
            await self.db.set_current_leader(self.node_id)
            
            if not self.heartbeat_task or self.heartbeat_task.done():
                self.heartbeat_task = asyncio.create_task(self.heartbeat_loop())
                log_info(self.node_id, "Restarted heartbeat task after recovery")
            
            log_info(self.node_id, "Recovered as leader (predetermined), continuing heartbeats")
        else:
            self.current_leader = expected_leader
            await self.db.set_current_leader(expected_leader)
            
            self.last_heartbeat_time = time.time() + 20.0
            
            if self.heartbeat_task:
                self.heartbeat_task.cancel()
                try:
                    await self.heartbeat_task
                except asyncio.CancelledError:
                    pass
                self.heartbeat_task = None
            
            log_info(self.node_id, f"Recovered as follower, expecting leader Node{expected_leader} (10s grace)")
        
        for peer_id in list(self.peer_connections.keys()):
            try:
                writer_conn = self.peer_connections[peer_id]
                writer_conn.close()
                await writer_conn.wait_closed()
            except:
                pass
        self.peer_connections.clear()

    
    async def handle_flush(self, message: Dict[str, Any], writer):
        log_info(self.node_id, "*** FLUSHING DATABASE ***")
        await self.db.flush_all_data()
        
        self.next_seq = 1
        self.last_executed_seq = 0
        self.pending_proposals = {}
        self.accepts_received = defaultdict(set)
        self.pending_2pc = {}
        
        for task in self.twopc_timeout_tasks.values():
            task.cancel()
        self.twopc_timeout_tasks.clear()
        
        self.current_ballot = 1  
        self.promises_received = set()
        self.is_candidate = False
        self.in_view_change = False
        
        initial_leaders = {1: 1, 2: 4, 3: 7}
        expected_leader = initial_leaders[self.cluster_id]
        
        if self.node_id == expected_leader:
            self.current_leader = self.node_id
            await self.db.set_current_leader(self.node_id)
            await self.db.set_current_ballot(1)
            
            if self.heartbeat_task:
                self.heartbeat_task.cancel()
                try:
                    await self.heartbeat_task
                except asyncio.CancelledError:
                    pass
            self.heartbeat_task = asyncio.create_task(self.heartbeat_loop())
            log_info(self.node_id, "Flushed as leader - restarted heartbeats")
        else:
            self.current_leader = expected_leader
            await self.db.set_current_leader(expected_leader)
            await self.db.set_current_ballot(1)
            
            self.last_heartbeat_time = time.time() + 10.0
            
            if self.heartbeat_task:
                self.heartbeat_task.cancel()
                try:
                    await self.heartbeat_task
                except asyncio.CancelledError:
                    pass
                self.heartbeat_task = None
            
            log_info(self.node_id, f"Flushed as follower - expecting leader Node{expected_leader}")
        
        await self.db.set_state('next_sequence_number', '1')
        await self.db.set_last_executed_seq(0)
        
        log_info(self.node_id, "Database flushed, state reset")

    
    async def handle_benchmark_start(self, message: Dict[str, Any], writer):
        self.enable_benchmark_mode()
        response = Message.response(
            message.get('request_id', 'benchmark_start'),
            'success',
            {'message': 'Benchmark mode enabled'}
        )
        data = MessageSerializer.serialize(response)
        writer.write(data)
        await writer.drain()

    async def handle_benchmark_stop(self, message: Dict[str, Any], writer):
        self.disable_benchmark_mode()
        response = Message.response(
            message.get('request_id', 'benchmark_stop'),
            'success',
            {'message': 'Benchmark mode disabled'}
        )
        data = MessageSerializer.serialize(response)
        writer.write(data)
        await writer.drain()

    async def handle_benchmark_stats(self, message: Dict[str, Any], writer):
        stats = self.get_benchmark_stats()
        response = Message.response(
            message.get('request_id', 'benchmark_stats'),
            'success',
            {'stats': stats}
        )
        data = MessageSerializer.serialize(response)
        writer.write(data)
        await writer.drain()

    async def handle_benchmark_reset(self, message: Dict[str, Any], writer):
        self.reset_benchmark_stats()
        response = Message.response(
            message.get('request_id', 'benchmark_reset'),
            'success',
            {'message': 'Benchmark stats reset'}
        )
        data = MessageSerializer.serialize(response)
        writer.write(data)
        await writer.drain()

    
    # ============================================================
    # NETWORKING HELPERS
    # ============================================================
    
    async def send_to_peer(self, peer_id: int, message: Dict[str, Any]):
        try:
            if peer_id not in self.peer_connections:
                host, port = get_node_address(peer_id)
                reader, writer = await asyncio.open_connection(host, port)
                self.peer_connections[peer_id] = writer
            
            writer = self.peer_connections[peer_id]
            data = MessageSerializer.serialize(message)
            writer.write(data)
            await writer.drain()
        
        except Exception as e:
            if peer_id in self.peer_connections:
                try:
                    self.peer_connections[peer_id].close()
                except:
                    pass
                del self.peer_connections[peer_id]
            
            if message.get('type') != MSG_HEARTBEAT:
                log_debug(self.node_id, f"Failed to send to Node{peer_id}: {e}")
    
    async def broadcast_to_peers(self, message: Dict[str, Any]):
        tasks = [self.send_to_peer(peer_id, message) for peer_id in self.peers]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    # ============================================================
    # CHECKPOINTING
    # ============================================================
    
    async def create_checkpoint(self, seq: int):
        if self.in_view_change:
            log_debug(self.node_id, f"Skipping checkpoint at seq={seq} during view change")
            return
        
        try:
            log_info(self.node_id, f"[CHECKPOINT] Creating checkpoint at seq={seq}")
            
            balances = await self.db.get_all_balances()
            
            checkpoint_id = await self.db.create_checkpoint(seq, balances)
            
            log_info(self.node_id, f"[CHECKPOINT] Created checkpoint {checkpoint_id} with {len(balances)} accounts")
            
            if AUTO_COMPACT_LOG:
                await self.compact_log(seq)
        
        except Exception as e:
            log_error(self.node_id, f"Failed to create checkpoint at seq={seq}: {e}")
    
    async def compact_log(self, checkpoint_seq: int):
        try:
            compact_before = max(1, checkpoint_seq - LOG_RETENTION_SIZE)
            
            deleted = await self.db.truncate_log_before(compact_before)
            
            if deleted > 0:
                log_info(self.node_id, f"[COMPACTION] Deleted {deleted} log entries before seq={compact_before}")
        
        except Exception as e:
            log_error(self.node_id, f"Failed to compact log: {e}")

    async def verify_account_ownership(self, account_id: int) -> bool:
        expected_cluster = get_cluster_for_account(account_id)
        
        if expected_cluster != self.cluster_id:
            log_warning(self.node_id, 
                    f"Account {account_id} should be in cluster {expected_cluster}, not {self.cluster_id}")
            return False
        
        return True
    
    


async def main():
    if len(sys.argv) != 2:
        print("Usage: python node.py <node_id>")
        sys.exit(1)
    
    node_id = int(sys.argv[1])
    node = Node(node_id)
    
    await node.start()
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print(f"\nShutting down Node {node_id}...")
        await node.stop()

if __name__ == '__main__':
    asyncio.run(main())