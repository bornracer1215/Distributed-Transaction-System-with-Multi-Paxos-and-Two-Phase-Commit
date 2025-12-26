#Database layer for node state management

import sqlite3
import json
import time
import asyncio
from typing import Optional, List, Dict, Any, Tuple
import os

from config import *

class NodeDatabase:

    
    def __init__(self, node_id: int, cluster_id: int):
        self.node_id = node_id
        self.cluster_id = cluster_id
        self.db_path = get_db_path(node_id)
        self.conn: Optional[sqlite3.Connection] = None
        self.shard_range = SHARD_MAPPING[cluster_id]
        
    def initialize(self):
        os.makedirs(DB_DIR, exist_ok=True)
        
        self.conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
            isolation_level=None  
        )
        
        self.conn.row_factory = sqlite3.Row
        self._setup_performance()
        self._create_schema()
        self._initialize_accounts()
        self._initialize_node_state()
        
        print(f"Node {self.node_id}: Database initialized at {self.db_path}")
        print(f"Node {self.node_id}: Managing accounts {self.shard_range[0]}-{self.shard_range[1]}")
    
    def _setup_performance(self):
        cursor = self.conn.cursor()
        
        if SQLITE_WAL_MODE:
            cursor.execute("PRAGMA journal_mode = WAL")
        
        cursor.execute("PRAGMA synchronous = NORMAL")
        cursor.execute("PRAGMA cache_size = -64000")
        cursor.execute("PRAGMA temp_store = MEMORY")
        cursor.execute("PRAGMA mmap_size = 30000000000")
        cursor.execute("PRAGMA page_size = 4096")
        
        print(f"Node {self.node_id}: Performance optimizations applied")
    
    def _create_schema(self):
        cursor = self.conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY,
                balance INTEGER NOT NULL DEFAULT 10
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS paxos_log (
                sequence_number INTEGER PRIMARY KEY,
                ballot_number INTEGER NOT NULL,
                transaction_id TEXT,
                transaction_type TEXT NOT NULL,
                transaction_data TEXT NOT NULL,
                phase TEXT,
                role TEXT,
                status TEXT DEFAULT 'committed',
                committed_at REAL NOT NULL,
                executed INTEGER DEFAULT 0
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS node_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS view_changes (
                view_number INTEGER PRIMARY KEY,
                old_leader_id INTEGER,
                new_leader_id INTEGER NOT NULL,
                ballot_number INTEGER NOT NULL,
                timestamp REAL NOT NULL
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS checkpoints (
                checkpoint_id INTEGER PRIMARY KEY AUTOINCREMENT,
                sequence_number INTEGER NOT NULL UNIQUE,
                account_snapshot TEXT NOT NULL,
                created_at REAL NOT NULL,
                num_accounts INTEGER NOT NULL
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS locks (
                account_id INTEGER PRIMARY KEY,
                transaction_id TEXT NOT NULL,
                locked_at REAL NOT NULL,
                lock_type TEXT NOT NULL
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS write_ahead_log (
                transaction_id TEXT NOT NULL,
                account_id INTEGER NOT NULL,
                old_balance INTEGER NOT NULL,
                new_balance INTEGER NOT NULL,
                operation TEXT NOT NULL,
                created_at REAL NOT NULL,
                PRIMARY KEY (transaction_id, account_id)
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_paxos_seq ON paxos_log(sequence_number)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_paxos_ballot ON paxos_log(ballot_number)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_checkpoint_seq ON checkpoints(sequence_number DESC)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_lock_time ON locks(locked_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_wal_txn ON write_ahead_log(transaction_id)')
        
        print(f"Node {self.node_id}: Schema created")
    
    def _initialize_accounts(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM accounts")
        count = cursor.fetchone()[0]
        
        if count == 0:
            start, end = self.shard_range
            print(f"Node {self.node_id}: Initializing {end - start + 1} accounts...")
            
            accounts = [(account_id, INITIAL_BALANCE) 
                       for account_id in range(start, end + 1)]
            
            cursor.executemany(
                'INSERT INTO accounts (id, balance) VALUES (?, ?)',
                accounts
            )
            print(f"Node {self.node_id}: Accounts initialized")
        else:
            print(f"Node {self.node_id}: Accounts already initialized ({count} accounts)")
    
    def _initialize_node_state(self):
        cursor = self.conn.cursor()
        
        default_state = {
            'current_ballot': '0',
            'current_leader': 'none',
            'last_executed_seq': '0',
            'current_view': '0',
            'next_sequence_number': '1'
        }
        
        for key, value in default_state.items():
            cursor.execute(
                'INSERT OR IGNORE INTO node_state (key, value) VALUES (?, ?)',
                (key, value)
            )
        
        print(f"Node {self.node_id}: Node state initialized")
    

    # ACCOUNT OPERATIONS

    def _get_balance_sync(self, account_id: int) -> Optional[int]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT balance FROM accounts WHERE id = ?', (account_id,))
        row = cursor.fetchone()
        return row[0] if row else None
    
    async def get_balance(self, account_id: int) -> Optional[int]:
        return await asyncio.to_thread(self._get_balance_sync, account_id)
    
    def _update_balance_sync(self, account_id: int, new_balance: int):
        cursor = self.conn.cursor()
        cursor.execute(
            'UPDATE accounts SET balance = ? WHERE id = ?',
            (new_balance, account_id)
        )
    
    async def update_balance(self, account_id: int, new_balance: int):
        await asyncio.to_thread(self._update_balance_sync, account_id, new_balance)
    
    def _execute_transfer_sync(self, sender: int, receiver: int, amount: int):
        cursor = self.conn.cursor()
        
        # Debit sender
        cursor.execute(
            'UPDATE accounts SET balance = balance - ? WHERE id = ?',
            (amount, sender)
        )
        
        # Credit receiver (only if in same shard)
        cursor.execute('SELECT id FROM accounts WHERE id = ?', (receiver,))
        if cursor.fetchone():
            cursor.execute(
                'UPDATE accounts SET balance = balance + ? WHERE id = ?',
                (amount, receiver)
            )
    
    async def execute_transfer(self, sender: int, receiver: int, amount: int):
        await asyncio.to_thread(self._execute_transfer_sync, sender, receiver, amount)
    
    def _get_all_balances_sync(self) -> Dict[int, int]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT id, balance FROM accounts ORDER BY id')
        return {row['id']: row['balance'] for row in cursor.fetchall()}
    
    async def get_all_balances(self) -> Dict[int, int]:
        return await asyncio.to_thread(self._get_all_balances_sync)
    
    def _restore_balances_sync(self, balances: Dict[int, int]):
        cursor = self.conn.cursor()
        for account_id, balance in balances.items():
            cursor.execute(
                'UPDATE accounts SET balance = ? WHERE id = ?',
                (balance, account_id)
            )
    
    async def restore_balances(self, balances: Dict[int, int]):
        await asyncio.to_thread(self._restore_balances_sync, balances)
    
   
    # PAXOS LOG OPERATIONS
    
    def _log_accepted_sync(self, sequence_number: int, ballot_number: int,
                          transaction_id: str, transaction_type: str,
                          transaction_data: Dict[str, Any], phase: Optional[str] = None,
                          role: Optional[str] = None):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO paxos_log 
            (sequence_number, ballot_number, transaction_id, transaction_type,
             transaction_data, phase, role, status, committed_at, executed)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'committed', ?, 0)
        ''', (sequence_number, ballot_number, transaction_id, transaction_type,
              json.dumps(transaction_data), phase, role, time.time()))
    
    async def log_accepted(self, sequence_number: int, ballot_number: int,
                          transaction_id: str, transaction_type: str,
                          transaction_data: Dict[str, Any], phase: Optional[str] = None,
                          role: Optional[str] = None):
        await asyncio.to_thread(
            self._log_accepted_sync,
            sequence_number, ballot_number, transaction_id,
            transaction_type, transaction_data, phase, role
        )
    
    def _mark_executed_sync(self, sequence_number: int):
        cursor = self.conn.cursor()
        cursor.execute(
            'UPDATE paxos_log SET executed = 1 WHERE sequence_number = ?',
            (sequence_number,)
        )
    
    async def mark_executed(self, sequence_number: int):
        await asyncio.to_thread(self._mark_executed_sync, sequence_number)
    
    def _get_log_entry_sync(self, sequence_number: int) -> Optional[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT * FROM paxos_log WHERE sequence_number = ?',
            (sequence_number,)
        )
        row = cursor.fetchone()
        if row:
            return {
                'sequence_number': row['sequence_number'],
                'ballot_number': row['ballot_number'],
                'transaction_id': row['transaction_id'],
                'transaction_type': row['transaction_type'],
                'transaction_data': json.loads(row['transaction_data']),
                'phase': row['phase'],
                'role': row['role'] if 'role' in row.keys() else None,
                'status': row['status'],
                'executed': bool(row['executed'])
            }
        return None
    
    async def get_log_entry(self, sequence_number: int) -> Optional[Dict[str, Any]]:
        return await asyncio.to_thread(self._get_log_entry_sync, sequence_number)
    
    def _get_all_log_entries_sync(self) -> List[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM paxos_log ORDER BY sequence_number')
        rows = cursor.fetchall()
        return [
            {
                'sequence_number': row['sequence_number'],
                'ballot_number': row['ballot_number'],
                'transaction_id': row['transaction_id'],
                'transaction_type': row['transaction_type'],
                'transaction_data': json.loads(row['transaction_data']),
                'phase': row['phase'],
                'role': row['role'] if 'role' in row.keys() else None,
                'status': row['status'],
                'executed': bool(row['executed'])
            }
            for row in rows
        ]
    
    async def get_all_log_entries(self) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(self._get_all_log_entries_sync)
    
    def _truncate_log_before_sync(self, sequence_number: int) -> int:
        cursor = self.conn.cursor()
        cursor.execute(
            'DELETE FROM paxos_log WHERE sequence_number < ?',
            (sequence_number,)
        )
        return cursor.rowcount
    
    async def truncate_log_before(self, sequence_number: int) -> int:
        deleted = await asyncio.to_thread(self._truncate_log_before_sync, sequence_number)
        return deleted
    
    # CHECKPOINT OPERATIONS
    
    def _create_checkpoint_sync(self, sequence_number: int, balances: Dict[int, int]) -> int:
        cursor = self.conn.cursor()
        
        snapshot = json.dumps(balances)
        
        # Insert checkpoint
        cursor.execute('''
            INSERT INTO checkpoints (sequence_number, account_snapshot, created_at, num_accounts)
            VALUES (?, ?, ?, ?)
        ''', (sequence_number, snapshot, time.time(), len(balances)))
        
        checkpoint_id = cursor.lastrowid
        
        cursor.execute('''
            DELETE FROM checkpoints 
            WHERE checkpoint_id NOT IN (
                SELECT checkpoint_id FROM checkpoints 
                ORDER BY sequence_number DESC 
                LIMIT ?
            )
        ''', (MAX_CHECKPOINTS,))
        
        return checkpoint_id
    
    async def create_checkpoint(self, sequence_number: int, balances: Dict[int, int]) -> int:
        return await asyncio.to_thread(self._create_checkpoint_sync, sequence_number, balances)
    
    def _get_latest_checkpoint_sync(self) -> Optional[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT * FROM checkpoints 
            ORDER BY sequence_number DESC 
            LIMIT 1
        ''')
        row = cursor.fetchone()
        
        if row:
            return {
                'checkpoint_id': row['checkpoint_id'],
                'sequence_number': row['sequence_number'],
                'balances': json.loads(row['account_snapshot']),
                'created_at': row['created_at'],
                'num_accounts': row['num_accounts']
            }
        return None
    
    async def get_latest_checkpoint(self) -> Optional[Dict[str, Any]]:
        return await asyncio.to_thread(self._get_latest_checkpoint_sync)
    
    def _get_checkpoint_count_sync(self) -> int:
        cursor = self.conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM checkpoints')
        return cursor.fetchone()[0]
    
    async def get_checkpoint_count(self) -> int:
        return await asyncio.to_thread(self._get_checkpoint_count_sync)
    
    async def restore_from_checkpoint(self, checkpoint: Dict[str, Any]):
        # Restore balances
        await self.restore_balances(checkpoint['balances'])
        
        # Update last executed sequence
        await self.set_last_executed_seq(checkpoint['sequence_number'])
        
        print(f"Node {self.node_id}: Restored from checkpoint at seq={checkpoint['sequence_number']}")
    
    # LOCK MANAGEMENT OPERATIONS (NEW for 2PC)
    
    def _acquire_lock_sync(self, account_id: int, transaction_id: str, lock_type: str = 'exclusive') -> bool:
        cursor = self.conn.cursor()
        
        # Check if already locked
        cursor.execute('SELECT transaction_id FROM locks WHERE account_id = ?', (account_id,))
        row = cursor.fetchone()
        
        if row:
            # Already locked by another transaction
            if row[0] != transaction_id:
                return False
            # Already locked by same transaction - idempotent
            return True
        
        # Acquire lock
        cursor.execute('''
            INSERT INTO locks (account_id, transaction_id, locked_at, lock_type)
            VALUES (?, ?, ?, ?)
        ''', (account_id, transaction_id, time.time(), lock_type))
        
        return True
    
    async def acquire_lock(self, account_id: int, transaction_id: str, lock_type: str = 'exclusive') -> bool:
        return await asyncio.to_thread(self._acquire_lock_sync, account_id, transaction_id, lock_type)
    
    def _release_lock_sync(self, account_id: int, transaction_id: str) -> bool:
        cursor = self.conn.cursor()
        
        # Only release if held by this transaction
        cursor.execute(
            'DELETE FROM locks WHERE account_id = ? AND transaction_id = ?',
            (account_id, transaction_id)
        )
        
        return cursor.rowcount > 0
    
    async def release_lock(self, account_id: int, transaction_id: str) -> bool:
        return await asyncio.to_thread(self._release_lock_sync, account_id, transaction_id)
    
    def _check_lock_sync(self, account_id: int) -> Optional[str]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT transaction_id FROM locks WHERE account_id = ?', (account_id,))
        row = cursor.fetchone()
        return row[0] if row else None
    
    async def check_lock(self, account_id: int) -> Optional[str]:
        return await asyncio.to_thread(self._check_lock_sync, account_id)
    
    def _is_locked_sync(self, account_id: int) -> bool:
        cursor = self.conn.cursor()
        cursor.execute('SELECT 1 FROM locks WHERE account_id = ?', (account_id,))
        return cursor.fetchone() is not None
    
    async def is_locked(self, account_id: int) -> bool:
        return await asyncio.to_thread(self._is_locked_sync, account_id)
    
    def _get_all_locks_sync(self) -> List[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM locks ORDER BY locked_at')
        rows = cursor.fetchall()
        return [
            {
                'account_id': row['account_id'],
                'transaction_id': row['transaction_id'],
                'locked_at': row['locked_at'],
                'lock_type': row['lock_type']
            }
            for row in rows
        ]
    
    async def get_all_locks(self) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(self._get_all_locks_sync)
    
    def _cleanup_expired_locks_sync(self, timeout: float = 10.0) -> int:
        cursor = self.conn.cursor()
        cutoff_time = time.time() - timeout
        
        cursor.execute('DELETE FROM locks WHERE locked_at < ?', (cutoff_time,))
        return cursor.rowcount
    
    async def cleanup_expired_locks(self, timeout: float = 10.0) -> int:
        return await asyncio.to_thread(self._cleanup_expired_locks_sync, timeout)
    

    # WRITE-AHEAD LOG OPERATIONS (NEW for 2PC)
    
    def _write_wal_entry_sync(self, transaction_id: str, account_id: int,
                              old_balance: int, new_balance: int, operation: str):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO write_ahead_log
            (transaction_id, account_id, old_balance, new_balance, operation, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (transaction_id, account_id, old_balance, new_balance, operation, time.time()))
    
    async def write_wal_entry(self, transaction_id: str, account_id: int,
                             old_balance: int, new_balance: int, operation: str):
        await asyncio.to_thread(
            self._write_wal_entry_sync,
            transaction_id, account_id, old_balance, new_balance, operation
        )
    
    def _get_wal_entries_sync(self, transaction_id: str) -> List[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT * FROM write_ahead_log WHERE transaction_id = ?',
            (transaction_id,)
        )
        rows = cursor.fetchall()
        return [
            {
                'transaction_id': row['transaction_id'],
                'account_id': row['account_id'],
                'old_balance': row['old_balance'],
                'new_balance': row['new_balance'],
                'operation': row['operation'],
                'created_at': row['created_at']
            }
            for row in rows
        ]
    
    async def get_wal_entries(self, transaction_id: str) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(self._get_wal_entries_sync, transaction_id)
    
    def _undo_from_wal_sync(self, transaction_id: str) -> int:
        cursor = self.conn.cursor()
        
        # Get all WAL entries for this transaction
        cursor.execute(
            'SELECT account_id, old_balance FROM write_ahead_log WHERE transaction_id = ?',
            (transaction_id,)
        )
        entries = cursor.fetchall()
        
        # Restore old balances
        for row in entries:
            account_id = row['account_id']
            old_balance = row['old_balance']
            cursor.execute(
                'UPDATE accounts SET balance = ? WHERE id = ?',
                (old_balance, account_id)
            )
        
        return len(entries)
    
    async def undo_from_wal(self, transaction_id: str) -> int:
        return await asyncio.to_thread(self._undo_from_wal_sync, transaction_id)
    
    def _delete_wal_entries_sync(self, transaction_id: str) -> int:
        cursor = self.conn.cursor()
        cursor.execute(
            'DELETE FROM write_ahead_log WHERE transaction_id = ?',
            (transaction_id,)
        )
        return cursor.rowcount
    
    async def delete_wal_entries(self, transaction_id: str) -> int:
        return await asyncio.to_thread(self._delete_wal_entries_sync, transaction_id)
    
    def _get_all_wal_entries_sync(self) -> List[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM write_ahead_log ORDER BY created_at')
        rows = cursor.fetchall()
        return [
            {
                'transaction_id': row['transaction_id'],
                'account_id': row['account_id'],
                'old_balance': row['old_balance'],
                'new_balance': row['new_balance'],
                'operation': row['operation'],
                'created_at': row['created_at']
            }
            for row in rows
        ]
    
    async def get_all_wal_entries(self) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(self._get_all_wal_entries_sync)
    

    # NODE STATE OPERATIONS
    
    def _get_state_sync(self, key: str) -> Optional[str]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT value FROM node_state WHERE key = ?', (key,))
        row = cursor.fetchone()
        return row[0] if row else None
    
    async def get_state(self, key: str) -> Optional[str]:
        return await asyncio.to_thread(self._get_state_sync, key)
    
    def _set_state_sync(self, key: str, value: str):
        cursor = self.conn.cursor()
        cursor.execute(
            'INSERT OR REPLACE INTO node_state (key, value) VALUES (?, ?)',
            (key, value)
        )
    
    async def set_state(self, key: str, value: str):
        await asyncio.to_thread(self._set_state_sync, key, value)
    
    async def get_current_ballot(self) -> int:
        value = await self.get_state('current_ballot')
        return int(value) if value else 0
    
    async def set_current_ballot(self, ballot: int):
        await self.set_state('current_ballot', str(ballot))
    
    async def increment_ballot(self) -> int:
        current = await self.get_current_ballot()
        new_ballot = current + 1
        await self.set_current_ballot(new_ballot)
        return new_ballot
    
    async def get_current_leader(self) -> Optional[int]:
        value = await self.get_state('current_leader')
        return int(value) if value and value != 'none' else None
    
    async def set_current_leader(self, leader_id: Optional[int]):
        value = str(leader_id) if leader_id else 'none'
        await self.set_state('current_leader', value)
    
    async def get_last_executed_seq(self) -> int:
        value = await self.get_state('last_executed_seq')
        return int(value) if value else 0
    
    async def set_last_executed_seq(self, seq: int):
        await self.set_state('last_executed_seq', str(seq))
    
    async def get_next_sequence_number(self) -> int:
        value = await self.get_state('next_sequence_number')
        return int(value) if value else 1
    
    async def increment_sequence_number(self) -> int:
        current = await self.get_next_sequence_number()
        next_seq = current + 1
        await self.set_state('next_sequence_number', str(next_seq))
        return current
    

    # VIEW CHANGE OPERATIONS
    
    async def log_view_change(self, view_number: int, old_leader: Optional[int],
                             new_leader: int, ballot_number: int):
        def _log():
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT INTO view_changes 
                (view_number, old_leader_id, new_leader_id, ballot_number, timestamp)
                VALUES (?, ?, ?, ?, ?)
            ''', (view_number, old_leader, new_leader, ballot_number, time.time()))
        
        await asyncio.to_thread(_log)
    
    async def get_all_view_changes(self) -> List[Dict[str, Any]]:
        def _get():
            cursor = self.conn.cursor()
            cursor.execute('SELECT * FROM view_changes ORDER BY view_number')
            rows = cursor.fetchall()
            return [
                {
                    'view_number': row['view_number'],
                    'old_leader_id': row['old_leader_id'],
                    'new_leader_id': row['new_leader_id'],
                    'ballot_number': row['ballot_number'],
                    'timestamp': row['timestamp']
                }
                for row in rows
            ]
        
        return await asyncio.to_thread(_get)
    
    # UTILITY OPERATIONS
    
    async def get_modified_accounts(self) -> Dict[int, int]:
        def _get():
            cursor = self.conn.cursor()
            cursor.execute(
                'SELECT id, balance FROM accounts WHERE balance != ?',
                (INITIAL_BALANCE,)
            )
            return {row['id']: row['balance'] for row in cursor.fetchall()}
        
        return await asyncio.to_thread(_get)
    
    async def flush_all_data(self):
        def _flush():
            cursor = self.conn.cursor()
            
            # Reset all balances
            cursor.execute('UPDATE accounts SET balance = ?', (INITIAL_BALANCE,))
            
            # Clear paxos log
            cursor.execute('DELETE FROM paxos_log')
            
            # Clear view changes
            cursor.execute('DELETE FROM view_changes')
            
            # Clear checkpoints
            cursor.execute('DELETE FROM checkpoints')
            
            # Clear locks (NEW)
            cursor.execute('DELETE FROM locks')
            
            # Clear WAL (NEW)
            cursor.execute('DELETE FROM write_ahead_log')
            
            # Reset node state
            cursor.execute("UPDATE node_state SET value = '0' WHERE key = 'current_ballot'")
            cursor.execute("UPDATE node_state SET value = 'none' WHERE key = 'current_leader'")
            cursor.execute("UPDATE node_state SET value = '0' WHERE key = 'last_executed_seq'")
            cursor.execute("UPDATE node_state SET value = '0' WHERE key = 'current_view'")
            cursor.execute("UPDATE node_state SET value = '1' WHERE key = 'next_sequence_number'")
        
        await asyncio.to_thread(_flush)
        print(f"Node {self.node_id}: Database flushed")
    
    def close(self):
        if self.conn:
            self.conn.close()
            print(f"Node {self.node_id}: Database connection closed")
    

    def get_all_account_ids(self) -> List[int]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT id FROM accounts ORDER BY id')
        rows = cursor.fetchall()
        return [row[0] for row in rows]

    async def export_accounts_to_dict(self) -> Dict[int, int]:
        def _export():
            cursor = self.conn.cursor()
            cursor.execute('SELECT id, balance FROM accounts ORDER BY id')
            rows = cursor.fetchall()
            return {row[0]: row[1] for row in rows}
        
        return await asyncio.to_thread(_export)