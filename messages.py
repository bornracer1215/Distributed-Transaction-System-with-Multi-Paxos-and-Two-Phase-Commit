"""
Message types and serialization for node communication
All messages are JSON-serializable dictionaries
"""

import json
import time
import uuid
from typing import Dict, Any, Optional, List

from config import *

class Message:
    
    @staticmethod
    def transaction(sender: int, receiver: int, amount: int, 
                   consistency: str = DEFAULT_CONSISTENCY,
                   request_id: Optional[str] = None) -> Dict[str, Any]:
        return {
            'type': MSG_TRANSACTION,
            'request_id': request_id or str(uuid.uuid4()),
            'transaction': {
                'sender': sender,
                'receiver': receiver,
                'amount': amount,
                'consistency': consistency
            },
            'timestamp': time.time()
        }
    
    @staticmethod
    def balance(account_id: int, consistency: str = DEFAULT_CONSISTENCY,
               request_id: Optional[str] = None) -> Dict[str, Any]:
        return {
            'type': MSG_BALANCE,
            'request_id': request_id or str(uuid.uuid4()),
            'account_id': account_id,
            'consistency': consistency,
            'timestamp': time.time()
        }
    
    @staticmethod
    def fail(node_id: int) -> Dict[str, Any]:
        return {
            'type': MSG_FAIL,
            'node_id': node_id,
            'timestamp': time.time()
        }
    
    @staticmethod
    def recover(node_id: int) -> Dict[str, Any]:
        return {
            'type': MSG_RECOVER,
            'node_id': node_id,
            'timestamp': time.time()
        }
    
    @staticmethod
    def prepare(from_node: int, ballot_number: int, 
               cluster_id: int) -> Dict[str, Any]:
        return {
            'type': MSG_PREPARE,
            'from': from_node,
            'ballot_number': ballot_number,
            'cluster_id': cluster_id,
            'timestamp': time.time()
        }
    
    @staticmethod
    def promise(from_node: int, ballot_number: int,
               accepted_proposals: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            'type': MSG_PROMISE,
            'from': from_node,
            'ballot_number': ballot_number,
            'accepted_proposals': accepted_proposals,
            'timestamp': time.time()
        }
    
    @staticmethod
    def accept(from_node: int, ballot_number: int, sequence_number: int,
              transaction: Dict[str, Any], phase: Optional[str] = None,
              role: Optional[str] = None) -> Dict[str, Any]:
        return {
            'type': MSG_ACCEPT,
            'from': from_node,
            'ballot_number': ballot_number,
            'sequence_number': sequence_number,
            'transaction': transaction,
            'phase': phase,
            'role': role,
            'timestamp': time.time()
        }
    
    @staticmethod
    def accepted(from_node: int, ballot_number: int, 
                sequence_number: int) -> Dict[str, Any]:
        return {
            'type': MSG_ACCEPTED,
            'from': from_node,
            'ballot_number': ballot_number,
            'sequence_number': sequence_number,
            'timestamp': time.time()
        }
    
    @staticmethod
    def commit(from_node: int, sequence_number: int) -> Dict[str, Any]:
        return {
            'type': MSG_COMMIT,
            'from': from_node,
            'sequence_number': sequence_number,
            'timestamp': time.time()
        }
    
    @staticmethod
    def heartbeat(from_node: int, ballot_number: int, 
                 cluster_id: int) -> Dict[str, Any]:
        return {
            'type': MSG_HEARTBEAT,
            'from': from_node,
            'ballot_number': ballot_number,
            'cluster_id': cluster_id,
            'timestamp': time.time()
        }
    
    @staticmethod
    def response(request_id: str, status: str, 
                data: Optional[Dict[str, Any]] = None,
                message: Optional[str] = None) -> Dict[str, Any]:
        return {
            'type': MSG_RESPONSE,
            'request_id': request_id,
            'status': status,
            'data': data or {},
            'message': message,
            'timestamp': time.time()
        }
    
    @staticmethod
    def error(request_id: str, error: str, 
             details: Optional[str] = None) -> Dict[str, Any]:
        return {
            'type': MSG_ERROR,
            'request_id': request_id,
            'error': error,
            'details': details,
            'timestamp': time.time()
        }
    
    @staticmethod
    def two_pc_prepare(from_node: int, transaction_id: str,
                      transaction: Dict[str, Any],
                      request_id: str,
                      from_cluster: int) -> Dict[str, Any]:
        return {
            'type': MSG_2PC_PREPARE,
            'from': from_node,
            'from_cluster': from_cluster,
            'transaction_id': transaction_id,
            'transaction': transaction,
            'request_id': request_id,
            'timestamp': time.time()
        }
    
    @staticmethod
    def two_pc_prepared(from_node: int, 
                       transaction_id: str,
                       from_cluster: int) -> Dict[str, Any]:
        return {
            'type': MSG_2PC_PREPARED,
            'from': from_node,
            'from_cluster': from_cluster,
            'transaction_id': transaction_id,
            'timestamp': time.time()
        }
    
    @staticmethod
    def two_pc_commit(from_node: int, 
                     transaction_id: str,
                     from_cluster: int) -> Dict[str, Any]:
        return {
            'type': MSG_2PC_COMMIT,
            'from': from_node,
            'from_cluster': from_cluster,
            'transaction_id': transaction_id,
            'timestamp': time.time()
        }
    
    @staticmethod
    def two_pc_abort(from_node: int, 
                    transaction_id: str,
                    from_cluster: int,
                    reason: Optional[str] = None) -> Dict[str, Any]:
        return {
            'type': MSG_2PC_ABORT,
            'from': from_node,
            'from_cluster': from_cluster,
            'transaction_id': transaction_id,
            'reason': reason,
            'timestamp': time.time()
        }
    
    @staticmethod
    def two_pc_ack(from_node: int,
                   transaction_id: str,
                   from_cluster: int,
                   phase: str) -> Dict[str, Any]:
        return {
            'type': MSG_2PC_ACK,
            'from': from_node,
            'from_cluster': from_cluster,
            'transaction_id': transaction_id,
            'phase': phase,
            'timestamp': time.time()
        }
    
    @staticmethod
    def print_balance(account_id: int) -> Dict[str, Any]:
        return {
            'type': MSG_PRINT_BALANCE,
            'account_id': account_id,
            'timestamp': time.time()
        }
    
    @staticmethod
    def print_db() -> Dict[str, Any]:
        return {
            'type': MSG_PRINT_DB,
            'timestamp': time.time()
        }

    @staticmethod
    def print_view() -> Dict[str, Any]:
        return {
            'type': MSG_PRINT_VIEW,
            'timestamp': time.time()
        }
    
    @staticmethod
    def performance() -> Dict[str, Any]:
        return {
            'type': MSG_PERFORMANCE,
            'timestamp': time.time()
        }


class MessageSerializer:
    
    @staticmethod
    def serialize(message: Dict[str, Any]) -> bytes:
        json_str = json.dumps(message)
        return (json_str + '\n').encode('utf-8')
    
    @staticmethod
    def deserialize(data: bytes) -> Dict[str, Any]:
        json_str = data.decode('utf-8').strip()
        return json.loads(json_str)
    
    @staticmethod
    def validate(message: Dict[str, Any]) -> bool:
        if 'type' not in message:
            return False
        
        if 'timestamp' not in message:
            return False
        
        msg_type = message['type']
        
        if msg_type == MSG_TRANSACTION:
            required = ['request_id', 'transaction']
            return all(field in message for field in required)
        
        elif msg_type == MSG_BALANCE:
            required = ['request_id', 'account_id']
            return all(field in message for field in required)
        
        elif msg_type in [MSG_PREPARE, MSG_PROMISE, MSG_ACCEPT, 
                         MSG_ACCEPTED, MSG_HEARTBEAT]:
            return 'from' in message and 'ballot_number' in message
        
        elif msg_type == MSG_RESPONSE:
            required = ['request_id', 'status']
            return all(field in message for field in required)
        
        elif msg_type in [MSG_2PC_PREPARE, MSG_2PC_PREPARED, MSG_2PC_COMMIT,
                         MSG_2PC_ABORT, MSG_2PC_ACK]:
            required = ['from', 'transaction_id', 'from_cluster']
            return all(field in message for field in required)
        
        return True


def create_transaction_id() -> str:
    return str(uuid.uuid4())

def get_message_type(message: Dict[str, Any]) -> str:
    return message.get('type', 'UNKNOWN')

def is_paxos_message(message: Dict[str, Any]) -> bool:
    paxos_types = [MSG_PREPARE, MSG_PROMISE, MSG_ACCEPT, 
                   MSG_ACCEPTED, MSG_COMMIT, MSG_HEARTBEAT]
    return get_message_type(message) in paxos_types

def is_client_message(message: Dict[str, Any]) -> bool:
    client_types = [MSG_TRANSACTION, MSG_BALANCE, MSG_FAIL, MSG_RECOVER]
    return get_message_type(message) in client_types

def is_2pc_message(message: Dict[str, Any]) -> bool:
    two_pc_types = [MSG_2PC_PREPARE, MSG_2PC_PREPARED, 
                    MSG_2PC_COMMIT, MSG_2PC_ABORT, MSG_2PC_ACK]
    return get_message_type(message) in two_pc_types