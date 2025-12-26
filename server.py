#Node Server - Handles incoming connections and message routing

import asyncio
import json
import os
import time
from typing import Optional, Dict, Any, Set
import logging

from config import *
from messages import Message, MessageSerializer
from database import NodeDatabase


class NodeServer:
    
    def __init__(self, node_id: int):
        validate_node_id(node_id)
        
        self.node_id = node_id
        self.cluster_id = get_cluster_for_node(node_id)
        self.host = HOST
        self.port = get_node_port(node_id)
        
        self.db = NodeDatabase(node_id, self.cluster_id)
        
        self.server: Optional[asyncio.Server] = None
        self.running = False
        self.failed = False
        
        self.connections: Set[asyncio.StreamWriter] = set()
        
        self.message_handler: Optional[callable] = None
        
        self._setup_logging()
        
        self.logger.info(f"Node {node_id} initialized (Cluster {self.cluster_id})")
    
    def _setup_logging(self):
        os.makedirs(LOG_DIR, exist_ok=True)
        
        self.logger = logging.getLogger(f'Node{self.node_id}')
        self.logger.setLevel(LOG_LEVEL)
        
        fh = logging.FileHandler(get_log_path(self.node_id))
        fh.setLevel(LOG_LEVEL)
        
        ch = logging.StreamHandler()
        ch.setLevel(LOG_LEVEL)
        
        formatter = logging.Formatter(
            f'%(asctime)s - Node{self.node_id} - %(levelname)s - %(message)s'
        )
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
    
    def set_message_handler(self, handler: callable):
        self.message_handler = handler
    
    async def start(self):
        self.db.initialize()
        
        self.server = await asyncio.start_server(
            self._handle_connection,
            self.host,
            self.port
        )
        
        self.running = True
        self.logger.info(f"Server started on {self.host}:{self.port}")
        
        addr = self.server.sockets[0].getsockname()
        self.logger.info(f"Serving on {addr}")
    
    async def stop(self):
        self.running = False
        
        for writer in self.connections.copy():
            writer.close()
            await writer.wait_closed()
        
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        
        self.db.close()
        
        self.logger.info("Server stopped")
    
    async def _handle_connection(self, reader: asyncio.StreamReader, 
                                 writer: asyncio.StreamWriter):
        addr = writer.get_extra_info('peername')
        self.logger.debug(f"New connection from {addr}")
        
        self.connections.add(writer)
        
        try:
            while self.running:  
                data = await reader.readline()
                
                if not data:
                    break
                
                try:
                    message = MessageSerializer.deserialize(data)
                    
                    if not MessageSerializer.validate(message):
                        self.logger.warning(f"Invalid message from {addr}: {message}")
                        error = Message.error('unknown', 'Invalid message format')
                        await self._send_message(writer, error)
                        continue
                    
                    self.logger.debug(f"Received {message['type']} from {addr}")
                    
                    if self.failed and message['type'] != MSG_RECOVER:
                        self.logger.debug(f"Node failed, ignoring {message['type']}")
                        continue

                    if self.message_handler:
                        await self.message_handler(message, writer)
                    else:
                        self.logger.warning("No message handler set!")
                
                except json.JSONDecodeError as e:
                    self.logger.warning(f"JSON decode error from {addr}: {e}")
                except Exception as e:
                    self.logger.error(f"Error handling message from {addr}: {e}")
        
        except asyncio.CancelledError:
            self.logger.debug(f"Connection handler cancelled for {addr}")
        except Exception as e:
            self.logger.error(f"Connection error from {addr}: {e}")
        finally:
            self.connections.discard(writer)
            writer.close()
            await writer.wait_closed()
            self.logger.debug(f"Connection closed from {addr}")
    
    async def _send_message(self, writer: asyncio.StreamWriter, 
                           message: Dict[str, Any]):
        try:
            data = MessageSerializer.serialize(message)
            writer.write(data)
            await writer.drain()
        except Exception as e:
            self.logger.error(f"Error sending message: {e}")
    
    async def send_response(self, writer: asyncio.StreamWriter, 
                           request_id: str, status: str, 
                           data: Optional[Dict[str, Any]] = None,
                           message: Optional[str] = None):
        response = Message.response(request_id, status, data, message)
        await self._send_message(writer, response)
    
    async def send_error(self, writer: asyncio.StreamWriter,
                        request_id: str, error: str,
                        details: Optional[str] = None):
        error_msg = Message.error(request_id, error, details)
        await self._send_message(writer, error_msg)
    
    def fail(self):
        self.failed = True
        self.logger.warning("*** NODE FAILED ***")
    
    def recover(self):
        self.failed = False
        self.logger.info("*** NODE RECOVERED ***")
    
    def is_failed(self) -> bool:
        return self.failed


async def test_server():
    print("=" * 60)
    print("Testing server.py")
    print("=" * 60)
    
    server = NodeServer(1)
    
    async def test_handler(message, writer):
        print(f"Handler received: {message['type']}")
        
        if message['type'] == MSG_BALANCE:
            response = Message.response(
                message['request_id'],
                'success',
                {'balance': 100}
            )
            data = MessageSerializer.serialize(response)
            writer.write(data)
            await writer.drain()
    
    server.set_message_handler(test_handler)
    
    await server.start()
    print(f"✓ Server started on port {server.port}")
    
    print("\nTesting client connection...")
    reader, writer = await asyncio.open_connection('localhost', server.port)
    
    balance_msg = Message.balance(100)
    data = MessageSerializer.serialize(balance_msg)
    writer.write(data)
    await writer.drain()
    print(f"✓ Sent balance query")
    
    response_data = await reader.readline()
    response = MessageSerializer.deserialize(response_data)
    assert response['type'] == MSG_RESPONSE
    assert response['status'] == 'success'
    assert response['data']['balance'] == 100
    print(f"✓ Received response: balance={response['data']['balance']}")
    
    writer.close()
    await writer.wait_closed()
    
    print("\nTesting failure/recovery...")
    server.fail()
    assert server.is_failed() == True
    print("✓ Node failed")
    
    server.recover()
    assert server.is_failed() == False
    print("✓ Node recovered")
    
    await server.stop()
    print("✓ Server stopped")
    
    print("\n" + "=" * 60)
    print("All server tests passed!")
    print("=" * 60)


if __name__ == '__main__':
    asyncio.run(test_server())