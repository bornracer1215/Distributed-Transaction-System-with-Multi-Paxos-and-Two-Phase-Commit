# SmallBank Benchmark for Distributed Transaction System

import asyncio
import time
import random
import numpy as np
from typing import Dict, Any, List, Tuple, Optional
from collections import defaultdict
import json
import sys
from datetime import datetime

from config import *
from client import Client


class WorkloadConfig:
    
    def __init__(self,
                 read_write_ratio: float = 0.5,
                 cross_shard_ratio: float = 0.3,
                 skew: float = 0.0,
                 duration: int = 60,
                 target_tps: int = 100,
                 num_clients: int = 10):
        #Initialize workload configuration
        self.read_write_ratio = read_write_ratio
        self.cross_shard_ratio = cross_shard_ratio
        self.skew = skew
        self.duration = duration
        self.target_tps = target_tps
        self.num_clients = num_clients
        
        # Validate parameters
        assert 0.0 <= read_write_ratio <= 1.0, "read_write_ratio must be in [0, 1]"
        assert 0.0 <= cross_shard_ratio <= 1.0, "cross_shard_ratio must be in [0, 1]"
        assert 0.0 <= skew <= 1.0, "skew must be in [0, 1]"
        assert duration > 0, "duration must be positive"
        assert target_tps > 0, "target_tps must be positive"
        assert num_clients > 0, "num_clients must be positive"


class ZipfianGenerator:
    
    def __init__(self, n: int, skew: float):
        #Initialize Zipfian generator for n items with given skew
        self.n = n
        self.skew = skew
        
        if skew == 0.0:
            # Uniform distribution
            self.zipf_values = None
        else:
            ranks = np.arange(1, n + 1)
            weights = 1.0 / np.power(ranks, skew)
            self.zipf_values = weights / np.sum(weights)
    
    def next(self) -> int:
        if self.zipf_values is None:
            # Uniform distribution
            return random.randint(0, self.n - 1)
        else:
            # Zipfian distribution
            return np.random.choice(self.n, p=self.zipf_values)


class AccountSelector:
    
    def __init__(self, config: WorkloadConfig):
        self.config = config
        
        # Total accounts across all shards
        self.total_accounts = 9000
        
        # Accounts per cluster
        self.cluster_ranges = {
            1: (1, 3000),
            2: (3001, 6000),
            3: (6001, 9000)
        }
        
        # Create Zipfian generators for each cluster
        if config.skew > 0.0:
            self.generators = {
                cluster_id: ZipfianGenerator(
                    end - start + 1,
                    config.skew
                )
                for cluster_id, (start, end) in self.cluster_ranges.items()
            }
        else:
            self.generators = None
    
    def select_account(self, cluster_id: int) -> int:
        start, end = self.cluster_ranges[cluster_id]
        
        if self.generators is None:
            # Uniform distribution
            return random.randint(start, end)
        else:
            # Zipfian distribution
            offset = self.generators[cluster_id].next()
            return start + offset
    
    def select_intra_shard_pair(self) -> Tuple[int, int]:
        cluster_id = random.randint(1, NUM_CLUSTERS)
        
        sender = self.select_account(cluster_id)
        receiver = self.select_account(cluster_id)
        
        # Ensure different accounts
        while receiver == sender:
            receiver = self.select_account(cluster_id)
        
        return sender, receiver
    
    def select_cross_shard_pair(self) -> Tuple[int, int]:
        # Select two accounts from different clusters (cross-shard)
        # Pick two different clusters
        cluster1 = random.randint(1, NUM_CLUSTERS)
        cluster2 = random.randint(1, NUM_CLUSTERS)
        
        while cluster2 == cluster1:
            cluster2 = random.randint(1, NUM_CLUSTERS)
        
        sender = self.select_account(cluster1)
        receiver = self.select_account(cluster2)
        
        return sender, receiver
    
    def select_transaction_accounts(self, is_cross_shard: bool) -> Tuple[int, int]:
        # Select accounts for a transaction
        if is_cross_shard:
            return self.select_cross_shard_pair()
        else:
            return self.select_intra_shard_pair()


class SmallBankBenchmark:
    
    def __init__(self, config: WorkloadConfig):
        self.config = config
        self.account_selector = AccountSelector(config)
        
        # Statistics
        self.stats = {
            'total_transactions': 0,
            'successful_transactions': 0,
            'failed_transactions': 0,
            'read_transactions': 0,
            'write_transactions': 0,
            'intra_shard_transactions': 0,
            'cross_shard_transactions': 0,
            'latencies': [],
            'throughput_samples': [],
            'start_time': None,
            'end_time': None,
            'errors': defaultdict(int)
        }
        
        self.running = False
        self.clients = []
    
    def generate_transaction(self) -> Dict[str, Any]:
        # Decide if read or write
        is_read = random.random() < self.config.read_write_ratio
        
        if is_read:
            # Balance query (read-only)
            cluster_id = random.randint(1, NUM_CLUSTERS)
            account_id = self.account_selector.select_account(cluster_id)
            
            return {
                'type': 'balance',
                'account_id': account_id,
                'is_read': True,
                'is_cross_shard': False
            }
        else:
            # Transfer (write)
            is_cross_shard = random.random() < self.config.cross_shard_ratio
            
            sender, receiver = self.account_selector.select_transaction_accounts(is_cross_shard)
            
            # Random amount between 1 and 5
            amount = random.randint(1, 5)
            
            return {
                'type': 'transfer',
                'sender': sender,
                'receiver': receiver,
                'amount': amount,
                'is_read': False,
                'is_cross_shard': is_cross_shard
            }
    
    async def execute_transaction(self, client: Client, txn: Dict[str, Any]) -> Dict[str, Any]:
        # Execute a single transaction and record results
        start_time = time.time()
        
        try:
            if txn['type'] == 'balance':
                result = await client.get_balance(txn['account_id'])
            else:  # transfer
                result = await client.transfer(
                    txn['sender'],
                    txn['receiver'],
                    txn['amount']
                )
            
            latency = time.time() - start_time
            
            return {
                'success': result.get('success', False),
                'latency': latency,
                'txn': txn,
                'error': result.get('error') if not result.get('success') else None
            }
        
        except Exception as e:
            latency = time.time() - start_time
            return {
                'success': False,
                'latency': latency,
                'txn': txn,
                'error': str(e)
            }
    
    async def client_worker(self, client_id: int):
        # Worker thread that continuously generates and executes transactions
        client = Client(client_id=client_id)
        
        # Calculate per-client rate (transactions per second)
        per_client_rate = self.config.target_tps / self.config.num_clients
        interval = 1.0 / per_client_rate if per_client_rate > 0 else 0
        
        last_txn_time = time.time()
        
        while self.running:
            # Generate transaction
            txn = self.generate_transaction()
            
            # Execute transaction
            result = await self.execute_transaction(client, txn)
            
            # Record statistics
            self.stats['total_transactions'] += 1
            
            if result['success']:
                self.stats['successful_transactions'] += 1
            else:
                self.stats['failed_transactions'] += 1
                if result['error']:
                    self.stats['errors'][result['error']] += 1
            
            if txn['is_read']:
                self.stats['read_transactions'] += 1
            else:
                self.stats['write_transactions'] += 1
            
            if txn['is_cross_shard']:
                self.stats['cross_shard_transactions'] += 1
            else:
                self.stats['intra_shard_transactions'] += 1
            
            self.stats['latencies'].append(result['latency'])
            
            # Rate limiting: sleep to maintain target TPS
            if interval > 0:
                elapsed = time.time() - last_txn_time
                sleep_time = max(0, interval - elapsed)
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                last_txn_time = time.time()
        
        await client.close()
    
    async def throughput_monitor(self):
        # Monitor and record throughput every second
        last_count = 0
        
        while self.running:
            await asyncio.sleep(1.0)
            
            current_count = self.stats['successful_transactions']
            tps = current_count - last_count
            self.stats['throughput_samples'].append(tps)
            last_count = current_count
    
    async def run(self):
        # Run the benchmark
        print("=" * 80)
        print("SmallBank Benchmark")
        print("=" * 80)
        print(f"Configuration:")
        print(f"  Duration: {self.config.duration}s")
        print(f"  Target TPS: {self.config.target_tps}")
        print(f"  Concurrent clients: {self.config.num_clients}")
        print(f"  Read/Write ratio: {self.config.read_write_ratio:.2f}")
        print(f"  Cross-shard ratio: {self.config.cross_shard_ratio:.2f}")
        print(f"  Skew (Zipfian): {self.config.skew:.2f}")
        print("=" * 80)
        
        # Start benchmark
        self.running = True
        self.stats['start_time'] = time.time()
        
        # Create client workers
        workers = [
            asyncio.create_task(self.client_worker(i))
            for i in range(self.config.num_clients)
        ]
        
        # Create throughput monitor
        monitor = asyncio.create_task(self.throughput_monitor())
        
        # Run for specified duration
        print(f"\nRunning benchmark for {self.config.duration} seconds...")
        
        # Progress updates every 10 seconds
        for i in range(self.config.duration // 10):
            await asyncio.sleep(10)
            elapsed = (i + 1) * 10
            tps = self.stats['successful_transactions'] / elapsed
            print(f"  [{elapsed}s] Transactions: {self.stats['total_transactions']}, "
                  f"Success: {self.stats['successful_transactions']}, "
                  f"TPS: {tps:.2f}")
        
        # Wait for remaining time
        remaining = self.config.duration % 10
        if remaining > 0:
            await asyncio.sleep(remaining)
        
        # Stop benchmark
        self.running = False
        self.stats['end_time'] = time.time()
        
        # Wait for workers to finish
        for worker in workers:
            worker.cancel()
        
        monitor.cancel()
        
        await asyncio.gather(*workers, monitor, return_exceptions=True)
        
        print("\nBenchmark completed!")
        
        # Print results
        self.print_results()
    
    def print_results(self):
        # Print benchmark results
        duration = self.stats['end_time'] - self.stats['start_time']
        
        print("\n" + "=" * 80)
        print("Benchmark Results")
        print("=" * 80)
        
        # Overall statistics
        print(f"\nOverall Statistics:")
        print(f"  Duration: {duration:.2f}s")
        print(f"  Total transactions: {self.stats['total_transactions']}")
        print(f"  Successful transactions: {self.stats['successful_transactions']}")
        print(f"  Failed transactions: {self.stats['failed_transactions']}")
        print(f"  Success rate: {self.stats['successful_transactions'] / max(1, self.stats['total_transactions']) * 100:.2f}%")
        
        # Throughput
        avg_tps = self.stats['successful_transactions'] / duration
        print(f"\nThroughput:")
        print(f"  Average TPS: {avg_tps:.2f}")
        
        if self.stats['throughput_samples']:
            print(f"  Peak TPS: {max(self.stats['throughput_samples'])}")
            print(f"  Min TPS: {min(self.stats['throughput_samples'])}")
        
        # Latency
        if self.stats['latencies']:
            latencies = sorted(self.stats['latencies'])
            n = len(latencies)
            
            print(f"\nLatency (ms):")
            print(f"  Mean: {np.mean(latencies) * 1000:.2f}")
            print(f"  Median: {np.median(latencies) * 1000:.2f}")
            print(f"  p50: {latencies[int(n * 0.50)] * 1000:.2f}")
            print(f"  p90: {latencies[int(n * 0.90)] * 1000:.2f}")
            print(f"  p95: {latencies[int(n * 0.95)] * 1000:.2f}")
            print(f"  p99: {latencies[int(n * 0.99)] * 1000:.2f}")
            print(f"  Min: {min(latencies) * 1000:.2f}")
            print(f"  Max: {max(latencies) * 1000:.2f}")
        
        # Transaction breakdown
        print(f"\nTransaction Breakdown:")
        print(f"  Read transactions: {self.stats['read_transactions']} "
              f"({self.stats['read_transactions'] / max(1, self.stats['total_transactions']) * 100:.1f}%)")
        print(f"  Write transactions: {self.stats['write_transactions']} "
              f"({self.stats['write_transactions'] / max(1, self.stats['total_transactions']) * 100:.1f}%)")
        print(f"  Intra-shard: {self.stats['intra_shard_transactions']} "
              f"({self.stats['intra_shard_transactions'] / max(1, self.stats['total_transactions']) * 100:.1f}%)")
        print(f"  Cross-shard: {self.stats['cross_shard_transactions']} "
              f"({self.stats['cross_shard_transactions'] / max(1, self.stats['total_transactions']) * 100:.1f}%)")
        
        # Errors
        if self.stats['errors']:
            print(f"\nErrors:")
            for error, count in sorted(self.stats['errors'].items(), key=lambda x: x[1], reverse=True):
                print(f"  {error}: {count}")
        
        print("\n" + "=" * 80)
    
    def save_results(self, filename: str):
        # Save results to JSON file
        results = {
            'config': {
                'read_write_ratio': self.config.read_write_ratio,
                'cross_shard_ratio': self.config.cross_shard_ratio,
                'skew': self.config.skew,
                'duration': self.config.duration,
                'target_tps': self.config.target_tps,
                'num_clients': self.config.num_clients
            },
            'stats': {
                'duration': self.stats['end_time'] - self.stats['start_time'],
                'total_transactions': self.stats['total_transactions'],
                'successful_transactions': self.stats['successful_transactions'],
                'failed_transactions': self.stats['failed_transactions'],
                'read_transactions': self.stats['read_transactions'],
                'write_transactions': self.stats['write_transactions'],
                'intra_shard_transactions': self.stats['intra_shard_transactions'],
                'cross_shard_transactions': self.stats['cross_shard_transactions'],
                'avg_tps': self.stats['successful_transactions'] / (self.stats['end_time'] - self.stats['start_time']),
                'latency_percentiles': {
                    'p50': float(np.percentile(self.stats['latencies'], 50)),
                    'p90': float(np.percentile(self.stats['latencies'], 90)),
                    'p95': float(np.percentile(self.stats['latencies'], 95)),
                    'p99': float(np.percentile(self.stats['latencies'], 99)),
                } if self.stats['latencies'] else {},
                'errors': dict(self.stats['errors'])
            },
            'timestamp': datetime.now().isoformat()
        }
        
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"\nResults saved to {filename}")


async def run_benchmark_suite():
    # Run a suite of benchmarks with different configurations
    
    scenarios = [
        {
            'name': 'Baseline - Balanced Workload',
            'config': WorkloadConfig(
                read_write_ratio=0.5,
                cross_shard_ratio=0.3,
                skew=0.0,
                duration=60,
                target_tps=1000,
                num_clients=20
            )
        },
        {
            'name': 'Read-Heavy Workload',
            'config': WorkloadConfig(
                read_write_ratio=0.8,
                cross_shard_ratio=0.3,
                skew=0.0,
                duration=60,
                target_tps=1000,
                num_clients=20
            )
        },
        {
            'name': 'Write-Heavy Workload',
            'config': WorkloadConfig(
                read_write_ratio=0.2,
                cross_shard_ratio=0.3,
                skew=0.0,
                duration=60,
                target_tps=1000,
                num_clients=20
            )
        },
        {
            'name': 'High Cross-Shard Workload',
            'config': WorkloadConfig(
                read_write_ratio=0.5,
                cross_shard_ratio=0.7,
                skew=0.0,
                duration=60,
                target_tps=1000,
                num_clients=20
            )
        },
        {
            'name': 'Skewed Workload (Hotspots)',
            'config': WorkloadConfig(
                read_write_ratio=0.5,
                cross_shard_ratio=0.3,
                skew=0.8,
                duration=60,
                target_tps=1000,
                num_clients=20
            )
        }
    ]
    
    print("\n" + "#" * 80)
    print("# SmallBank Benchmark Suite")
    print("#" * 80)
    print(f"\nRunning {len(scenarios)} benchmark scenarios...\n")
    
    for i, scenario in enumerate(scenarios, 1):
        print(f"\n{'=' * 80}")
        print(f"Scenario {i}/{len(scenarios)}: {scenario['name']}")
        print(f"{'=' * 80}")
        
        benchmark = SmallBankBenchmark(scenario['config'])
        await benchmark.run()
        
        # Save results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"benchmark_results_{timestamp}_scenario{i}.json"
        benchmark.save_results(filename)
        
        # Wait between scenarios
        if i < len(scenarios):
            print(f"\nWaiting 5 seconds before next scenario...")
            await asyncio.sleep(5)
    
    print("\n" + "#" * 80)
    print("# Benchmark Suite Complete!")
    print("#" * 80)


async def main():
    # Main entry point
    if len(sys.argv) > 1:
        if len(sys.argv) >= 7:
            config = WorkloadConfig(
                read_write_ratio=float(sys.argv[1]),
                cross_shard_ratio=float(sys.argv[2]),
                skew=float(sys.argv[3]),
                duration=int(sys.argv[4]),
                target_tps=int(sys.argv[5]),
                num_clients=int(sys.argv[6])
            )
            
            print("\nRunning custom benchmark configuration...")
            benchmark = SmallBankBenchmark(config)
            await benchmark.run()
            
            # Save results
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"benchmark_results_{timestamp}_custom.json"
            benchmark.save_results(filename)
        else:
            print("Usage: python benchmark.py <read_ratio> <cross_shard_ratio> <skew> <duration> <target_tps> <num_clients>")
            print("Example: python benchmark.py 0.5 0.3 0.0 60 100 10")
            print("\nOr run without arguments to execute the full benchmark suite")
            sys.exit(1)
    else:
        # Run full benchmark suite
        await run_benchmark_suite()


if __name__ == '__main__':
    asyncio.run(main())