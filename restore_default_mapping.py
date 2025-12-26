def restore_default_mapping():
    
    print("="*60)
    print("Restoring Default Shard Mapping")
    print("="*60)
    
    with open('config.py', 'r') as f:
        lines = f.readlines()
    
    start_idx = None
    end_idx = None
    
    for i, line in enumerate(lines):
        if 'SHARD_MAPPING = {' in line:
            start_idx = i
        if start_idx is not None and '}' in line:
            end_idx = i
            break
    
    if start_idx is None:
        print("ERROR: Could not find SHARD_MAPPING in config.py")
        return False
    
    new_lines = [
        'SHARD_MAPPING = {\n',
        '    1: (1, 3000),\n',
        '    2: (3001, 6000),\n',
        '    3: (6001, 9000),\n',
        '}\n'
    ]
    
    lines = lines[:start_idx] + new_lines + lines[end_idx+1:]
    
    with open('config.py', 'w') as f:
        f.writelines(lines)
    
    print("\n✓ Shard mapping restored to default:")
    print("  Cluster 1: accounts 1-3000")
    print("  Cluster 2: accounts 3001-6000")
    print("  Cluster 3: accounts 6001-9000")
    print("\n" + "="*60)
    
    return True


if __name__ == '__main__':
    restore_default_mapping()
    print("\nDone! You can now run your tests.")