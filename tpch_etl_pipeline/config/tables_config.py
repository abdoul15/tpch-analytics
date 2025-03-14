TABLE_PARTITION_CONFIG = {
    'public.lineitem': {
        'partition_column': 'l_orderkey',
        'num_partitions': 13
    },
    'public.orders': {
        'partition_column': 'o_orderkey', 
        'num_partitions': 10
    },
    'public.customer': {
        'partition_column': 'c_custkey',
        'num_partitions': 5
    },

    'public.partsupp': {
        'partition_column': 'ps_partkey',
        'num_partitions': 5
    },

    'public.supplier': {
        'partition_column': 's_suppkey',
        'num_partitions': 5
    },

    # Pour les petites tables comme nation/region
    'public.nation': None,
    'public.region': None
}