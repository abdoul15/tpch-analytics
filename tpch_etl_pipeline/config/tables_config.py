TABLE_PARTITION_CONFIG = {
    'public.lineitem': {
        'partition_column': 'l_orderkey'
    },
    'public.orders': {
        'partition_column': 'o_orderkey'
    },
    'public.customer': {
        'partition_column': 'c_custkey'
    },

    'public.partsupp': {
        'partition_column': 'ps_partkey'
    },

    'public.part': {
        'partition_column': 'p_partkey'
    },

    'public.supplier': {
        'partition_column': 's_suppkey'
    },

    # Pour les petites tables comme nation/region
    'public.nation': None,
    'public.region': None
}