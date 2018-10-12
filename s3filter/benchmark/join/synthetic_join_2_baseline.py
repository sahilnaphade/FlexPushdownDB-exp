# -*- coding: utf-8 -*-
"""Synthetic Baseline Benchmarks

"""

from datetime import datetime

from s3filter.benchmark.join import runner
from s3filter.query.join import synthetic_join_baseline
from s3filter.query.join.synthetic_join_settings import SyntheticBaselineJoinSettings
import pandas as pd

from s3filter.util.test_util import gen_test_id
import numpy as np

def main():
    max_orderdate = datetime.strptime('1992-01-15', '%Y-%m-%d')
    min_shipdate = datetime.strptime('1992-01-15', '%Y-%m-%d')

    settings = SyntheticBaselineJoinSettings(
        parallel=True, use_pandas=True, secure=False, use_native=False, buffer_size=0,
        use_shared_mem=False, shared_memory_size=-1, sf=1,
        table_A_key='customer',
        table_A_parts=2,
        table_A_sharded=False,
        table_A_field_names=['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal',
                             'c_mktsegment',
                             'c_comment'],
        table_A_filter_fn=lambda df: df['c_custkey'].astype(np.int) <= 100,
        table_A_AB_join_key='c_custkey',
        table_B_key='orders',
        table_B_parts=2,
        table_B_sharded=False,
        table_B_field_names=['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate',
                             'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment'],
        table_B_filter_fn=lambda df: pd.to_datetime(df['o_orderdate']) < max_orderdate,
        table_B_AB_join_key='o_custkey',
        table_B_BC_join_key=None,
        table_B_detail_field_name='o_totalprice',
        table_C_key=None,
        table_C_parts=None,
        table_C_sharded=None,
        table_C_field_names=None,
        table_C_filter_fn=None,
        table_C_BC_join_key=None,
        table_C_detail_field_name=None)

    # expected_result = 1171288505.15
    expected_result = 4429.68

    query_plan = synthetic_join_baseline.query_plan(settings)

    runner.run(query_plan, expected_result=expected_result, test_id=gen_test_id())


if __name__ == "__main__":
    main()
