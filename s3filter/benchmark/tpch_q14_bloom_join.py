# -*- coding: utf-8 -*-
"""TPCH Q14 Bloom Join Benchmark

"""

import os
from datetime import datetime, timedelta

from s3filter import ROOT_DIR
from s3filter.plan.query_plan import QueryPlan
from s3filter.query import tpch_q14
from s3filter.util.test_util import gen_test_id


def main():
    run(True)
    run(False)


def run(is_streamed):
    """

    :return: None
    """

    print('')
    print("TPCH Q14 Bloom Join")
    print("-------------------")

    query_plan = QueryPlan(None, is_streamed)

    # Query plan
    # DATE is the first day of a month randomly selected from a random year within [1993 .. 1997].
    date = '1993-01-01'
    min_shipped_date = datetime.strptime(date, '%Y-%m-%d')
    max_shipped_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=30)

    part_scan = query_plan.add_operator(
        tpch_q14.sql_scan_part_partkey_type_part_where_brand12_operator_def('part_scan'))
    part_scan_project = query_plan.add_operator(tpch_q14.project_partkey_type_operator_def('part_scan_project'))
    part_bloom_create = query_plan.add_operator(tpch_q14.bloom_create_p_partkey_operator_def('part_bloom_create'))
    lineitem_scan = query_plan.add_operator(
        tpch_q14.bloom_scan_lineitem_where_shipdate_operator_def(min_shipped_date, max_shipped_date, 'lineitem_scan'))
    lineitem_scan_project = query_plan.add_operator(
        tpch_q14.project_partkey_extendedprice_discount_operator_def('lineitem_scan_project'))
    join = query_plan.add_operator(tpch_q14.join_part_lineitem_operator_def('join'))
    aggregate = query_plan.add_operator(tpch_q14.aggregate_promo_revenue_operator_def('aggregate'))
    aggregate_project = query_plan.add_operator(tpch_q14.project_promo_revenue_operator_def('aggregate_project'))
    collate = query_plan.add_operator(tpch_q14.collate_operator_def('collate'))

    part_scan.connect(part_scan_project)
    part_scan_project.connect(part_bloom_create)
    part_bloom_create.connect(lineitem_scan)
    join.connect_left_producer(part_scan_project)
    lineitem_scan.connect(lineitem_scan_project)
    join.connect_right_producer(lineitem_scan_project)
    join.connect(aggregate)
    aggregate.connect(aggregate_project)
    aggregate_project.connect(collate)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../benchmark-output"), gen_test_id())

    # Start the query
    query_plan.execute()

    # Assert the results
    # num_rows = 0
    # for t in collate.tuples():
    #     num_rows += 1
    #     print("{}:{}".format(num_rows, t))

    collate.print_tuples()

    # Write the metrics
    query_plan.print_metrics()

    field_names = ['promo_revenue']

    assert len(collate.tuples()) == 1 + 1

    assert collate.tuples()[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    assert collate.tuples()[1] == [15.090116526324298]


if __name__ == "__main__":
    main()
