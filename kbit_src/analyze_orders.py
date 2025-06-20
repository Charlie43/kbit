import json
from itertools import count
from pathlib import Path

import polars

import duckdb
from duckdb import DuckDBPyConnection
from polars import DataFrame

from kbit_src.utils.data_load import load_strats, setup_duckdb


# just realised my screen didnt start recording, no idea why not, sorry!

# Order Reconstruction: For each unique order (client_order_id), reconstruct its complete lifecycle by:
# Identifying all related events (updates and executions) from the order_events table
# Sorting them chronologically by event_time
# Detecting anomalies such as:
# Missing NEW status (orders that start with PARTIALLY_FILLED or FILLED)
# Out-of-order status transitions (e.g., CANCELLED → PARTIALLY_FILLED)
# Duplicate status updates
# Time-travel (events with timestamps out of sequence)


def main():
    strats: DataFrame = load_strats()
    con: DuckDBPyConnection = setup_duckdb()

    con.execute("""create or replace table orders_with_timeline as (
    select client_order_id, array_agg(order_status order by event_time) as timeline
    from pg.order_events
    group by client_order_id)""")

    # TODO: how do i know the expected order?
    # basically if anything comes after cancelled?
    # NEW, PARTIALLY_FILLED, FILLED, CANCELLED, REJECTED

    # TODO: should've started with order by event_type
    out_of_order_orders = []

    for order in con.execute("SELECT * FROM orders_with_timeline").fetchall():
        order_timeline: list[str]
        order_id, order_timeline = order
        order_timeline = list(filter(lambda x: x is not None, order_timeline))

        # TODO: should've just used ROW_NUMBER for this but oh well
        if 'CANCELLED' in order_timeline and order_timeline.index('CANCELLED') != len(order_timeline) - 1:
            out_of_order_orders.append(order_id)

    orders_missing_new = """select client_order_id
                            from orders_with_timeline
                            where not array_to_string(timeline, ',') like '%NEW%'"""

    recon_quantity = """
                     with exec_quantity_per_order as (select client_order_id, sum(exec_quantity) as sum_exec_quantity
                                                      from pg.order_events
                                                      where event_type = 'EXECUTION'
                                                      group by client_order_id),
                          final_fill_quantity as (select order_events.client_order_id,
                                                         id,
                                                         order_events.filled_quantity,
                                                         row_number()
                                                         over (PARTITION BY order_events.client_order_id order by order_events.event_time desc) as rn
                                                  from pg.order_events
                                                  where event_type = 'ORDER_UPDATE')
                     select a.client_order_id
                     from pg.order_events a
                              join exec_quantity_per_order b on a.client_order_id = b.client_order_id
                              join final_fill_quantity c
                                   on a.client_order_id = c.client_order_id and a.id = c.id and c.rn = 1
                     where a.filled_quantity != b.sum_exec_quantity"""

    has_exec_but_no_update = """
                             with has_exec as (select distinct client_order_id
                                               from pg.order_events
                                               where event_type = 'EXECUTION'),
                                  has_order_update as (select distinct client_order_id
                                                       from pg.order_events
                                                       where event_type = 'ORDER_UPDATE')
                             select b.client_order_id
                             from pg.order_events a
                                      join has_exec b on a.client_order_id = b.client_order_id
                                      left join has_order_update c on b.client_order_id = c.client_order_id
                             where c.client_order_id is null"""

    mismatch_fee = """
                   select client_order_id
                   from pg.order_events
                   where fee is not null
                       and (is_maker = True
                           AND (fee = round((exec_quantity * price * 0.0002), 8)))
                      OR (is_maker = False AND (fee = round((exec_quantity * price * 0.0004), 8)));"""

    # TODO: should've just used string builder or something
    output = ['=== Order Analysis Report ===']
    # Total Orders: X
    unique_orders = con.execute('select count(distinct client_order_id) from pg.order_events').fetchall()[0][0]

    output += [f'Total Orders: {unique_orders}']

    orders_by_strat = """select strategy_id, count(distinct client_order_id)
                         from pg.order_events
                         group by strategy_id;"""

    output += ['Orders by Strategy:']
    orders_by_strat = con.execute(orders_by_strat).fetchall()
    output += [f'- {x[0]}: {x[1]} orders' for x in orders_by_strat]
    orders_with_issue = ''
    total_orders_missing_new = [x[0] for x in con.execute(orders_missing_new).fetchall()]
    orders_with_issue += ': Missing NEW status\n'.join(
        total_orders_missing_new) + ': Missing NEW status\n'
    orders_with_issue += ': Out of order events (CANCELLED before PARTIALLY_FILLED)\n'.join(
        out_of_order_orders) + ': Out of order events (CANCELLED before PARTIALLY_FILLED)\n'

    output += ['Lifecycle Anomalies Found: \n' + orders_with_issue]
    orders_with_exec_issue = ''
    mismatched_quantities = [x[0] for x in con.execute(recon_quantity).fetchall()]
    orders_with_exec_issue += ': Mismatched Exec Quantity with Filled Quantity\n'.join(
        mismatched_quantities) + ': Mismatched Exec Quantity with Filled Quantity\n'
    mismatched_fee = [x[0] for x in con.execute(mismatch_fee).fetchall()]
    orders_with_exec_issue += ': Mismatched Fee\n'.join(
        mismatched_fee) + ': Mismatched Fee\n'
    exec_no_update = [x[0] for x in con.execute(has_exec_but_no_update).fetchall()]
    orders_with_exec_issue += ': Has Exec but no Update\n'.join(
        exec_no_update) + ': Has Exec but no Update\n'

    output += ['Execution Mismatches: \n' + orders_with_exec_issue]

    incorrect_orders = sum([len(total_orders_missing_new), len(out_of_order_orders),
                            len(mismatched_quantities), len(mismatched_fee), len(exec_no_update)])

    output += [f'Data Quality Score: {round((unique_orders / incorrect_orders) * 100)}%']
    print('\n'.join(output))


# TODO: should've just started with this instead of playing around but oh well
def hacky_but_quick():
    con: DuckDBPyConnection = setup_duckdb()

    order_ids = con.execute("SELECT DISTINCT client_order_id from pg.order_events").fetchall()

    for order in order_ids:
        # do everything in this loop 1 by 1
        pass


def print_summary():
    pass


# Every EXECUTION event has at least one corresponding ORDER_UPDATE event
# No orphaned executions (executions without any order updates)
# isnt this the same thing?

#     todo: just noticed the time


# TODO: pip freeze to update requirements


if __name__ == '__main__':
    main()
