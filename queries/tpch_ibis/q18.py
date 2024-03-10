import ibis


def tpc_h18(con, QUANTITY=300):
    """Large Volume Customer Query (Q18)

    The Large Volume Customer Query ranks customers based on their having
    placed a large quantity order. Large quantity orders are defined as those
    orders whose total quantity is above a certain level."""

    customer = con.table("customer")
    orders = con.table("orders")
    t = con.table("lineitem")

    subgq = t.group_by([t.l_orderkey])
    subq = subgq.aggregate(qty_sum=t.l_quantity.sum())
    subq = subq.filter([subq.qty_sum > QUANTITY])

    q = customer
    q = q.join(orders, customer.c_custkey == orders.o_custkey)
    q = q.join(t, orders.o_orderkey == t.l_orderkey)
    q = q.filter([q.o_orderkey.isin(subq.l_orderkey)])

    gq = q.group_by(
        [q.c_name, q.c_custkey, q.o_orderkey, q.o_orderdate, q.o_totalprice]
    )
    q = gq.aggregate(sum_qty=q.l_quantity.sum())
    q = q.order_by([ibis.desc(q.o_totalprice), q.o_orderdate])
    return q.limit(100)
