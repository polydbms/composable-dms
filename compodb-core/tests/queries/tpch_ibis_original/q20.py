from .utils import add_date


def tpc_h20(con, COLOR="forest", DATE="1994-01-01", NATION="CANADA"):
    """Potential Part Promotion Query (Q20)

    The Potential Part Promotion Query identifies suppliers in a particular
    nation having selected parts that may be candidates for a promotional
    offer."""

    supplier = con.table("supplier")
    nation = con.table("nation")
    partsupp = con.table("partsupp")
    part = con.table("part")
    lineitem = con.table("lineitem")

    q1 = supplier.join(nation, supplier.s_nationkey == nation.n_nationkey)

    q3 = part.filter([part.p_name.like(f"{COLOR}%")])
    q2 = partsupp

    q4 = lineitem.filter(
        [
            lineitem.l_partkey == q2.ps_partkey,
            lineitem.l_suppkey == q2.ps_suppkey,
            lineitem.l_shipdate >= DATE,
            lineitem.l_shipdate < add_date(DATE, dy=1),
        ]
    )

    q2 = q2.filter(
        [
            partsupp.ps_partkey.isin(q3.p_partkey),
            partsupp.ps_availqty > 0.5 * q4.l_quantity.sum(),
        ]
    )

    q1 = q1.filter([q1.n_name == NATION, q1.s_suppkey.isin(q2.ps_suppkey)])

    q1 = q1[q1.s_name, q1.s_address]

    return q1.order_by(q1.s_name)