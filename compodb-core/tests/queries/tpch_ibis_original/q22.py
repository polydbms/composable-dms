def tpc_h22(con, COUNTRY_CODES=("13", "31", "23", "29", "30", "18", "17")):
    """Global Sales Opportunity Query (Q22)

    The Global Sales Opportunity Query identifies geographies where there are
    customers who may be likely to make a purchase."""

    customer = con.table("customer")
    orders = con.table("orders")

    q = customer.filter(
        [
            customer.c_acctbal > 0.00,
            customer.c_phone.substr(0, 2).isin(COUNTRY_CODES),
        ]
    )
    q = q.aggregate(avg_bal=customer.c_acctbal.mean())

    custsale = customer.filter(
        [
            customer.c_phone.substr(0, 2).isin(COUNTRY_CODES),
            customer.c_acctbal > q.avg_bal,
            ~(orders.o_custkey == customer.c_custkey).any(),
        ]
    )
    custsale = custsale[
        customer.c_phone.substr(0, 2).name("cntrycode"), customer.c_acctbal
    ]

    gq = custsale.group_by(custsale.cntrycode)
    outerq = gq.aggregate(numcust=custsale.count(), totacctbal=custsale.c_acctbal.sum())

    return outerq.order_by(outerq.cntrycode)