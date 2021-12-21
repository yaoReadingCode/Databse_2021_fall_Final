import mysql_check
import pandas as pd


def question_3_example_get_customers(country):
    """
    This is an example of what your answers should look like.
    :param country:
    :return:
    """

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("admin", "7Senses_kiki", "tutorialdb.cbezzskgwcl3.us-east-2.rds.amazonaws.com")

    # You will provide your SQL queries in this format. %s is a parameter.
    q = """
        select customerNumber, customerName, country
            from classicmodels.customers
            where
            country = %s
    """

    # Connect and run the query
    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q, [country])
    result = cur.fetchall()

    # Convert to a Data Frame and return
    result = pd.DataFrame(result)

    return result


def question_3_revenue_by_country():

    mysql_check.set_connect_info("admin", "7Senses_kiki", "tutorialdb.cbezzskgwcl3.us-east-2.rds.amazonaws.com")

    q = """
        select customers.country, sum(orderdetails.quantityOrdered * orderdetails.priceEach) as revenue
            from ((classicmodels.orderdetails join classicmodels.orders
                on orderdetails.orderNumber = orders.orderNumber and orders.status = 'Shipped')
                join classicmodels.customers
                on orders.customerNumber = customers.customerNumber)
            group by customers.country
    """

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)
    result = cur.fetchall()

    # Convert to a Data Frame and return
    result = pd.DataFrame(result)

    return result


def question_3_purchases_and_payments():

    mysql_check.set_connect_info("admin", "7Senses_kiki", "tutorialdb.cbezzskgwcl3.us-east-2.rds.amazonaws.com")

    q = """
        select T.customerNumber, T.customerName, total_spent,
            sum(payments.amount) as total_payments, (total_spent - sum(payments.amount)) as total_unpaid
            from (classicmodels.payments join
            (select customers.customerNumber, customers.customerName, 
                sum(orderdetails.quantityOrdered * orderdetails.priceEach) as total_spent
                from ((classicmodels.orderdetails join classicmodels.orders
                    on orderdetails.orderNumber = orders.orderNumber)
                    join classicmodels.customers
                    on orders.customerNumber = customers.customerNumber)
                group by customers.customerNumber)
                as T
            on payments.customerNumber = T.customerNumber)
            group by customerNumber
            order by customerName
    """

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)
    result = cur.fetchall()

    # Convert to a Data Frame and return
    result = pd.DataFrame(result)

    return result


def question_3_customers_and_lines():

    mysql_check.set_connect_info("admin", "7Senses_kiki", "tutorialdb.cbezzskgwcl3.us-east-2.rds.amazonaws.com")

    q = """
        select distinct customerNumber, customerName
            from classicmodels.customers
            where customerNumber not in 
            (select distinct T.customerNumber
                from (classicmodels.products join
                (select customers.customerNumber, customers.customerName, orderdetails.productCode
                    from ((classicmodels.orderdetails join classicmodels.orders
                        on orderdetails.orderNumber = orders.orderNumber)
                        join classicmodels.customers
                        on orders.customerNumber = customers.customerNumber))
                    as T
                on products.productCode = T.productCode)
                where products.productLine = 'Plane' or products.productLine = 'Trucks and Buses')  
    """

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)
    result = cur.fetchall()

    # Convert to a Data Frame and return
    result = pd.DataFrame(result)

    return result


