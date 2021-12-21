import mysql_check
import pandas as pd


def schema_operation_1():
    """
    This is an example of what your answers should look like.
    :param country:
    :return:
    """

    res = None

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("admin", "7Senses_kiki", "tutorialdb.cbezzskgwcl3.us-east-2.rds.amazonaws.com")

    q0 = """
    drop table if exists classicmodels_star.date_dimension;
    """

    q = """
    create table classicmodels_star.date_dimension
    (
        orderDate date not null,
        month int null,
        quarter int null,
        year int null,
        constraint date_dimension_pk
            primary key (orderDate)
    );
    """

    print('DDL statement:')
    print(q)
    print()

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res0 = cur.execute(q0)
    res = cur.execute(q)

    return res


def schema_operation_2():
    """
    This is an example of what your answers should look like.
    :param country:
    :return:
    """

    res = None

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("admin", "7Senses_kiki", "tutorialdb.cbezzskgwcl3.us-east-2.rds.amazonaws.com")

    q0 = """
    drop table if exists classicmodels_star.location_dimension;
    """

    q = """
    create table classicmodels_star.location_dimension
    (
        customerNumber int not null,
        city varchar(50) null,
        country varchar(50) null,
        region varchar(4) null,
        constraint location_dimension_pk
            primary key (customerNumber)
    );
    """

    print('DDL statement:')
    print(q)
    print()

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res0 = cur.execute(q0)
    res = cur.execute(q)

    return res


def schema_operation_3():
    """
    This is an example of what your answers should look like.
    :param country:
    :return:
    """

    res = None

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("admin", "7Senses_kiki", "tutorialdb.cbezzskgwcl3.us-east-2.rds.amazonaws.com")

    q0 = """
    drop table if exists classicmodels_star.product_dimension;
    """

    q = """
    create table classicmodels_star.product_dimension
    (
        productCode varchar(15) not null,
        productScale varchar(10) null,
        productLine varchar(50) null,
        constraint product_dimension_pk
            primary key (productCode)
    );
    """

    print('DDL statement:')
    print(q)
    print()

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res0 = cur.execute(q0)
    res = cur.execute(q)

    return res


def schema_operation_4():
    """
    This is an example of what your answers should look like.
    :param country:
    :return:
    """
    res = None

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("admin", "7Senses_kiki", "tutorialdb.cbezzskgwcl3.us-east-2.rds.amazonaws.com")

    q0 = """
    drop table if exists classicmodels_star.fact;
    """

    q = """
    create table classicmodels_star.fact
    (
        customerNumber int not null,
        productCode varchar(15) not null,
        orderDate date not null,
        quantityOrdered int null,
        priceEach decimal(10,2) null,
        constraint fact_pk
            primary key (customerNumber, productCode, orderDate),
        constraint fact_location_dimension_fk
            foreign key (customerNumber) references location_dimension (customerNumber),
        constraint fact_product_dimension_fk
            foreign key (productCode) references product_dimension (productCode),
        constraint fact_date_dimension_fk
            foreign key (orderDate) references date_dimension (orderDate)
    );
    """

    print('DDL statement:')
    print(q)
    print()

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res0 = cur.execute(q0)
    res = cur.execute(q)

    return res


def data_transformation_1():

    mysql_check.set_connect_info("admin", "7Senses_kiki", "tutorialdb.cbezzskgwcl3.us-east-2.rds.amazonaws.com")

    q1 = """
    insert into classicmodels_star.date_dimension
    select distinct orderDate, month(orderDate),
            quarter(orderDate), year(orderDate)
    from classicmodels.orders
    """

    q2 = """
    insert into classicmodels_star.location_dimension
    select customerNumber, city, country,
    case country 
            when 'USA' then 'NA'
            when 'Canada' then 'NA'
            when 'Philippines' then 'AP'
            when 'Hong Kong' then 'AP'
            when 'Singapore' then 'AP'
            when 'Japan' then 'AP'
            when 'Australia' then 'AP'
            when 'New Zealand' then 'AP'
            else 'EMEA'
    end as region
    from classicmodels.customers
    """

    print('Data Transformation for table date_dimension:')
    print(q1)
    print()

    print('Data Transformation for table location_dimension:')
    print(q2)
    print()

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res1 = cur.execute(q1)
    res2 = cur.execute(q2)

    return (res1, res2)


def data_transformation_2():
    
    mysql_check.set_connect_info("admin", "7Senses_kiki", "tutorialdb.cbezzskgwcl3.us-east-2.rds.amazonaws.com")

    q1 = """
    insert into classicmodels_star.product_dimension
    select productCode, productScale, productLine
    from classicmodels.products
    """

    q2 = """
    insert into classicmodels_star.fact
    select customers.customerNumber, products.productCode, 
    orders.orderDate, orderdetails.quantityOrdered, orderdetails.priceEach
    from (((classicmodels.customers join classicmodels.orders
        on customers.customerNumber = orders.customerNumber)
        join classicmodels.orderdetails
        on orders.orderNumber = orderdetails.orderNumber)
        join classicmodels.products
        on orderdetails.productCode = products.productCode)
    """

    print('Data Transformation for table product_dimension:')
    print(q1)
    print()

    print('Data Transformation for table fact:')
    print(q2)
    print()

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res1 = cur.execute(q1)
    res2 = cur.execute(q2)

    return (res1, res2)


def sales_by_year_region():
    
    mysql_check.set_connect_info("admin", "7Senses_kiki", "tutorialdb.cbezzskgwcl3.us-east-2.rds.amazonaws.com")

    q = """
    select year, region, sum(quantityOrdered * priceEach) as order_total_value
    from ((classicmodels_star.date_dimension join classicmodels_star.fact
    on date_dimension.orderDate = fact.orderDate) join classicmodels_star.location_dimension
    on fact.customerNumber = location_dimension.customerNumber)
    group by year, region
    """

    print('Query:')
    print(q)
    print()

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)

    result = cur.fetchall()
    result = pd.DataFrame(result)

    return result


def sales_by_quarter_year_county_region():
    
    mysql_check.set_connect_info("admin", "7Senses_kiki", "tutorialdb.cbezzskgwcl3.us-east-2.rds.amazonaws.com")

    q = """
    select year, quarter, region, country, sum(quantityOrdered * priceEach) as order_total_value
    from ((classicmodels_star.date_dimension join classicmodels_star.fact
    on date_dimension.orderDate = fact.orderDate) join classicmodels_star.location_dimension
    on fact.customerNumber = location_dimension.customerNumber)
    group by year, quarter, region, country
    """

    print('Query:')
    print(q)
    print()

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)

    result = cur.fetchall()
    result = pd.DataFrame(result)

    return result


def sales_by_product_line_scale_year():
    
    mysql_check.set_connect_info("admin", "7Senses_kiki", "tutorialdb.cbezzskgwcl3.us-east-2.rds.amazonaws.com")

    q = """
    select productLine, productScale, year, sum(quantityOrdered * priceEach) as order_total_value
    from ((classicmodels_star.date_dimension join classicmodels_star.fact
    on date_dimension.orderDate = fact.orderDate) join classicmodels_star.product_dimension
    on fact.productCode = product_dimension.productCode)
    group by productLine, productScale, year
    """

    print('Query:')
    print(q)
    print()

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)

    result = cur.fetchall()
    result = pd.DataFrame(result)

    return result
