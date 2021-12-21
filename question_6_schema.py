import mysql_check


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
    drop table if exists RACI.Person;
    """

    q = """
    create table RACI.Person
    (
        UNI varchar(6) not null,
        last_name varchar(128) null,
        first_name varchar(128) null,
        email varchar(128) null,
        constraint Person_pk
            primary key (UNI)
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
    drop table if exists RACI.Project;
    """

    q = """
    create table RACI.Project
    (
        project_id varchar(128) not null,
        project_name varchar(128) null,
        start_date date null,
        end_date date null,
        account_person varchar(6) null,
        constraint Project_pk
            primary key (project_id),
        constraint Project_Person_UNI_fk
            foreign key (account_person) references Person (UNI)
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
    drop table if exists RACI.Role;
    """

    q = """
    create table RACI.Role
    (
        project_id varchar(128) not null,
        UNI varchar(6) not null,
        type varchar(16) null,
        constraint Role_pk
            primary key (UNI, project_id),
        constraint Role_Person_UNI_fk
            foreign key (UNI) references Person (UNI),
        constraint Role_Project_project_id_fk
            foreign key (project_id) references Project (project_id)
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