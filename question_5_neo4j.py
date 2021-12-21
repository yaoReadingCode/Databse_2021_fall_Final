import py2neo
import pandas as pd
import neo4j_check


def directed_tom_hanks():

    neo4j_check.set_neo4j_connect_info("neo4j", "7Senses_kiki")

    q = """
    match (n)-[:ACTED_IN]->(m)<-[:DIRECTED]-(n2) where n.name="Tom Hanks" return n.name, m.title, n2.name
    """
    gr = neo4j_check.get_graph()

    result = gr.run(q)
    result = pd.DataFrame(result)

    return result


def directed_themselves():

    neo4j_check.set_neo4j_connect_info("neo4j", "7Senses_kiki")

    q = """
    match (n)-[:ACTED_IN]->(m)<-[:DIRECTED]-(n2) where n.name=n2.name return n.name, m.title, n2.name
    """
    gr = neo4j_check.get_graph()

    result = gr.run(q)
    result = pd.DataFrame(result, columns=['name_1', 'title', 'name_2'])

    return result


def both_reviewed(person_1_name, person_2_name):

    neo4j_check.set_neo4j_connect_info("neo4j", "7Senses_kiki")

    q = """
    match (n)-[:REVIEWED]->(m)<-[:REVIEWED]-(n2) where n.name=$x and n2.name=$y return n.name, m.title, n2.name
    """
    gr = neo4j_check.get_graph()

    result = gr.run(q, {'x':person_1_name, 'y':person_2_name})
    result = pd.DataFrame(result, columns=['name_1', 'title', 'name_2'])

    return result


