from neo4j import GraphDatabase


class DbWrapper(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def print_greeting(self, message):
        with self._driver.session() as session:
            greeting = session.write_transaction(self._create_and_return_greeting, message)
            print(greeting)

    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run("CREATE (a:Greeting) "
                        "SET a.message = $message "
                        "RETURN a.message + ', from node ' + id(a)", message=message)
        return result.single()[0]

    def run_query(self, query, **kwargs):
        def _query_function(tx, **kwargs2):
            result = tx.run(query, **kwargs2)
            return result.data()

        with self._driver.session() as session:
            result = session.write_transaction(_query_function, **kwargs)
            print(result)
            return result

    def query_movies_by_year(self, year):
        return self.run_query("MATCH (a:Movie) WHERE a.released = $year RETURN a.title", year=year)

    def query_all_directors(self):
        return self.run_query("MATCH (a:Person) -[:DIRECTED]-> (Movie) WHERE (a:Person) -[:ACTED_IN]-> "
                              "(Movie) RETURN a, COLLECT(DISTINCT Movie)")

    def query_all_actors_and_directors(self):
        return self.run_query("MATCH (a:Person) -[:DIRECTED]-> (c:Movie) RETURN a")

    # this one doesn't work - does not have a return type
    def delete_all_nodes_and_relationships(self):
        self.run_query("MATCH (n) DETACH DELETE n")

    def find_all_actors_in_film(self, name):
        return self.run_query("MATCH (a:Person) -[:ACTED_IN]-> (c:Movie) WHERE c.title = $name RETURN a", name=name)

    def load_artist_data(self):
        return self.run_query("LOAD CSV FROM 'https://neo4j.com/docs/cypher-manual/4.1/csv/artists.csv' AS line "
                              "CREATE (:Artist { name: line[1], year: toInteger(line[2])})")

    # not in use
    def load_dummy_data(self):
        return self.run_query("LOAD CSV FROM 'file:///home/beth/PycharmProjects/GraphDatabases/neo4jProject/DummyData"
                              ".tsv' AS line "
                              "CREATE (:Artist { name: line[1], year: toInteger(line[2])})")

    def check_for_presence_in_database(self, value):
        return self.run_query("MATCH (a:Paper) WHERE a.id = $value RETURN a", value=value)
        # run query to return value - if not in database null will be returned

    def add_relationship_to_database(self, value1, value2):
        return self.run_query("MATCH (a:Paper) WHERE a.id = $value1 MATCH (b:Paper) WHERE b.id = $value2 CREATE (a)-["
                              ":REFERENCES]->(b)", value1=value1, value2=value2)

    def add_new_node_to_database(self, value):
        return self.run_query("CREATE (a:Paper { id: $value})", value=value)
        # CREATE (a:Paper { id: 2})


db = DbWrapper("neo4j://localhost:7687", "neo4j", "neo4j")
# example.load_dummy_data()

db.delete_all_nodes_and_relationships()
with open('/home/beth/share/share/PaperReferences.txt', 'r') as fp:
    MAX_LINES = 1000
    for linenum in range(MAX_LINES):
        line = fp.readline()
        if line == "":
            break
        else:
            val1, val2 = map(int, line.split('\t'))
            print(val1)
            print(val2)
            res = db.check_for_presence_in_database(val1)
            if len(res) == 0:
                db.add_new_node_to_database(val1)
            res = db.check_for_presence_in_database(val2)
            if len(res) == 0:
                db.add_new_node_to_database(val2)
            db.add_relationship_to_database(val1, val2)

# result = db.query_movies_by_year(1992)
# db.delete_all_nodes_and_relationships()
# result = db.query_all_actors_and_directors()
# 'file:///home/beth/PycharmProjects/GraphDatabases/neo4jProject/DummyData.tsv'
# fname = '/home/beth/share/share/PaperReferences.txt'
# with open(fname, 'r') as fp:
#   for n in range(10):
#      print(fp.readline().strip())


db.close()
