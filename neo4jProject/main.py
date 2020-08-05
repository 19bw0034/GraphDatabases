from neo4j import GraphDatabase


class HelloWorldExample(object):

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
        return self.run("MATCH (a:Person) -[:DIRECTED]-> (c:Movie) RETURN a")

    # this one doesn't work - does not have a return type
    def delete_all_nodes_and_relationships(self):
        return self.run("MATCH (n) DETACH DELETE n")

    def find_all_actors_in_film(self, name):
        return self.run("MATCH (a:Person) -[:ACTED_IN]-> (c:Movie) WHERE c.title = $name RETURN a", name=name)


example = HelloWorldExample("neo4j://localhost:7687", "neo4j", "neo4j")
result = example.query_movies_by_year(1992)
