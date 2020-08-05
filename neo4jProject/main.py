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

    @staticmethod
    def _query_movies_by_year(tx, year):
        result = tx.run("MATCH (a:Movie) WHERE a.released = $year RETURN a.title", year=year)
        return result.data()


example = HelloWorldExample("neo4j://localhost:7687", "neo4j", "neo4j")
example.run_query("MATCH (a:Movie) WHERE a.released = $year RETURN a.title", year=1992)


#"MATCH (a:Movie) WHERE a.released = $year RETURN a.title", year=year