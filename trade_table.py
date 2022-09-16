
from logging import Filter
from mimetypes import init


class Query ():
    def __init__(self) -> None:
        pass
        
    def create_query(self, table, columns, where):
        pass

class ConcreteQuery(Query):
    def __init__(self) -> None:
        super().__init__()

    def create_query(self, table, columns, where):
        self.table = table
        self.columns = columns
        self.where = where
        
        self.query = "SELECT "
        for i  in range(len(columns) - 1):
            self.query += self.columns[i]
            self.query += ", "
        
        self.query += self.columns[-1] + " "

        self.query += "FROM " + self.table + " "
        self.query += "WHERE " + self.where + " = ? "

        return self.query

class FilterQuery(Query):
    def __init__(self, concrete) -> None:
        self.concrete = concrete
        super().__init__()

class FilterBySymbol(FilterQuery):
    def __init__(self, concrete) -> None:
        super().__init__(concrete)
    def create_query(self, table, columns, where):
        return self.concrete.create_query(table, columns, where) + "and symbol = ? "

class FilterByType(FilterQuery):
    def __init__(self, concrete) -> None:
        super().__init__(concrete)
    def create_query(self, table, columns, where):
        return self.concrete.create_query(table, columns, where) + "and type = ? "


class FilterByDateRage(FilterQuery):
    def __init__(self, concrete) -> None:
        super().__init__(concrete)
    def create_query(self, table, columns, where):
        return self.concrete.create_query(table, columns, where) + "and trade_id > maxTimeuuid( ? ) and trade_id < minTimeuuid ( ? ) "

def create_query(query):
    return FilterQuery(query)

def filter_by_symbol(query):
    return FilterBySymbol(query)

def filter_by_type(query):
    return FilterByType(query)

def filter_by_date(query):
    return FilterByDateRage(query)

my_query = ConcreteQuery()
my_query = FilterByType(my_query)
my_query = FilterBySymbol(my_query)
my_query = FilterByDateRage(my_query)


print(my_query.create_query("trades_by_a_td", ["toDate(trade_id)", "trade_id", "type",
                            "symbol", "shares", "price", "amount"], "account"))

        