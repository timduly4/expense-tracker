import sqlalchemy
from sqlalchemy import (MetaData, Table, Column, Integer, String, Date, Float)
import getpass
from datetime import datetime
from ipdb import set_trace as db


class DB_expenses(object):

    def __init__(self, db_name):
        """
        Initialize database connection and table
        """
        super(DB_expenses, self).__init__()
        user = getpass.getuser()
        db_str = "postgres://{usr}:{usr}@localhost".format(usr=user)
        my_db = db_str + "/" + db_name
        self.engine = sqlalchemy.create_engine(my_db)

        self.metadata = MetaData()
        self.tb_exp = Table("tb_exp",
                            self.metadata,
                            Column("exp_id", Integer, primary_key=True),
                            Column("purchase_date", Date, nullable=False),
                            Column("description", String, nullable=False),
                            Column("credit_card", String, nullable=False),
                            Column("category", String, nullable=False),
                            Column("amount", Float, nullable=False))
        self.metadata.create_all(self.engine)

    def insert_expense(self, expense_info):
        """
        Insert expense information as a row in the table

        :param expense_info: Row in expense table (dict)
        :return: exp_id: Expense ID (int)
        """
        # Add check for duplicates

        # Insert unique expense information into database
        ins = self.tb_exp.insert().values(purchase_date=expense_info["purchase_date"],
                                          description=expense_info["description"],
                                          credit_card=expense_info["credit_card"],
                                          category=expense_info["category"],
                                          amount=expense_info["amount"])
        connection = self.engine.connect()
        result = connection.execute(ins)

        return result.inserted_primary_key[0]


if __name__ == "__main__":
    db_test = DB_expenses("test")
    row = {"purchase_date": datetime.strptime("2018-08-03", "%Y-%m-%d").date(),
           "description": "Trader Joe's",
           "credit_card": "Chase Sapphire",
           "category": "Grocery",
           "amount": 30.25}
    exp_id = db_test.insert_expense(row)
    print "Inserted exp_id = %s" % exp_id
