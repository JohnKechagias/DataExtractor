import sqlite3
from pathlib import Path
from sqlite3 import Error
from typing import Any, List, Optional, Union
from constants import *



sql_command = str

create_customers_table = """
    CREATE TABLE IF NOT EXISTS CUSTOMERS(
        CUSTOMER_CODE INT PRIMARY KEY,
        FIRSTNAME TEXT NOT NULL,
        LASTNAME TEXT NOT NULL
    );
"""

create_invoices_table = """
    CREATE TABLE IF NOT EXISTS INVOICES(
        CUSTOMER_CODE INT NOT NULL,
        INVOICE_CODE INT PRIMARY KEY,
        AMOUNT FLOAT NOT NULL,
        DATE TEXT NOT NULL,
        FOREIGN KEY (CUSTOMER_CODE) REFERENCES CUSTOMERS(CUSTOMER_CODE))
"""

create_invoice_items_table = """
    CREATE TABLE IF NOT EXISTS INVOICE_ITEMS(
        INVOICE_CODE INT NOT NULL,
        ITEM_CODE TEXT PRIMARY KEY,
        AMOUNT FLOAT NOT NULL,
        QUANTITY INTEGER NOT NULL,
        FOREIGN KEY (INVOICE_CODE) REFERENCES INVOICES(INVOICE_CODE))
"""

create_tables_commands = (
    create_customers_table,
    create_invoices_table,
    create_invoice_items_table
)

class Database:
    _conn = None

    @classmethod
    def connect(cls, db_file: Path) -> sqlite3.Connection:
        """ Creates a database connection to an SQLite database. """
        conn = None
        try:
            conn = sqlite3.connect(db_file)
        except Error as e:
            print(e)

        cls._conn = conn
        cls.execute('PRAGMA foreign_keys = on')

    @classmethod
    def execute(
        cls,
        command: sql_command,
        parameters: Optional[Union[Any, List[Any]]] = None
    ):
        """ Executes an SQL command.

        Args:
            conn: Connection object.
            command: A sql command.
            parameters: Command parameters.
        """
        try:
            cursor = cls._conn.cursor()
            if parameters is None:
                cursor.execute(command)
            else:
                cursor.execute(command, parameters)
        except Error as e:
            print(e)

        return cursor

    @classmethod
    def _create_tables(cls):
        """ Creates default db tables. """
        for command in create_tables_commands:
            cls.execute(command)

    @classmethod
    def export_tables_to_csv(cls):
        """ Exports table data to csv files. A csv file is created
        for each table. Each csv has the following specifications:

        - ASCII file (UNIX format).
        - First line of the file is the heading.
        - Each field are separated by comma character “,”.
        - All Field are double quoted regardless of data type (e.g. “Olivier”).

        Csvs are saved to the data folder.
        """
        tables = ('CUSTOMERS', 'INVOICES', 'INVOICE_ITEMS')
        cursor = cls._conn.cursor()

        for table in tables:
            cursor.execute(f'SELECT * FROM {table}')
            with open(data_folder / f'{table}.csv', 'w+', encoding='ascii') as file:

                # Format and write headings to the file
                headings = [f'"{i[0]}"' for i in cursor.description]
                file.write(','.join(headings) + '\n')

                # Format data as to match the original data description
                for entry in list(cursor):
                    entry = list(map(str, list(entry)))

                    if table == 'CUSTOMERS':
                        entry[0] = 'CUST' + '0' * (8 - len(entry[0])) + entry[0]
                    elif table == 'INVOICES':
                        entry[0] = 'CUST' + '0' * (8 - len(entry[0])) + entry[0]
                        entry[1] = 'INVO' + '0' * (8 - len(entry[1])) + entry[1]
                    elif table == 'INVOICE_ITEMS':
                        entry[0] = 'INVO' + '0' * (8 - len(entry[0])) + entry[0]

                    # Format and write entries to the file
                    for index, _ in enumerate(entry):
                        entry[index] = f'"{entry[index]}"'
                    file.write(','.join(entry) + '\n')
