from pathlib import Path
from timeit import timeit
import sys
from database import Database
from data_extractor import DataExtractor
from functools import partial
from constants import *
from table_metadata import create_table_metadata



if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise ValueError('Please provide a users file.')

    filepath = Path(sys.argv[1])
    Database.connect('database.db')

    tables_metadata = create_table_metadata(data_folder)
    data_extractor = DataExtractor(tables_metadata)

    s = partial(data_extractor.extract, filepath, Path('data') / 'CUSTOMER.csv')
    print(timeit(s, number=1))
