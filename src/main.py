import sys
from pathlib import Path
from database import Database
from data_extractor import DataExtractor
from constants import *



# TABLE METADATA
# Table metadata contain data that are needed during the data
# extraction process.
# Each table metadata contain 3 values:
# 1. The table's name.
# 2. The column index that denotes the column that we need to
#    compair with the lookup table (for matching elements).
# 3. The column index that denotes the column that is gonna be
#    used to create the next lookup table.
#
#
# HOW ARE THE METADATA GENERATED
# 1. Create a relationship graph based on the foreign keys that
#    link the tables.
# 2. Cross check the table's headings and create a lookup table
#    for each link. We link each link with a pair of values that
#    denote the common columns. Ex. `(0, 1) : (1, 0)` means that
#    the second col of the first table is the same as the first
#    col of the second table.
# 3. Dfs the graph and find a valid search order.
# 4. Create the metadata based on the dfs result and the links
#    lookup table.
#
#
# DATA EXTRACTION PROCESS
# The data extraction process has 6 distinct steps:
# 1. Determine the order in which we are going to search
#    through the table files ---depends on the foreign keys
#    of each table.
# 2. Create a list that contains the metadata for each table.
# 3. Initialize a DataExtractor with the table's metadata.
# 4. Create a starting lookup table from the input file.
#    The lookup table is a list with elements that we want
#    to extract. For each table we select a column and for
#    each row we select the field of the column and we
#    determine if the field's value is included in the lookup
#    table. If that's the case, we include the row to the
#    extracted data.
# 5. For each table file:
#       Default initialize line_list.
#       Split file into smaller ones.
#       Async search each smaller file.
#          For each row:
#              If the field ---field's index is the 2nd metadata
#              --- is in the lookup table:
#                  1. Add the row to the line_list so that it
#                     can written the the new file.
#                  2. Add the field with index equal to the 3rd
#                     metadata to the new lookup table.
#
#       Replace the old lookup table with the new one.
#       Write the stored data from the line_list to the new
#           file.
# 6. Delete temp files.

if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise ValueError('Please provide an input file.')

    filepath = Path(sys.argv[1])
    # Database.connect('database.db')

    data_extractor = DataExtractor(data_folder)
    data_extractor.extract(filepath)
