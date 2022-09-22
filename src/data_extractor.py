import os
from functools import partial
import itertools
from typing import List, Set
from multiprocessing.pool import ThreadPool
from constants import *



class DataExtractor:
    def __init__(
        self,
        tables_metadata: List[List],
        temp_folder: Path = None
    ) -> None:
        """Constructor.

        Args:
            tables_metadata:
                List of metadata for each table.
            temp_folder:
                Folder that the temp files will be generated to.
        """
        if temp_folder is None:
            temp_folder = Path('.').parent
        self.temp_folder = temp_folder
        self.tables_metadata = tables_metadata

    def extract(
        self,
        input_file: Path,
        tables_folder: Path = None
    ) -> None:
        """ Extracts data from the csv files based on the input file.

        Args:
            input_file: The input file that contains all the data
                on which the matching process is based on.
            tables_folder: Path to the table's files.
                Defaults to the data folder.
        """
        if tables_folder is None:
            tables_folder = data_folder

        lookup_table = DataExtractor.get_lookup_table(input_file)
        pool = ThreadPool()

        for index, table_metadata in enumerate(self.tables_metadata):
            # List that stores matched data rows from the table
            # files so that they can be written to the new files.
            # The "list.append" operation is atomic.
            line_list = []
            filepath = data_folder / table_metadata[0]
            # Split large files into smaller ones.
            filelist = DataExtractor.split_file(filepath, self.temp_folder, index)

            func = partial(
                DataExtractor.extract_lines,
                line_list,
                lookup_table,
                table_metadata[1],
                table_metadata[2]
            )
            # Create threads that search a smaller file, and based on the
            # findings 1. create a new lookup table that's gonna be used for
            # the next table data matching and 2. add the matched row to the
            # line_list so the can be written to a new file.
            split_lookup_table = pool.map_async(func, filelist)

            lookup_table = list(itertools.chain.from_iterable(split_lookup_table.get()))

            with open(output_folder / table_metadata[0], 'w') as table:
                table.writelines(line_list)

        # Delete temp files that were generated from "split_file".
        self.delete_temp_files()

        pool.close()
        pool.join()

    @staticmethod
    def extract_lines(
        line_list: List[str],
        lookup_table: Set,
        col_index: int,
        ltable_index: int,
        filepath: Path,
    ) -> List[str]:
        """ Extracts matching data from a file.

        Args:
            line_list: List to append the matching lines that we want
                to write to the smaller file.
            lookup_table: A Set that includes all the field values that
                we are looking for.
            col_index: Index that determines the col to search for a field
                that matching with something in the lookup table.
            ltable_index: Index that determines the new col that the matching
                items of which are going to constitute the new lookup table.
            filepath: The filepath of the file to seach into and extract data.
        """
        new_lookup_table = []
        with open(filepath, 'r', encoding='ascii') as file:
            for line in file.readlines():
                split_line = line.split(',')
                if split_line[col_index] in lookup_table:
                    line_list.append(line)
                    new_lookup_table.append(split_line[ltable_index])

        return new_lookup_table

    @staticmethod
    def get_lookup_table(filepath: Path) -> Set:
        """ Returns a lookup table that is based on the
        elements of the input file.

        Args:
            filepath: Path to the input file.
        """
        users = set()
        with open(filepath, 'r', encoding='ascii') as items_f:
            users = {i.replace('\n', '') for i in items_f}
        return users

    @staticmethod
    def split_file(
        filepath: Path,
        output_folder: Path,
        file_num = 0,
        chunk_size: int = 1000000,
        median_row_size: int = 80
    ) -> List[str]:
        """ Splits a file into smaller ones.

        Args:
            filepath: Path to the file.
            output_folder: Folder to output the smaller files.
            file_num: Number that is used to seperate
                temp files of different larger files.
            chunk_size: Number that specifies the max size (in bytes)
                of the smaller files. Defaults to 1000000.
            median_row_size: Median size of each row of the large file
                (in bytes).
        """
        filelist = []
        with open(filepath, encoding='ascii') as file:
            num_of_files = 0
            while True:
                buffer = file.readlines(median_row_size * chunk_size)
                if not buffer:
                    break
                filename = f'out_file{file_num}{num_of_files}.temp'
                out_file = open(output_folder / filename, 'wt')
                out_file.writelines(buffer)
                out_file.close()
                num_of_files += 1
                filelist.append(filename)

        return filelist

    def delete_temp_files(self):
        """ Deletes temp files from the default `temp_folder`. """
        for file in self.temp_folder.iterdir():
            if file.name.endswith('.temp'):
                os.remove(file)
