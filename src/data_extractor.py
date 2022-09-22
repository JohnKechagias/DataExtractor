from functools import partial
import itertools
from typing import List, Set

from multiprocessing.pool import ThreadPool
from multiprocessing import Queue
from constants import *



class DataExtractor:
    def __init__(
        self,
        table_queue: List[str],
        items_chunk_size: int = 500,
        seach_chunk_size: int = 65000,
    ) -> None:
        self.table_queue = table_queue
        self.items_chunk_size = items_chunk_size
        self.seach_chunk_size = seach_chunk_size

    def extract(
        self,
        input_file: Path,
        search_file: Path
    ):
        lookup_table = DataExtractor.get_lookup_table(input_file)

        pool = ThreadPool()

        for index, table_values in enumerate(self.table_queue):
            line_list = []
            filepath = data_folder / table_values[0]
            size = filepath.stat().st_size
            # Split large table files into smaller ones
            filelist = DataExtractor.split_file(filepath, index)

            func = partial(
                DataExtractor.extract_lines,
                line_list,
                lookup_table,
                table_values[1],
                table_values[2]
            )
            split_lookup_table = pool.map_async(func, filelist)

            lookup_table = list(itertools.chain.from_iterable(split_lookup_table.get()))

            with open(output_folder / table_values[0], 'w') as table:
                table.writelines(line_list)

        pool.close()
        pool.join()

    @staticmethod
    def extract_lines(
        queue: List[str],
        lookup_table: Set,
        col_index: int,
        col_to_add_index: int,
        filepath: Path,
    ) -> List[str]:
        new_lookup_table = []
        with open(filepath, 'r', encoding='ascii') as file:
            for line in file.readlines():
                split_line = line.split(',')
                if split_line[col_index] in lookup_table:
                    queue.append(line)
                    new_lookup_table.append(split_line[col_to_add_index])

        return new_lookup_table

    @staticmethod
    def get_lookup_table(filepath: Path) -> Set:
        users = set()
        with open(filepath, 'r', encoding='ascii') as items_f:
            users = {i.replace('\n', '') for i in items_f}
        return users

    @staticmethod
    def split_file(
        filepath: Path,
        itteration_num = 0,
        chunk_size: int = 1000000
    ) -> List[str]:
        filelist = []
        with open(filepath, encoding='ascii') as file:
            num_of_files = 0
            while True:
                buffer = file.readlines(80 * chunk_size)
                if not buffer:
                    break
                filename = f'out_file{itteration_num}{num_of_files}.txt'
                out_file = open(filename, 'wt')
                out_file.writelines(buffer)
                out_file.close()
                num_of_files += 1
                filelist.append(filename)

        return filelist

    @staticmethod
    def writter(filepath: Path = 'x.csv', queue: Queue = None) -> None:
        with open(filepath, 'w', encoding='ascii') as file:
            while not queue.empty():
                try:
                    line = queue.get()
                    file.write(line)
                except Exception as qe:
                        print('Empty Queue or dead process')
