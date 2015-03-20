import os
import shutil
import collections
import itertools
import multiprocessing

class MapReduce(object):
    """a simplified mapreduce implementation with multiprocessing

    Attributes:
      mapper (function): A user-defined function. See __init__ for
        more information.
      reducer (function): A user-defined function. See __init__ for
        more information.
      combiner (function): A user-defined function. See __init__ for
        more information.
      pool (obj): a multiprocessing.Pool object that handle the workers
      
    """

    def __init__(self, mapper, reducer, combiner=None, num_workers=None):
        """    
        Args:
          mapper (function): A user-defined function that takes an
            input and produces a set of intermediate key/value tuples
            on disk.
          reducer (function): A user-defined function that accepts
            an intermediate key and a set of values for that key. It
            merges the values to form a final result.
          combiner (function, optional): An optional user-defined
            function that performs partial merging on the intermediate
            output of mapper. Combiner and reducer typically have the
            same implementation.
          num_workers (int): The number of workers in the pool. Default
            is the number of CPUs in the system
          
        """
        self.mapper = mapper
        self.reducer = reducer
        self.combiner = combiner
        self.pool = multiprocessing.Pool(num_workers)
        
    
    def get_input_files(self, dir_path):
        """Get all file names in the given directory.
        Args:
          dir_path (str): the directory path that is being queried

        Returns:
          list of file names

        """

        return [f for f in os.listdir(dir_path)
                if os.path.isfile(os.path.join(dir_path,f))]
                
    def inputSplits(self, n, f, fillvalue=None):
        """a helper function that help to split data into several
           smaller files, each contain n lines of the original input
           (except for the last one.)
           
        Args:
          n (str): number of lines in each split
          f (file): the file handler to be splited
          fillvalue (str, optional): If the iterables (files) are
            of uneven length, missing values are filled-in with
            fillvalue. 

        Returns:
          an iterator that aggregates elements from each of the f

        """
        iter_list = [iter(f)] * n
        return itertools.izip_longest(*iter_list, fillvalue=fillvalue)
    
    def partition(self, mapper_result):
        """Group the intermediate result by each key.

        Args:
          mapper_result (list of str): list of filename of
            mapper's intermediate result

        Returns:
          a list of (key, values) tuples.
        """

        partitions = collections.defaultdict(list)
        for filename in mapper_result:
            with open(filename, 'r') as f:
                for line in f:
                    key_value = line.split()
                    partitions[key_value[0]].append(int(key_value[1]))
        return partitions.items()
    
    def run(self, input_dir, lines_per_split=2000, chunksize=None):
        """initiate the MapReduce Job
        
        Args:
          input_dir (str): the directory that contains the data to
            be processed
          lines_per_split (int, optional): number of lines in
            each split. Default to 2000 lines per split.
          chunksize (int, optional): The number of chunks that inputs
            will be chopped into. Each chunk is submitted to the
            process pool as separate tasks. Default is length of
            iterable divided by 4 * num_cpu and round up to the
            closest interger
        Returns:
          The final output of mapreduce job, a list of (key, values) tuples.
          The final output is also written to disk at wc_output/wc_result.txt
        
        """
          
        input_files = self.get_input_files(input_dir)
        # if no files in input_dir, quit the job and return empty list
        if len(input_files) == 0:
            return []
            
        tmp_dir = 'inputSplits'
        try:
            os.mkdir(tmp_dir) # temporary directory to hold splits of input
        except OSError:
            pass
        tmp_files = []

        # splitting the input files into smaller files on disk
        for inputfile in input_files:
            with open(os.path.join(input_dir, inputfile), 'r') as f:
                for i, split in enumerate(
                    self.inputSplits(lines_per_split, f, fillvalue=''), 1):
                    tmp_file = os.path.join(
                        tmp_dir, inputfile+'_tmp_{0}'.format(i))
                    tmp_files.append(tmp_file)
                    with open(tmp_file, 'w') as f_output:
                        f_output.writelines(split)
        
        mapper_result = self.pool.map(self.mapper, tmp_files, chunksize=chunksize)
        if self.combiner != None:
            self.combiner(mapper_result)
        partitions = self.partition(mapper_result)
        reducer_result = self.pool.map(self.reducer, partitions)
        try:
            os.mkdir('wc_output') # directory to store output
        except OSError:
            pass
        with open(os.path.join('wc_output', 'wc_result.txt'), 'w') as f:
            f.writelines(word + ' ' + str(frequency) + '\n'
                for word, frequency in reducer_result)
        # remove the temporary directory
        shutil.rmtree(tmp_dir, ignore_errors=True)        
        return reducer_result
