import os
import shutil
import collections
import itertools
import heapq
from operator import itemgetter
import multiprocessing

class Counter(object):
    """a multiprocessing safe counter

    Attributes:
      val (multiprocessing.Value obj): A shared value between processes
      lock (lock obj): A non-recursive lock object
      
    """
    
    def __init__(self, val=0):
        """    
        Args:
          val (int, optional): vale that used to initialize
            multiprocessing.Value obj. Default to 0.
          
        """
        self.val = multiprocessing.Value('i', val)
        self.lock = multiprocessing.Lock()

    def getValue(self):
        """Get the counter value
        Args: No args

        Returns:
          counter value

        """
        with self.lock:
            return self.val.value
            
    def getValue_and_increment(self):
        """Get the counter value and then increment it by 1
        Args: No args

        Returns:
          counter value

        """
        with self.lock:
            val = self.val.value
            self.val.value += 1
            return val           

class map_worker(multiprocessing.Process):
    """Map workers class that execute mapper function

    Attributes:
      tasks (multiprocessing.JoinableQueue obj): A queue that contains
        the file names of splits.
      results (multiprocessing.JoinableQueue obj): A buffer that store
        the intermediate result of the mapper function
      mappers (function): the user-defined map function
      
    """  
    def __init__(self, tasks, results, mapper):
        """    
        Args:
          tasks (multiprocessing.JoinableQueue obj): A queue that contains
            the file names of splits.
          results (multiprocessing.JoinableQueue obj): A buffer that store
            the intermediate result of the mapper function
          mappers (function): the user-defined map function
          
        """
        multiprocessing.Process.__init__(self)
        self.tasks = tasks
        self.results = results
        self.mapper = mapper

    def run(self):
        """Initiate the Map worker to perform mapper function
          to all words in the splits. Put the results into
          the buffer.
        
        Args: no args
        Returns: no return
        
        """
        while True:
            filename = self.tasks.get()
            if filename is None:
                # sentinel value -> quit
                self.results.put(None)
                self.tasks.task_done()
                return True           
            with open(filename, 'r') as f:
                for line in f:
                    for word in line.strip().split():
                        self.results.put(self.mapper(word))
            self.tasks.task_done()
            
class spill_worker(multiprocessing.Process):
    """Spill workers class that sorts and partitions the intermediate
      results of mapper.
    
    Class variable:
      bufferSize (int): the maximum of the buffersize is
        harded coded to 32767.
    
    Attributes:
      tasks (multiprocessing.JoinableQueue obj): A buffer that contains
        the intermediate result of mapper function
      results (multiprocessing.Queue obj): A queue that stores spill
        file names
      ID (Counter obj): A multiprocessing safe counter that is used to
        make a unique file name for each spill file.
      combiner (function): An optional user-defined function
        that performs partial merging on the intermediate output
        of mapper.
      buffer (collections.defaultdict obj): the buffer that store the
        key value pairs extracted from the tasks buffer. The buffer
        contents are flushed to disk when it's "almost" full.
      percent (float): The percentage that is used to define
        the threshold for how full the buffer should be before
        flushing the buffer contents into disk. Default to 80%.
      count (int): the number of key value pairs that are extracted
        from the task buffer. Reset to 0 when the buffer contents
        are flushed to disk.
      
      
    """ 
    
    bufferSize = 32767
    
    def __init__(self, tasks, results, ID, combiner=None, percent=0.8):
        """
        args:
          tasks (multiprocessing.JoinableQueue obj): A buffer that contains
            the intermediate result of mapper function
          results (multiprocessing.Queue obj): A queue that stores spill
            file names
          ID (Counter obj): A multiprocessing safe counter that is used to
            make a unique file name for each spill file.
          combiner (function, optional): An optional user-defined function
            that performs partial merging on the intermediate output
            of mapper.
          percent (float, optional): The percentage that is used to define
            the threshold for how full the buffer should be before flushing
            the buffer contents into disk. Default to 80%.          
      
        """
        
        multiprocessing.Process.__init__(self)
        self.tasks = tasks
        self.results = results
        self.ID = ID
        self.combiner = combiner
        self.buffer = collections.defaultdict(list)
        self.percent=percent      
        self.count = 0
        
    
    def flush_buffer(self):
        """Flush the buffer contents to disk.
          Save to mapper_intermediate_result/spill_<ID>
        
        Args: no args
        Returns: no return
        
        """
        # if combiner is given, run it
        if self.combiner != None:
            for key in self.buffer:                    
                key, value = self.combiner(key, self.buffer[key])
                self.buffer[key] = [value]
        key_values_list = self.buffer.items()
        key_values_list.sort(key=itemgetter(0))
        spill_file = os.path.join(
            'mapper_intermediate_result',
            'spill_{0}'.format(self.ID.getValue_and_increment()))            
        with open(spill_file, 'w') as f:
            for key, values in key_values_list:
                for v in values:
                        f.write(key + ' ' + str(v) + '\n')                       
            self.results.put(spill_file)
                    
    def run(self):
        """Initiate the Spill worker to perform sorting and
          partitioning on the mapper's output.
        
        Args: no args
        Returns: no return
        
        """
        while True:
            key_value = self.tasks.get()
            if key_value is None:
                # sentinel value -> quit
                if len(self.buffer) > 0:
                    self.flush_buffer()
                self.results.put(None)
                self.tasks.task_done()
                return True
            self.buffer[key_value[0]].append(int(key_value[1]))
            self.count += 1                                                
            # if buffer is almost full, flush the content into disk
            if self.count >= int(self.bufferSize * self.percent):
                self.flush_buffer()
                # reset count and buffer
                self.count = 0
                self.buffer.clear()
            self.tasks.task_done()            
            
class MapReduce(object):
    """a simplified mapreduce implementation with multiprocessing

    Attributes:
      mapper (function): A user-defined function. See __init__ for
        more information.
      reducer (function): A user-defined function. See __init__ for
        more information.
      combiner (function): A user-defined function. See __init__ for
        more information.
      num_workers (int): The number of worker processes
      
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
          num_workers (int, optional): The number of worker processes.
            Default to the number of CPUs in the system.
            If multiprocessing.cpu_count() is not implemented, default
            to 4.

          
        """
        self.mapper = mapper
        self.reducer = reducer
        self.combiner = combiner
        if num_workers is None:
            try:
                self.num_workers = multiprocessing.cpu_count()
            except NotImplementedError:
                self.num_workers = 4
        else:
            self.num_workers = num_workers
            
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
    
    def run(self, input_dir, lines_per_split=2000):
        """initiate the MapReduce Job.
          Write result to wc_output/wc_result.txt
        
        Args:
          input_dir (str): the directory that contains the data to
            be processed
          lines_per_split (int, optional): number of lines in
            each split. Default to 2000 lines per split.
            
        Returns:
          True if successful, False otherwise.
        
        """
        
        # get files in the input_dir
        input_files = self.get_input_files(input_dir)
        
        # if no files in input_dir, quit the job and return false
        if len(input_files) == 0:
            return False
                   
        # create temporary directory to hold splits of input
        tmp_dir = 'inputSplits'
        try:
            os.mkdir(tmp_dir) 
        except OSError:
            pass
        
        # setup queue with file names and a output queue
        tasks = multiprocessing.JoinableQueue()
        map_results = [multiprocessing.JoinableQueue() for i in xrange(self.num_workers)]
        spill_results = [multiprocessing.Queue() for i in xrange(self.num_workers)]

        # splitting the input files into smaller files on disk
        for inputfile in input_files:
            with open(os.path.join(input_dir, inputfile), 'r') as f:
                for i, split in enumerate(
                    self.inputSplits(lines_per_split, f, fillvalue=''), 1):
                    tmp_file = os.path.join(
                        tmp_dir, inputfile+'_tmp_{0}'.format(i))
                    tasks.put(tmp_file)  # Enqueue tasks
                    with open(tmp_file, 'w') as f_output:
                        f_output.writelines(split)
        
        # Add a sentinel for each map_worker
        for i in xrange(self.num_workers):
            tasks.put(None)
                                   
        # create map workers
        map_workers = [map_worker(tasks, map_results[i], self.mapper)
                       for i in xrange(self.num_workers)]
        
        # create spill workers to handle intermediate map results
        ID = Counter(0)
        spill_workers = [spill_worker(map_results[i],
                         spill_results[i], ID, self.combiner, percent=0.8)
                         for i in xrange(self.num_workers)]
        
        # create a directory to hold mappers result
        try:
            os.mkdir('mapper_intermediate_result')
        except OSError:
            pass
        
        # start workers               
        for worker in map_workers:
            worker.start()
        for worker in spill_workers:
            worker.start()
        
        # wait for tasks completion
        tasks.join()
        for q in map_results:
            q.join()
        
        # create directory wc_output to store output
        try:
            os.mkdir('wc_output')
        except OSError:
            pass
  
        with open(os.path.join('wc_output', 'wc_result.txt'), 'w') as f:    
            # merge the spill files into one big sorted file
            spill_files = []
            for q in spill_results:
                while True:
                    spill_file = q.get()
                    if spill_file is None:
                        # sentinel value -> quit
                        break          
                    spill_files.append(open(spill_file, 'r'))
            key = None        
            values = []        
            for line in heapq.merge(*spill_files):
                key_value = line.split()
                if key is None:
                    key = key_value[0]
                elif key != key_value[0]:
                    # see a new key, apply reducer and 
                    # flush the result to disk
                    reducer_output = self.reducer(key, values)
                    f.write(reducer_output[0] +
                            ' ' + str(reducer_output[1]) + '\n')
                    key = key_value[0]                            
                    values = []   # clear the list       
                values.append(int(key_value[1]))
                
            # don't forget to flush the last one
            reducer_output = self.reducer(key, values)
            f.write(reducer_output[0] +
                    ' ' + str(reducer_output[1]) + '\n')
                    
            # close all files even though they will be removed shortly
            for file_handler in spill_files:
                file_handler.close()
            
        # remove the temporary directory
        shutil.rmtree(tmp_dir, ignore_errors=True)
        shutil.rmtree('mapper_intermediate_result', ignore_errors=True)        
        return True
