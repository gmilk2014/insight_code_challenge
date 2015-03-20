import os
import shutil
import collections
import string
import multiproc_MapReduce

            
def mapper(filename):
    """
    Read a file and store <word 1> pairs on disk
    at the directory mapper_intermediate_result
    return the file name of intermedialte reult
    
    Args:
      filename (str): input files name

    Returns:
      the file name of intermedialte reult
    """

    tmp_dir = 'mapper_intermediate_result'
    try:
        os.mkdir(tmp_dir) # temporary directory to hold splits of input
    except OSError:
        pass

    with open(filename, 'r') as f:
        filename = os.path.basename(filename)
        with open(os.path.join(tmp_dir, filename), 'w') as f_output:
            for line in f:
                for word in line.strip().split():
                    clean_up_word = word.strip(string.punctuation).lower()
                    # the following if-else statement is to ensure that
                    # if the word contains only punctuations, we will
                    # leave it as it is.
                    if clean_up_word == '':
                        f_output.write(word + ' 1\n')
                    else:
                        f_output.write(clean_up_word + ' 1\n')
    return os.path.join(tmp_dir, filename)
    
def combiner(mapper_results):
    """
    Read the intermediate result from mapper and combine the result
    with what reducer would do. Overwrite the original intermediate
    result.
    
    Args:
      mapper_results (list of str): list of mapper result file names

    Returns:
      no return value
    """
    
    for filename in mapper_results:
        combine = collections.defaultdict(int)
        with open(filename, 'r') as f:
            for line in f:
                try:
                    key, value = line.split()
                except ValueError:
                    print 'line = ' + line
                combine[key] += 1
        with open(filename, 'w') as f:
            for key in combine:
                f.write(key + ' ' + str(combine[key]) + '\n')

def reducer(partition):
    """
    Convert the partitioned data into a tuple (word, frequency)
    
    Args:
      partition (tuple): tuple of the form (word, [value1, value2,...])

    Returns:
      no return value
    """
    
    word, frequency = partition
    return (word, sum(frequency)) 
