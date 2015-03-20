import os
import collections
import itertools
import string

def get_input_files(dir_path):
    """Get all file names in the given directory
        dir_path: the directory path          
    """
    return [os.path.join(dir_path,f) for f in os.listdir(dir_path)
            if os.path.isfile(os.path.join(dir_path,f))]

def running_median(dir_path):
    """
    Calculates the median number of words per line, for each line
    of the text files in the dir_path directory. If there are multiple
    files in that directory, the files will be combined into a
    single stream in alphabetical order.
    
    Args:
      dir_path (str): the directory of input files

    Returns:
      True if successful, False otherwise.
    """
    try:
        os.mkdir('wc_output') # directory to store output
    except OSError:
        pass
    histogram = collections.defaultdict(int)
    num_lines = 0
    input_files = get_input_files(dir_path)
    
    # check if no files available in the input directory
    if len(input_files) == 0:
        return False
    input_files.sort()
    f_objs = [open(f, 'r') for f in input_files]
    files_iter = itertools.chain(*f_objs)
    with open(os.path.join('wc_output', 'med_result.txt'), 'w') as f_output:
        for line in files_iter:
            num_lines += 1
            num_words = len(line.strip().split())            
            histogram[num_words] += 1           
            keys_list = histogram.keys()
            keys_list.sort()
            current_position = 0
            median = None
            
            # look for median
            for i in xrange(len(keys_list)):
                current_position += histogram[keys_list[i]]
                if current_position == ((num_lines + 1) / 2):
                    if (num_lines & 1) == 1:    # num_lines is odd
                        median = keys_list[i]
                    else:   # num_lines is even
                        median=(keys_list[i]+keys_list[i+1])/2.0
                    break
                elif current_position > ((num_lines + 1) / 2):
                    median = keys_list[i]
                    break               
            # write to output file       
            f_output.write(str(median) + '\n')
    # close up all files
    for f in f_objs:
        f.close()
    return True
    
