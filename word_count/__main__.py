"""Driver program to run word_count and running_median
Please make sure the directory wc_input is in the same
directory with word_count
Example:
    $ python word_count 2000
"""

import sys
import multiproc_MapReduce
from word_count import *
from running_median import *

def usage():
    return ('Usage: python word_count <lines_per_split>\n' +
              'lines_per_split (int): number of lines in ' +
              'each split. Default to 2000 lines per split.')
              
def main(input_dir, lines_per_split):
    # this is to make sure directory wc_input exist
    try:
        os.mkdir(input_dir)
    except OSError:
        pass
    MapReduce_job = multiproc_MapReduce.MapReduce(
        mapper, reducer, combiner=reducer, num_workers=4)
    MapReduce_job.run(input_dir, lines_per_split)       
    running_median(input_dir)
    
if __name__ == '__main__':
    input_dir = 'wc_input'
    lines_per_split = 2000
    if len(sys.argv) > 2:
        print usage()
        sys.exit(1)
    if len(sys.argv) == 2:
        try:
            lines_per_split = int(sys.argv[1])
        except ValueError:
            print usage()
            sys.exit(1)
    main(input_dir, lines_per_split)
    
    
    
