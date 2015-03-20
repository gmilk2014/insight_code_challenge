#Insight Data Engineering Fellows Program - Coding Challenge

Write clean, well-documented code that scales for large amounts of data.

###Task1: word counting
My approach is to implement a MapReduce framework with python multiprocessing modules.
MapReduce is a programming model designed for processing large volumes of data in parallel by dividing the work into a set of independent tasks.

The modern MapReduce implementation often includes a filesystem that helps to manage big data in fast, distributed and falt-tolerance way, but for my simple multiprocessing MapReduce framework I did not implement such file system. I can certainly augement my code in the future to incorporate this.

The pipeline for my multiprocess MapReduce word counting job can roughly be summarized as

1. split data into several chunks, each chunk contains several lines of original data
2. distribute chunks to map workers and store the result on disk
3. combiner then perform partial sum on the itermediate result from mappers
4. partitioner then aggregates all the intermediate result and feed the partition result to reducer because all values for the same key are always reduced together regardless of which mapper is its origin
5. reducer take the partition and sum the values for each key. This step usually refers to as 'summary'
6. the final output is written to wc_output/wc_result.txt
Note that the final output is not sorted. It's certainly a good feature to be added in the future.

####Aside:
#####What is a word?
The sample output hints that the uppercase words are converted into lowercase words. However, since 'word' is not really clearly defined in original problem, and in natural language processing context, how to define a word is also an open-ended qeustions. It really depends on your real tasks, such as spell checking, part of speech tagging and etc. So I am making the following rules to define a word:

1. all words will be converted into lowercase
2. a word contains no leading or trailing whitespace
3. a word contains no leading or trailing punctuation
4. a word may contain number
5. However, stand-alone pure punctuations are word
6. When rule 2 & 3 are violated, all leading and trailing whitespaces and punctuations are removed.

#####Examples:
- hello! -> hello (rule 6)
- kc789@cornell.edu -> kc789@cornell.edu (stay as it is becuase punctuations inside a word is ok)
- %#$%# -> %#$%# (rule 5)
- World -> world (rule 1)

###Task2: running median
Running Median keeps track of the median for a stream of numbers, updating the median for each new number.
There are many good algorithms to do this, using two heaps is particularly a good one. However, it does not scale well with large data. In general, finding running median is not trivial, and to find the exact solution with memory constraint is very hard. If exact solution is not required, there are some statistical methods to estimate the median.

To simplify the problem, observe that all data are positive integers corresponding to the number of words in a given line, so I make the following assumption:
>Suppose we have a huge data that contains n lines, and the number of words in each line is in the range 1 to k where k << n, and k can fit into main memory while n lines of data cannot.
With this assumption, I can update the distributition of the numbers when I see a new number on the fly and compute the new running median.

####Algorithm:
0. create a dictionary
1. read in a line
2. get the number of words in the line
3. use the number of words as key
   - if key already exist:
      - increment the value associated with the key by 1
   - else:
      - create a new key associate with value 1
4. update the median and write it to disk
5. repeat 1-4 till end of files

The magic here is, given the distributition of the numbers, how to find the median?
I do the following:

1. get all the keys from the dictionary
2. sort the keys
3. compute the cumulative distribution until we find the mid point

#####Worst case:
Sorting keys is O(k * log k) and finding mid point is O(k/2) = O(k) (when all numbers are uniformly distributed)
#####Best case:
Sorting keys is O(k) and finding mid point is O(1) (think of the case when all numbers are the same)
