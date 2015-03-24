#Insight Data Engineering Fellows Program - Coding Challenge

Write clean, well-documented code that scales for large amounts of data.

###How to run the program?
The program is developed under python 2.7.6 without any outside extra dependencies. Any python version after 2.6.x should works as well but note that python 3 may or may not work.

Simply run `$ ./run.sh` or `$ python word_count <lines_per_split>`
where `<lines_per_split>` is the number of lines in each split. Default to 2000 lines per split.
Please make sure the directory wc_input is in the same directory with word_count.

###Task1: word counting
My approach is to implement a MapReduce framework with python multiprocessing modules.
MapReduce is a programming model designed for processing large volumes of data in parallel by dividing the work into a set of independent tasks.

The modern MapReduce implementation often includes a filesystem that helps to manage big data in fast, distributed and fault-tolerance way, but for my simple multiprocessing MapReduce framework I did not implement such file system. I can certainly augement my code in the future to incorporate this.

The pipeline for my multiprocess MapReduce word counting job can roughly be summarized as

1. split data into several chunks, each chunk contains several lines of original data
2. distribute chunks to map workers that perform user-defined map function. In the word counting case, the map function simply emit (word, 1) given a word in file. (word, 1) pair is collected into a buffer.
3. spill workers will try to sort the content of the buffer by alphabetical order of keys and flush the content to disk when the buffer is 'almost' full. An optional combiner function is called before flushing to disk.
4. We then merge all sorted spill files on disk into one file.
5. reducer take the merged file and sum the values for each key. This step usually refers to as 'summary'
6. the final output is written to wc_output/wc_result.txt

####Aside:
#####What is a word?
A 'word' is not really clearly defined in original problem, so please refer to [FAQ](https://github.com/InsightDataScience/cc-example#faq). In natural language processing literature, how to define a word is an open-ended qeustions. It really depends on your real tasks, such as spell checking, part of speech tagging and etc.

###Task2: running median
Running Median keeps track of the median for a stream of numbers, updating the median for each new number.
There are many good algorithms to do this, using two heaps is particularly a good one. However, it does not scale well with large data. In general, finding running median is not trivial, and to find the exact solution with memory constraint is very hard. If exact solution is not required, there are some probabilistic methods to estimate the median.

To simplify the problem, observe that all data are positive integers corresponding to the number of words in a given line, so I make the following assumption:
```
Suppose we have a huge data that contains n lines, and the number of words in each line
is in the range 1 to k where k << n, and k can fit into main memory while n lines of
data cannot.
```
With this assumption, I can update the distributition of the numbers when I see a new number on the fly and compute the new running median.

####Algorithm:
1. create a dictionary
2. read in a line
3. get the number of words in the line
4. use the number of words as key
   - if key already exist:
      - increment the value associated with the key by 1
   - else:
      - create a new key associate with value 1
5. update the median and write it to disk
6. repeat 2-5 till end of files

The magic here is, given the distributition of the numbers, how to find the median?
I do the following:

1. get all the keys from the dictionary
2. sort the keys
3. compute the cumulative distribution until we find the mid point

Note that *k'* denotes the current number of distinct keys as opposed to *k* which denotes the maximum value of *k'*
#####Worst case:
######Each round:
Sorting keys is O(*k'* * log *k'*) and finding mid point is O(*k'*/2) = O(*k'*) (when all numbers are uniformly distributed)
#####Best case:
######Each round:
Sorting keys is O(*k'*) and finding mid point is O(1) (think of the case when all numbers are the same)

####What if the assumption does not hold?
If at some point the main memory is not able to hold the histogram because the assumption made above is violated, i.e. k is too big to fit into main memory, we can compress the histogram using [q-digest](http://www.cs.virginia.edu/~son/cs851/papers/ucsb.sensys04.pdf). Q-digest algorithm will tend to preserve the accurate count of the numbers that frequently occur and aggregate the count of the numbers being seen less frequently into a larger bucket. This will indeed introduce error into the final result but there is got to be a trade-off between space and error. No free lunch!! Note that q-digest is not implemented in my code yet but would be a good feature to add on in the future. For more information on q-digest, please see the [original papaer](http://www.cs.virginia.edu/~son/cs851/papers/ucsb.sensys04.pdf).
