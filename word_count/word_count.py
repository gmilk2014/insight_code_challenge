import string
         
def mapper(word):
    """
    Remove all punctuations and make word lowercase
    then emit (word, 1) tuple.
    If the words contains only punctuations, emit ('<INVALID>', 1)
    
    Args:
      word (str): a key word

    Returns:
      ('<INVALID>', '1') If the words contains only punctuations.
      Otherwise, (word, '1')
    """
    
    table = string.maketrans("","")
    word = word.translate(table, string.punctuation)
    word = word.lower()
    if word == '':
        word = '<INVALID>'
    return (word, '1')

def reducer(key, values):
    """
    Sum all the numbers in the list values
    
    Args:
      key (str): a key word
      values (list of int): a list of integers

    Returns:
      key with its associate sum of values
    """
    
    return (key, sum(values))
