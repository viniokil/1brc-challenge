import logging
from timeit_decorator import timeit


# Configure logging
logging.basicConfig(level=logging.INFO)

MAX_LINES = 1000000000

# MAX_LINES = 10 * 1000

# use file object as iterator
@timeit()
def read_iter():
    with open('measurements.txt','r') as f:
        lino = 0
        for line in f:
            lino+=1
            if lino == MAX_LINES:
                break
    return(lino)

print(read_iter())
