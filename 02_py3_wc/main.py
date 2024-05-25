MAX_LINES: int = 1000000000

# use file object as iterator
def read_iter():
    with open('measurements.txt','r') as f:
        lino: int = 0
        for line in f:
            lino+=1
            if lino == MAX_LINES:
                break
    return(lino)

print(read_iter())

# time pypy3 00_py_wc/main.py
# 1000000000
# pypy3 00_py_wc/main.py  57,86s user 2,72s system 99% cpu 1:00,59 total
