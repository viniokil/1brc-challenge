MAX_LINES = 1000000000


file_path = "measurements.txt"

# use file object as iterator
def read_iter():
    with open(file_path,'r') as f:
        lino = 0
        for line in f:
            lino+=1
            if lino == MAX_LINES:
                break
    return(lino)

print(read_iter())

# time python3 0_py_wc/main.py
# 1000000000
# 0_py_simple/main.py  60,65s user 1,71s system 99% cpu 1:02,36 total

# time pypy 0_py_wc/main.py
# 1000000000
# pypy 0_py_wc/main.py  27,02s user 2,06s system 99% cpu 29,075 total
