MAX_LINES: int = 1000000000

def divide_chunks(max: int, size: int):
    for i in range(0, max, size):
        first = i
        last = first + size
        if last > max:
            last = max
        print(f"{first}:{last}")


# divide_chunks(MAX_LINES, 10000)


# use file object as iterator
def wc():
    chunk_size: int = 10 * 1000
    with open('measurements.txt','r') as f:
        lines = f.readlines(10)
        print(lines)
        return(len(lines))

print(wc())

# time pypy3 00_py_wc/main.py
# 1000000000
# pypy3 00_py_wc/main.py  57,86s user 2,72s system 99% cpu 1:00,59 total






# def csv_reader(file_name):
#     for row in open(file_name, "r"):
#         yield row

# csv_gen = csv_reader("measurements.txt")
# row_count = 0

# for row in csv_gen:
#     row_count += 1

# print(f"Row count is {row_count}")