import pandas as pd


import dask.dataframe as dd
from numpy import float16, int16

from pandas import DataFrame

# def temp_converter_to_int(s: str) -> int16:
#     r = int16(s.replace('.', ''))
#     return r

temp_to_int = lambda t: int16((t.replace('.', '')))

# t = temp_to_int('19.4')
# print(t)

colnames = ['Station', 'Temp']
dtype = {'Station': 'string[pyarrow]',
         'Temp': float16}
converters={'Temp': temp_to_int}

# df = pd.read_csv('measurements_10k.txt', sep=';',
#                  names=colnames,
#                  header=None,
#                  dtype=dtype,
#                 #  converters=converters,
#                 #  index_col=0,
#                  memory_map=True,
#                 )
#                 #  nrows=1000)
# # print(pd.options.display.max_rows)


# # Set up multiprocessing
# pool = multiprocessing.Pool()


# print(df.tail(10))

# print(df.info(memory_usage="deep"))

# print(len(df))


import multiprocessing


# Read the large DataFrame in chunks
chunks = pd.read_csv('measurements.txt', sep=';',
                 names=colnames,
                 header=None,
                 dtype=dtype,
                #  converters=converters,
                #  index_col=0,
                 memory_map=True,
                 chunksize=100000
                )
                #  nrows=1000)

# Define a function to be applied to each chunk
def process_chunk(chunk: DataFrame):
    # Process the chunk here
    # lc = 0
    # for row in chunk.iterrows():
    #     lc =+ 1
    return len(chunk)

# Set up multiprocessing
pool = multiprocessing.Pool()

print(type(chunks))
# Apply the function to each chunk in parallel
result = pool.map(process_chunk, chunks)

# Concatenate the result into a single DataFrame
# result_df = pd.concat(result)

print(sum(result))

# Close the multiprocessing pool
pool.close()
