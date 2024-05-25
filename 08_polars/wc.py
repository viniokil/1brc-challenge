import polars as pl

import os


colnames = ['Station', 'Temp']

csv_schema={
    "Station": pl.String,
    "Temp": pl.Float32
}

# df = pl.read_csv("measurements.txt", has_header=False, separator=';', schema=csv_schema, n_rows=100*1000*1000)
# print(df.estimated_size("mb"))



MAX_LINES = 1000000000
batch_size=100000
# batch_size=50000

def wc(file_path="measurements.txt"):
    reader = pl.read_csv_batched(
        file_path,
        has_header=False,
        separator=";",
        new_columns=csv_schema,
        # dtypes=[pl.String, pl.Float32]
        # n_rows=MAX_LINES,
        batch_size=batch_size
    )

    parallel_batches=os.cpu_count()
    lines = 0

    batches = reader.next_batches(parallel_batches)
    while batches:
        for df in batches:
            lines+=df.height
            # lines += len(df)
        batches = reader.next_batches(parallel_batches)
    print(lines)


if __name__ == "__main__":
    wc()
    import cProfile

    cProfile.run('wc()')




# parallel_batches=2
# batches = reader.next_batches(parallel_batches)

# for df in batches:
#     print(df)
#     print(df.height)
#     print(df.estimated_size("mb"))

# df = pl.read_csv_batched("measurements.txt", has_header=False, separator=';')


# df = pl.scan_csv("measurements_10k.txt", has_header=False, separator=';', schema=csv_schema)
