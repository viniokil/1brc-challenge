import polars as pl

import os

colnames = ['station', 'temp']

csv_schema={
    "station": pl.String,
    "temp": pl.Float32
}

MAX_LINES = 1000000000
batch_size=100000
# batch_size=50000

# df = pl.read_csv("measurements.txt", has_header=False, separator=';', schema=csv_schema, n_rows=100*1000*1000)
# print(df.estimated_size("mb"))


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


'''
l = "New Orleans;22.1"
station_stat =
{
    "Abha": {
        "avg_sum": 18.0,
        "avg_count": 2,
        "min": 23.0,
        "max": 59.2
    },
}
'''
# def process_line(l: str):
#     station, temp = l.split(";")
#     temp = float(temp)
#     if not STATISTIC.get(station):
#         STATISTIC[station] = {
#             "avg_sum": temp,
#             "avg_count": int(1),
#             "min": temp,
#             "max": temp
#         }
#     else:
#         avg_sum = STATISTIC[station]["avg_sum"] + temp
#         STATISTIC[station]["avg_sum"] = avg_sum
#         avg_count = STATISTIC[station]["avg_count"] + 1
#         STATISTIC[station]["avg_count"] = avg_count

#         if temp < STATISTIC[station]["min"]:
#             STATISTIC[station]["min"] = temp

#         if temp > STATISTIC[station]["max"]:
#             STATISTIC[station]["max"] = temp


if __name__ == "__main__":
    result_df_schema = {
        "station": pl.String,
        "avg_sum": pl.Float64,
        "avg_count": pl.Int32,
        "min": pl.Float64,
        "max": pl.Float64,
    }

    # result_df = pl.DataFrame(schema=result_df_schema)

    file_path="measurements.txt"
    reader = pl.read_csv_batched(
        file_path,
        has_header=False,
        separator=";",
        new_columns=csv_schema,
        # dtypes=[pl.String, pl.Float64],
        dtypes=[pl.String, pl.Float64],
        # n_rows=MAX_LINES,
        batch_size=batch_size
    )

    parallel_batches=os.cpu_count()
    lines = 0

    batches = reader.next_batches(parallel_batches)
    result_dfs = []
    while batches:
        for df in batches:
            lines+=df.height
            # select mi
            result_dfs.append(
                df.group_by('station').agg(
                    pl.col('temp').sum().alias('avg_sum'),
                    pl.col('temp').count().alias('avg_count'),
                    pl.col('temp').min().alias('min'),
                    pl.col('temp').mean().alias('avg'),
                    pl.col('temp').max().alias('max'),
                )
                # .sort('station')
            )
            # print(result_df)
        batches = reader.next_batches(parallel_batches)
    print(lines)


    # result_df = pl.DataFrame(schema=result_df_schema)
    # for df in result_dfs:
    #     print(len(df))
    result_df: pl.DataFrame = pl.concat(result_dfs)
    result = result_df.group_by('station').agg(
        # pl.col('avg_sum').truediv(pl.col("avg_count")).alias('avg'),
        pl.col('min').min(),
        pl.col('avg').mean(),
        # pl.col('avg_sum').sum().truediv(pl.col("avg_count").sum()).alias('avg_div'),
        (pl.col('avg_sum').sum() / pl.col('avg_count').sum()).alias('avg_div'),
        pl.col('avg_sum').sum(),
        pl.col('avg_count').sum(),
        pl.col('max').max(),
    ).sort('station')
    print(result)
    print(result.select(pl.sum('avg_count')))

    result_json_diff = pl.DataFrame(result.select(
        pl.col('station'),
        pl.col('avg_sum'),
        pl.col('avg_count'),
        pl.col('min'),
        pl.col('max'),
        ))
    # result_json_diff.write_json('polars_avg.json', row_oriented=True)

    r_dict = dict()
    for row in result_json_diff.rows():
        r_dict[row[0]] = {
            "avg_sum": row[1],
            "avg_count": row[2],
            "min": row[3],
            "max": row[4],
        }
    # print(r_dict)
    import json
    # print(json.dumps(r_dict, ensure_ascii=False))
    out_file = "polars_avg.json"
    with open(out_file, 'w', encoding ='utf8') as json_file:
        json.dump(r_dict, json_file, ensure_ascii = False, indent=2)

    # print(result.select(
    #     pl.col('station'),
    #     pl.col('min'),
    #     pl.col('avg'),
    #     pl.col('max'),
    #     ))
    # print(result_dfs)


    # wc("measurements.txt")
    # import cProfile

    # cProfile.run('wc()')




# parallel_batches=2
# batches = reader.next_batches(parallel_batches)

# for df in batches:
#     print(df)
#     print(df.height)
#     print(df.estimated_size("mb"))

# df = pl.read_csv_batched("measurements.txt", has_header=False, separator=';')


# df = pl.scan_csv("measurements_10k.txt", has_header=False, separator=';', schema=csv_schema)
