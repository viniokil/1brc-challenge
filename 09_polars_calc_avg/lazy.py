import polars as pl

'''
STATISTIC: dict = {
    "Abha": {
        "min": 23.0,
        "avg": 18.0,
        "max": 59.2
    },
    "Adelaide": {
        "min": -27.8,
        "avg": 17.3,
        "max": 58.5
    },
    }
'''
def print_stations_stat(stations_stat: dict) -> str:
    result_stat = dict(sorted(stations_stat.items()))
    output: str = "{"
    for station, stat in result_stat.items():
        output += f'{station}={stat["min"]}/{round(stat['avg'],1)}/{stat["max"]}, '
    output = output.rstrip(", ")
    output += "}"
    return output



file_path="measurements.txt"
csv_schema={
    "station": pl.String,
    "temp": pl.Float64
}

def calc_avg_1():
    q1 = (
        pl.scan_csv(
            file_path,
            has_header=False,
            separator=";",
            # dtypes=[pl.String, pl.Float64],
            schema=csv_schema,
            )
        .group_by('station')
        .agg(
            pl.col('temp').min().alias('min'),
            pl.col('temp').mean().alias('avg'),
            pl.col('temp').max().alias('max'),
        )
        .sort('station')
    )

    df = q1.collect(streaming=True)
    # print(df)
    stations_stat = dict()
    for row in df.rows():
        stations_stat[row[0]] = {
            "min": row[1],
            "avg": row[2],
            "max": row[3],
        }

    result_output = print_stations_stat(stations_stat)

    print(result_output)

    # with open('09_polars_calc_avg_out_q1.txt', 'w') as f:
    #     f.write(result_output)


def calc_avg_2():
    q1 = (
        pl.scan_csv(
            file_path,
            has_header=False,
            separator=";",
            # dtypes=[pl.String, pl.Float64],
            schema=csv_schema,
            )
        .group_by('station')
        .agg(
            pl.col('temp').min().alias('min'),
            pl.col('temp').mean().alias('avg'),
            pl.col('temp').max().alias('max'),
        )
        .sort('station')
    )

    df = q1.collect(streaming=True)

    df_res= df.select([
        pl.format("{}={}/{}/{}",
            pl.col('station'),
            pl.col('min'),
            pl.col('avg').round(1),
            pl.col('max'),
        ).alias("fmt")
    ]).select(
        pl.format("{{}}",
            pl.col("fmt").str.concat(delimiter=', ')
        )
    )

    result_output = str(df_res[0,0])

    with open('09_polars_calc_avg_out_q2.txt', 'w') as f:
        f.write(result_output)

    return result_output



import cProfile

# cProfile.run('calc_avg_1()')

# cProfile.run('calc_avg_2()')
calc_avg_2()
