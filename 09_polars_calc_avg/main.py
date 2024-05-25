import polars as pl

file_path="measurements.txt"
csv_schema={
    "station": pl.String,
    "temp": pl.Float64
}


def calc_avg():
    q1 = (
        pl.scan_csv(
            file_path,
            has_header=False,
            separator=";",
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

    # with open('09_polars_calc_avg_out_q2.txt', 'w') as f:
    #     f.write(result_output)

    return result_output



# import cProfile

# cProfile.run('calc_avg()')
print(calc_avg())

# ➤ time python 09_polars_calc_avg/main.py > 09_polars_calc_avg_out.txt
# python 09_polars_calc_avg/main.py > 09_polars_calc_avg_out.txt  99,63s user 2,06s system 1386% cpu 7,336 total
