import polars as pl

file_path = "measurements.txt"
csv_schema = {"station": pl.String, "temp": pl.Float64}


def calc_avg():
    q1 = (
        pl.scan_csv(
            file_path,
            has_header=False,
            separator=";",
            schema=csv_schema,
            # n_rows=1000*1000
        )
        .group_by("station")
        .agg(
            pl.col("temp").min().alias("min"),
            pl.col("temp").mean().alias("avg"),
            pl.col("temp").max().alias("max"),
        )
        .sort("station")
    )

    q2 = (
        q1.select(
            [
                pl.format(
                    "{}={}/{}/{}",
                    pl.col("station"),
                    pl.col("min"),
                    pl.col("avg").round(1),
                    pl.col("max"),
                ).alias("fmt")
            ]
        )
        .select(pl.format("{{}}", pl.col("fmt").str.concat(delimiter=", ")))
        .lazy()
    )
    # print(q2.explain())

    df_res = q2.collect(streaming=True)

    result_output = str(df_res[0, 0])
    print(result_output)

    with open('10_polars_optimize_out.txt', 'w') as f:
        f.write(result_output)

    # return result_output


# import cProfile

# cProfile.run('calc_avg()')
calc_avg()

# ➤ time python 10_polars_calc_avg/main.py > 10_polars_calc_avg_out.txt
# python 10_polars_calc_avg/main.py > 10_polars_calc_avg_out.txt  99,63s user 2,06s system 1386% cpu 7,336 total
