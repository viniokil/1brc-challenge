import polars as pl

csv_file_path = csv_file_path="measurements.txt"
csv_schema = {"station": pl.String, "temp": pl.Float64}

parquet_file = "measurements.parquet"

lf = pl.scan_csv(
            csv_file_path,
            has_header=False,
            separator=";",
            schema=csv_schema,
            )

lf.sink_parquet(parquet_file)
