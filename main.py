import click

RAW_CSV_PATH = "data/taxi_tripdata.csv"
RAW_TABLE_NAME = "taxi_trips"
POSTGRES_URI = "postgresql+psycopg2://admin:password@localhost:5432/mydb"

FEATURES_TABLE = "taxi_trip_features"


@click.command()
@click.option(
    "--pipeline",
    type=click.Choice(["ingest", "aggregate"], case_sensitive=False),
    required=True,
)
def main(pipeline: str) -> None:
    """Run a pipeline stage."""
    if pipeline == "ingest":
        from ingestion.load_csv_to_postgres import CsvToPostgresLoader

        loader = CsvToPostgresLoader(
            csv_path=RAW_CSV_PATH,
            table_name=RAW_TABLE_NAME,
            postgres_uri=POSTGRES_URI,
        )
        loader.run()
    elif pipeline == "aggregate":
        from aggregation.location_hourly_features import (
            LocationHourlyFeatureAggregator,
        )

        aggregator = LocationHourlyFeatureAggregator(
            source_table=RAW_TABLE_NAME,
            target_table=FEATURES_TABLE,
            postgres_uri=POSTGRES_URI,
        )
        aggregator.run()


if __name__ == "__main__":
    main()
