import click


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
            csv_path="data/taxi_tripdata.csv",
            table_name="green_taxi_trips",
            postgres_uri="postgresql+psycopg2://admin:password@localhost:5432/mydb",
        )
        loader.run()
    elif pipeline == "aggregate":
        from aggregation.location_hourly_features import (
            LocationHourlyFeatureAggregator,
        )

        aggregator = LocationHourlyFeatureAggregator(
            source_table="green_taxi_trips",
            target_table="green_taxi_location_hourly_features",
            postgres_uri="postgresql+psycopg2://admin:password@localhost:5432/mydb",
        )
        aggregator.run()


if __name__ == "__main__":
    main()
