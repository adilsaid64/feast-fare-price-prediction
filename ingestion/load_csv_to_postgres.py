from typing import Any
import pandas as pd
from sqlalchemy import Engine, create_engine, text


class CsvToPostgresLoader:
    """Load a CSV file into a Postgres table."""

    def __init__(self, csv_path: str, table_name: str, postgres_uri: str) -> None:
        """Initialize loader configuration."""
        self.csv_path: str = csv_path
        self.table_name: str = table_name
        self.postgres_uri: str = postgres_uri

    def load_csv(self) -> pd.DataFrame:
        """Read and normalize the CSV file."""
        df: pd.DataFrame = pd.read_csv(self.csv_path)
        df.columns = [c.strip().lower() for c in df.columns]
        df = df.rename(
            columns={
                "lpep_pickup_datetime": "pickup_datetime",
                "lpep_dropoff_datetime": "dropoff_datetime",
            }
        )
        df["pickup_datetime"] = pd.to_datetime(
            df["pickup_datetime"], errors="coerce")
        df["dropoff_datetime"] = pd.to_datetime(
            df["dropoff_datetime"], errors="coerce")
        df = df.dropna(subset=["pickup_datetime"])
        print("Dataframe Loadad - Shape, ", df.shape)
        return df

    def create_table(self) -> None:
        """Create the destination table if it does not exist."""
        sql: str = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            vendorid INTEGER,
            pickup_datetime TIMESTAMP NOT NULL,
            dropoff_datetime TIMESTAMP,
            store_and_fwd_flag VARCHAR(1),
            ratecodeid INTEGER,
            pulocationid INTEGER,
            dolocationid INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE PRECISION,
            fare_amount DOUBLE PRECISION,
            extra DOUBLE PRECISION,
            mta_tax DOUBLE PRECISION,
            tip_amount DOUBLE PRECISION,
            tolls_amount DOUBLE PRECISION,
            ehail_fee DOUBLE PRECISION,
            improvement_surcharge DOUBLE PRECISION,
            total_amount DOUBLE PRECISION,
            payment_type INTEGER,
            trip_type INTEGER,
            congestion_surcharge DOUBLE PRECISION
        );
        """
        with self._engine().begin() as conn:
            conn.execute(text(sql))

    def write(self, df: pd.DataFrame) -> None:
        """Persist the dataframe to Postgres."""
        df.to_sql(
            self.table_name,
            self._engine(),
            if_exists="append",
            index=False,
            chunksize=20_000,
            method="multi",
        )

    def run(self) -> None:
        """Execute the ingestion pipeline."""
        print("Loading CSV")
        df: pd.DataFrame = self.load_csv()
        print("Creating table")
        self.create_table()
        print("Writing data")
        self.write(df)
        print(f"Loaded {len(df)} rows into {self.table_name}")

    def _engine(self) -> Engine:
        """Create a SQLAlchemy engine."""
        return create_engine(self.postgres_uri)
