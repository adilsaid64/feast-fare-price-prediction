from sqlalchemy import Engine, create_engine, text


class LocationHourlyFeatureAggregator:
    """Aggregate trip events into hourly location features."""

    def __init__(
        self,
        source_table: str,
        target_table: str,
        postgres_uri: str,
    ) -> None:
        """Initialize aggregation configuration."""
        self.source_table: str = source_table
        self.target_table: str = target_table
        self.postgres_uri: str = postgres_uri

    def create_table(self) -> None:
        """Create the feature table if it does not exist."""
        sql: str = f"""
        CREATE TABLE IF NOT EXISTS {self.target_table} (
            pulocationid INTEGER NOT NULL,
            event_timestamp TIMESTAMP NOT NULL,
            trip_count INTEGER,
            avg_fare_amount DOUBLE PRECISION,
            avg_trip_distance DOUBLE PRECISION,
            PRIMARY KEY (pulocationid, event_timestamp)
        );
        """
        with self._engine().begin() as conn:
            conn.execute(text(sql))

    def aggregate(self) -> None:
        """Compute hourly aggregates from raw trip data."""
        sql: str = f"""
        INSERT INTO {self.target_table}
        SELECT
            pulocationid,
            DATE_TRUNC('hour', pickup_datetime) AS event_timestamp,
            COUNT(*) AS trip_count,
            AVG(fare_amount) AS avg_fare_amount,
            AVG(trip_distance) AS avg_trip_distance
        FROM {self.source_table}
        GROUP BY pulocationid, event_timestamp
        ON CONFLICT (pulocationid, event_timestamp)
        DO NOTHING;
        """
        with self._engine().begin() as conn:
            conn.execute(text(sql))

    def run(self) -> None:
        """Execute the aggregation pipeline."""
        print("Creating feature table")
        self.create_table()
        print("Aggregating features")
        self.aggregate()
        print("Aggregation complete")

    def _engine(self) -> Engine:
        """Create a SQLAlchemy engine."""
        return create_engine(self.postgres_uri)
