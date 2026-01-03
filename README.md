# feast-fare-price-prediction
A Feast-based feature store demonstrating offline and online features for fare price prediction on NYC taxi data.

## Setup

1. Run `uv sync`

2. Load env variables by `source .env`

3. Create data directory by   `mkdir data`

4. Download data from kaggle  `kaggle datasets download -d anandaramg/taxi-trip-data-nyc -p data`

5. Unzip data `unzip data/taxi-trip-data-nyc.zip -d data`
