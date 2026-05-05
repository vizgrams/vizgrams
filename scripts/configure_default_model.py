#!/usr/bin/env python3
# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Configure the 'default' model with public datasets via the API.

Usage:
    # Local
    python scripts/configure_default_model.py

    # Against prod via SSH tunnel
    python scripts/configure_default_model.py --api-url http://localhost:9000

    # Against prod directly (if API is exposed)
    python scripts/configure_default_model.py --api-url https://vizgrams.com
"""

import argparse
import sys
import time

import requests


def api(base: str, method: str, path: str, json=None, expect=(200, 201, 202)):
    url = f"{base}{path}"
    r = getattr(requests, method)(url, json=json)
    if r.status_code not in expect:
        print(f"  FAIL {method.upper()} {path}: {r.status_code} {r.text[:120]}")
        return None
    return r.json() if r.text else {}


def configure(base: str):
    B = f"{base}/api/v1/model/default"

    # =====================================================================
    # 1. Create model (if not exists)
    # =====================================================================
    r = requests.get(f"{B}")
    if r.status_code == 404:
        print("Creating 'default' model...")
        api(base, "post", "/api/v1/model", json={
            "name": "default",
            "display_name": "Public Datasets",
            "description": "OpenFlights, USGS Earthquakes, CoinGecko Crypto, REST Countries",
            "owner": "vizgrams",
        }, expect=(200, 201))
    else:
        print("Model 'default' exists")

    # =====================================================================
    # 2. Configure tools
    # =====================================================================
    print("\nConfiguring tools...")
    api(base, "put", f"{B}/config".replace(base, ""), json={"tools": {
        "openflights": {"enabled": True},
        "usgs_earthquakes": {"enabled": True},
        "rest_countries": {"enabled": True},
        "coingecko": {"enabled": True},
    }})

    # =====================================================================
    # 3. Extractors
    # =====================================================================
    print("\nConfiguring extractors...")

    extractors = {
        "openflights": """schedule:
  cron: "0 3 * * 0"
tasks:
  - name: airports
    tool: openflights
    command: airports
    params: {}
    output:
      table: airports
      write_mode: REPLACE
      columns:
        - name: iata_code
          json_path: $.iata_code
        - name: icao_code
          json_path: $.icao_code
        - name: name
          json_path: $.name
        - name: city
          json_path: $.city
        - name: country
          json_path: $.country
        - name: latitude
          json_path: $.latitude
          type: FLOAT
        - name: longitude
          json_path: $.longitude
          type: FLOAT
        - name: altitude_ft
          json_path: $.altitude_ft
          type: INTEGER
        - name: timezone
          json_path: $.timezone
  - name: airlines
    tool: openflights
    command: airlines
    params: {}
    output:
      table: airlines
      write_mode: REPLACE
      columns:
        - name: iata_code
          json_path: $.iata_code
        - name: icao_code
          json_path: $.icao_code
        - name: name
          json_path: $.name
        - name: callsign
          json_path: $.callsign
        - name: country
          json_path: $.country
        - name: active
          json_path: $.active
  - name: routes
    tool: openflights
    command: routes
    params: {}
    output:
      table: routes
      write_mode: REPLACE
      columns:
        - name: airline_code
          json_path: $.airline_code
        - name: source_airport_code
          json_path: $.source_airport_code
        - name: dest_airport_code
          json_path: $.dest_airport_code
        - name: codeshare
          json_path: $.codeshare
        - name: stops
          json_path: $.stops
          type: INTEGER
        - name: equipment
          json_path: $.equipment""",

        "usgs_earthquakes": """schedule:
  cron: "0 * * * *"
tasks:
  - name: recent_earthquakes
    tool: usgs_earthquakes
    command: recent
    params:
      days: "7"
      min_magnitude: "2.5"
    output:
      table: earthquakes
      write_mode: APPEND
      columns:
        - name: event_id
          json_path: $.event_id
        - name: magnitude
          json_path: $.magnitude
          type: FLOAT
        - name: magnitude_type
          json_path: $.magnitude_type
        - name: place
          json_path: $.place
        - name: time
          json_path: $.time
        - name: latitude
          json_path: $.latitude
          type: FLOAT
        - name: longitude
          json_path: $.longitude
          type: FLOAT
        - name: depth_km
          json_path: $.depth_km
          type: FLOAT
        - name: felt
          json_path: $.felt
          type: INTEGER
        - name: alert
          json_path: $.alert
        - name: tsunami
          json_path: $.tsunami
        - name: significance
          json_path: $.significance
          type: INTEGER
        - name: type
          json_path: $.type
        - name: title
          json_path: $.title
        - name: url
          json_path: $.url""",

        "rest_countries": """schedule:
  cron: "0 4 1 * *"
tasks:
  - name: countries
    tool: rest_countries
    command: countries
    params: {}
    output:
      table: countries
      write_mode: REPLACE
      columns:
        - name: name_common
          json_path: $.name_common
        - name: name_official
          json_path: $.name_official
        - name: cca2
          json_path: $.cca2
        - name: cca3
          json_path: $.cca3
        - name: region
          json_path: $.region
        - name: subregion
          json_path: $.subregion
        - name: capital
          json_path: $.capital
        - name: population
          json_path: $.population
          type: INTEGER
        - name: area
          json_path: $.area
          type: FLOAT
        - name: latitude
          json_path: $.latitude
          type: FLOAT
        - name: longitude
          json_path: $.longitude
          type: FLOAT
        - name: languages
          json_path: $.languages
        - name: currencies
          json_path: $.currencies
        - name: flag_emoji
          json_path: $.flag_emoji
        - name: borders
          json_path: $.borders""",

        "coingecko": """schedule:
  cron: "*/15 * * * *"
tasks:
  - name: coins
    tool: coingecko
    command: coins
    params: {}
    output:
      table: coins
      write_mode: REPLACE
      columns:
        - name: coin_id
          json_path: $.id
        - name: symbol
          json_path: $.symbol
        - name: name
          json_path: $.name
        - name: current_price
          json_path: $.current_price
          type: FLOAT
        - name: market_cap
          json_path: $.market_cap
          type: FLOAT
        - name: market_cap_rank
          json_path: $.market_cap_rank
          type: INTEGER
        - name: total_volume
          json_path: $.total_volume
          type: FLOAT
        - name: high_24h
          json_path: $.high_24h
          type: FLOAT
        - name: low_24h
          json_path: $.low_24h
          type: FLOAT
        - name: price_change_percentage_24h
          json_path: $.price_change_percentage_24h
          type: FLOAT
        - name: circulating_supply
          json_path: $.circulating_supply
          type: FLOAT
        - name: total_supply
          json_path: $.total_supply
          type: FLOAT
        - name: ath
          json_path: $.ath
          type: FLOAT
        - name: ath_date
          json_path: $.ath_date
        - name: last_updated
          json_path: $.last_updated""",
    }

    for tool, yaml_content in extractors.items():
        r = api(base, "put", f"{B}/tool/{tool}/extract".replace(base, ""),
                json={"content": yaml_content})
        print(f"  {tool}: {'OK' if r is not None else 'FAIL'}")

    # =====================================================================
    # 4. Entities
    # =====================================================================
    print("\nConfiguring entities...")

    entities = {
        "Earthquake": """entity: Earthquake
description: "A seismic event recorded by the USGS"
display:
  list: [title, magnitude, time, depth_km]
  detail: [title, magnitude, magnitude_type, place, time, latitude, longitude, depth_km, alert, tsunami, significance]
identity:
  event_id:
    type: STRING
    semantic: PRIMARY_KEY
attributes:
  title:
    type: STRING
    semantic: IDENTIFIER
  magnitude:
    type: FLOAT
    semantic: MEASURE
  magnitude_type:
    type: STRING
    semantic: ATTRIBUTE
  place:
    type: STRING
    semantic: ATTRIBUTE
  time:
    type: STRING
    semantic: TIMESTAMP
  latitude:
    type: FLOAT
    semantic: ATTRIBUTE
  longitude:
    type: FLOAT
    semantic: ATTRIBUTE
  depth_km:
    type: FLOAT
    semantic: MEASURE
  felt:
    type: INTEGER
    semantic: MEASURE
  alert:
    type: STRING
    semantic: ATTRIBUTE
  tsunami:
    type: STRING
    semantic: ATTRIBUTE
  significance:
    type: INTEGER
    semantic: MEASURE
  type:
    type: STRING
    semantic: ATTRIBUTE
  url:
    type: STRING
    semantic: ATTRIBUTE
relations: {}""",

        "CryptoAsset": """entity: CryptoAsset
description: "A cryptocurrency"
display:
  list: [name, symbol, coin_id]
  detail: [name, symbol, coin_id, ath, ath_date, circulating_supply, total_supply]
identity:
  coin_id:
    type: STRING
    semantic: PRIMARY_KEY
attributes:
  symbol:
    type: STRING
    semantic: IDENTIFIER
  name:
    type: STRING
    semantic: IDENTIFIER
  ath:
    type: FLOAT
    semantic: MEASURE
  ath_date:
    type: STRING
    semantic: TIMESTAMP
  circulating_supply:
    type: FLOAT
    semantic: MEASURE
  total_supply:
    type: FLOAT
    semantic: MEASURE
events:
  price_tick:
    description: "Point-in-time price observation"
    attributes:
      inserted_at:
        type: STRING
        semantic: INSERTED_AT
      price:
        type: FLOAT
        semantic: MEASURE
      volume:
        type: FLOAT
        semantic: MEASURE
      market_cap:
        type: FLOAT
        semantic: MEASURE
      market_cap_rank:
        type: INTEGER
        semantic: ATTRIBUTE
      price_change_pct_24h:
        type: FLOAT
        semantic: MEASURE
      high_24h:
        type: FLOAT
        semantic: MEASURE
      low_24h:
        type: FLOAT
        semantic: MEASURE
relations: {}""",
    }

    for name, yaml_content in entities.items():
        r = api(base, "put", f"{B}/entity/{name}/yaml".replace(base, ""),
                json={"content": yaml_content})
        print(f"  {name}: {'OK' if r is not None else 'FAIL'}")

    # =====================================================================
    # 5. Mappers
    # =====================================================================
    print("\nConfiguring mappers...")

    mappers = {
        "earthquake": """schedule:
  cron: "5 * * * *"

mapper: earthquake
description: "Map raw earthquake data to Earthquake entity"
grain: eq
sources:
  - alias: eq
    table: earthquakes
    columns: [event_id, title, magnitude, magnitude_type, place, time, latitude, longitude, depth_km, felt, alert, tsunami, significance, type, url]
    filter: "event_id IS NOT NULL AND event_id != ''"
targets:
  - entity: Earthquake
    columns:
      - name: event_id
        expr: eq.event_id
      - name: title
        expr: eq.title
      - name: magnitude
        expr: eq.magnitude
      - name: magnitude_type
        expr: eq.magnitude_type
      - name: place
        expr: eq.place
      - name: time
        expr: eq.time
      - name: latitude
        expr: eq.latitude
      - name: longitude
        expr: eq.longitude
      - name: depth_km
        expr: eq.depth_km
      - name: felt
        expr: eq.felt
      - name: alert
        expr: eq.alert
      - name: tsunami
        expr: eq.tsunami
      - name: significance
        expr: eq.significance
      - name: type
        expr: eq.type
      - name: url
        expr: eq.url""",

        "crypto_asset": """schedule:
  cron: "5 */1 * * *"

mapper: crypto_asset
description: "Map CoinGecko data to CryptoAsset dimension"
grain: co
sources:
  - alias: co
    table: coins
    columns: [coin_id, symbol, name, ath, ath_date, circulating_supply, total_supply]
    filter: "coin_id IS NOT NULL AND coin_id != ''"
targets:
  - entity: CryptoAsset
    columns:
      - name: coin_id
        expr: co.coin_id
      - name: symbol
        expr: co.symbol
      - name: name
        expr: co.name
      - name: ath
        expr: co.ath
      - name: ath_date
        expr: co.ath_date
      - name: circulating_supply
        expr: co.circulating_supply
      - name: total_supply
        expr: co.total_supply""",

        "crypto_price_tick": """schedule:
  cron: "5 */1 * * *"

mapper: crypto_price_tick
description: "Map CoinGecko snapshot to CryptoAsset price_tick event stream"
depends_on:
  - crypto_asset
grain: co
sources:
  - alias: co
    table: coins
    columns: [coin_id, current_price, total_volume, market_cap, market_cap_rank, price_change_percentage_24h, high_24h, low_24h, last_updated]
    filter: "coin_id IS NOT NULL AND coin_id != ''"
targets:
  - entity: CryptoAssetPriceTickEvent
    columns:
      - name: coin_id
        expr: co.coin_id
      - name: price
        expr: co.current_price
      - name: volume
        expr: co.total_volume
      - name: market_cap
        expr: co.market_cap
      - name: market_cap_rank
        expr: co.market_cap_rank
      - name: price_change_pct_24h
        expr: co.price_change_percentage_24h
      - name: high_24h
        expr: co.high_24h
      - name: low_24h
        expr: co.low_24h""",
    }

    for name, yaml_content in mappers.items():
        r = api(base, "put", f"{B}/mapper/{name}".replace(base, ""),
                json={"content": yaml_content})
        print(f"  {name}: {'OK' if r is not None else 'FAIL'}")

    # =====================================================================
    # 6. Features
    # =====================================================================
    print("\nConfiguring features...")

    features = {
        ("CryptoAsset", "current_price"): """feature_id: cryptoasset.current_price
name: Current Price
entity_type: CryptoAsset
entity_key: coin_id
data_type: FLOAT
materialization_mode: materialized
expr: argmax(price_tick.price, price_tick.inserted_at)""",

        ("CryptoAsset", "market_cap"): """feature_id: cryptoasset.market_cap
name: Market Cap
entity_type: CryptoAsset
entity_key: coin_id
data_type: FLOAT
materialization_mode: materialized
expr: argmax(price_tick.market_cap, price_tick.inserted_at)""",

        ("CryptoAsset", "market_cap_rank"): """feature_id: cryptoasset.market_cap_rank
name: Market Cap Rank
entity_type: CryptoAsset
entity_key: coin_id
data_type: INTEGER
materialization_mode: materialized
expr: argmax(price_tick.market_cap_rank, price_tick.inserted_at)""",

        ("CryptoAsset", "price_change_24h"): """feature_id: cryptoasset.price_change_24h
name: Price Change 24h
entity_type: CryptoAsset
entity_key: coin_id
data_type: FLOAT
materialization_mode: materialized
expr: argmax(price_tick.price_change_pct_24h, price_tick.inserted_at)""",

        ("CryptoAsset", "total_volume"): """feature_id: cryptoasset.total_volume
name: Total Volume
entity_type: CryptoAsset
entity_key: coin_id
data_type: FLOAT
materialization_mode: materialized
expr: argmax(price_tick.volume, price_tick.inserted_at)""",
    }

    for (entity, fname), yaml_content in features.items():
        r = api(base, "put",
                f"{B}/entity/{entity}/feature/{fname}".replace(base, ""),
                json={"content": yaml_content})
        print(f"  {entity}.{fname}: {'OK' if r is not None else 'FAIL'}")

    # =====================================================================
    # 7. Queries
    # =====================================================================
    print("\nConfiguring queries...")

    queries = {
        "recent_earthquakes": """name: recent_earthquakes
root: Earthquake
description: "Recent earthquakes ordered by time"
attributes:
  - title
  - magnitude
  - place
  - time
  - latitude
  - longitude
  - depth_km
  - alert
order:
  - time: desc""",

        "top_cryptos": """name: top_cryptos
root: CryptoAsset
description: "Top cryptocurrencies by market cap"
attributes:
  - name
  - symbol
  - current_price
  - market_cap
  - market_cap_rank
  - price_change_24h
  - total_volume
order:
  - market_cap_rank: asc""",
    }

    for name, yaml_content in queries.items():
        r = api(base, "put", f"{B}/query/{name}".replace(base, ""),
                json={"content": yaml_content})
        print(f"  {name}: {'OK' if r is not None else 'FAIL'}")

    # =====================================================================
    # 8. Views
    # =====================================================================
    print("\nConfiguring views...")

    views = {
        "earthquake_map": """name: earthquake_map
type: map
query: recent_earthquakes
visualization:
  lat: latitude
  lon: longitude
  label: title
  popup: [magnitude, place, time, depth_km]
  size: magnitude""",

        "crypto_prices": """name: crypto_prices
type: table
query: top_cryptos
visualization:
  columns: [name, symbol, current_price, market_cap_rank, price_change_24h, total_volume]""",
    }

    for name, yaml_content in views.items():
        r = api(base, "put", f"{B}/view/{name}".replace(base, ""),
                json={"content": yaml_content})
        print(f"  {name}: {'OK' if r is not None else 'FAIL'}")

    # =====================================================================
    # 9. Run initial extractions
    # =====================================================================
    print("\nRunning initial extractions...")
    for tool in ["usgs_earthquakes", "coingecko"]:
        r = api(base, "post",
                f"{B}/tool/{tool}/extract/execute".replace(base, ""),
                expect=(200, 202))
        print(f"  {tool}: {'started' if r is not None else 'FAIL'}")

    print("\nWaiting 30s for extractions to complete...")
    time.sleep(30)

    # Materialize entities
    print("\nMaterializing entities...")
    for entity in ["Earthquake", "CryptoAsset"]:
        r = api(base, "post",
                f"{B}/entity/{entity}/rematerialize".replace(base, ""),
                expect=(200, 202))
        print(f"  {entity}: {'started' if r is not None else 'FAIL'}")

    time.sleep(10)

    # Run mappers
    print("\nRunning mappers...")
    for entity in ["Earthquake", "CryptoAsset", "CryptoAssetPriceTickEvent"]:
        r = api(base, "post",
                f"{B}/entity/{entity}/mapper/execute".replace(base, ""),
                expect=(200, 202))
        print(f"  {entity}: {'started' if r is not None else 'FAIL'}")

    time.sleep(15)

    # Reconcile features
    print("\nReconciling features...")
    r = api(base, "post",
            f"{B}/feature/reconcile".replace(base, ""),
            expect=(200, 202))
    print(f"  CryptoAsset: {'started' if r is not None else 'FAIL'}")

    # Set as active model
    print("\nSetting default as active model...")
    r = api(base, "post",
            f"{B}/set-active".replace(base, ""),
            expect=(200, 202))
    print(f"  {'OK' if r is not None else 'FAIL'}")

    print("\nDone! Check the feed for earthquake + crypto data.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Configure the default model")
    parser.add_argument("--api-url", default="http://localhost:8000",
                        help="Base URL of the vizgrams API")
    args = parser.parse_args()
    configure(args.api_url)
