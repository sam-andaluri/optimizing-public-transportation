"""Defines trends calculations for stations"""
import logging
import os

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


FAUST_BROKER_URL = os.getenv("FAUST_BROKER_URL", "kafka://localhost:9092")

app = faust.App(
    "stations-stream",
    broker=FAUST_BROKER_URL,
    store="memory://",
    consumer_auto_offset_reset="earliest",
)
topic = app.topic("org.chicago.cta.stations", value_type=Station)
out_topic = app.topic(
    "org.chicago.cta.stations.table.v1",
    value_type=TransformedStation,
    partitions=1,
)
table = app.Table(
    "transformed_stations",
    default=lambda: None,
    partitions=1,
    changelog_topic=out_topic,
)


def line_for_station(station):
    """Returns the CTA line represented by a Kafka Connect station row."""
    if station.red:
        return "red"
    if station.blue:
        return "blue"
    if station.green:
        return "green"
    return "unknown"


@app.agent(topic)
async def transform_stations(stations):
    async for station in stations:
        line_color = line_for_station(station)
        transformed_station = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line_color,
        )
        table[station.station_id] = transformed_station
        await out_topic.send(key=str(station.station_id), value=transformed_station)


if __name__ == "__main__":
    app.main()
