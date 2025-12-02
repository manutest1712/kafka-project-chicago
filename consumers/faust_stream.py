"""Defines trends calculations for stations"""
import logging
import faust

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record, serializer="json"):
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
class TransformedStation(faust.Record, serializer="json"):
    station_id: int = 0
    station_name: str = ""
    order: int = 0
    line: str = ""


#
# Faust App
#
app = faust.App(
    "stations-stream",
    broker="kafka://localhost:9092",
    store="memory://",
)

#
# Input topic from Kafka Connect
# Kafka Connect output topic = topic.prefix + table name
# In your config: topic.prefix = "org.city."
# So stations table outputs to: org.city.stations
#
stations_topic = app.topic("org.city.stations", value_type=Station)

#
# Output topic
#
out_topic = app.topic("org.city.stations.transformed", partitions=1, value_type=TransformedStation)

#
# Table to store latest station info (not required for logic but required by assignment)
#
table = app.Table(
    "stations_transformed",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)

# -------------------
# Helper to determine line color
# -------------------
def resolve_line(station: Station) -> str:
    if station.red:
        return "red"
    if station.blue:
        return "blue"
    if station.green:
        return "green"
    return "unknown"

#
# Stream processor
#
@app.agent(stations_topic)
async def transform_station(stations):
    async for station in stations:
        print("in method")

        transformed = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=resolve_line(station),
        )

        key = str(transformed.station_id)
        # Write to table (optional but part of assignment structure)
        table[key] = transformed

        # Produce to output topic
        await out_topic.send(key=key, value=transformed)

        logger.info(f"Transformed station: {transformed}")
        print(f"Transformed station: {transformed}")


if __name__ == "__main__":
    app.main()
