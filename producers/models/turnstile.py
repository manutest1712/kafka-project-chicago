"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware

logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    # Load the value schema
    value_schema = avro.load(
        f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""

        # Normalize and sanitize station name for topic name usage
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        #
        # Filled per station topic name
        #
        topic_name = f"org.city.turnstile.{station_name}"

        super().__init__(
            topic_name,
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=1,      # Usually 1 per station is fine
            num_replicas=1         # Single-node dev Kafka generally uses 1 replica
        )

        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)
        
    def to_epoch_millis(self, ts):
        if isinstance(ts, int):
            return ts
        if isinstance(ts, float):
            return int(ts)
        if hasattr(ts, "timestamp"):   # datetime
            return int(ts.timestamp() * 1000)
        raise TypeError(f"Unsupported timestamp type: {type(ts)}")

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""

        logger.info(f"Producing turnstile event for station {self.station.name} at time {timestamp}")
        
        # Number of simulated entries during this interval
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)

        #
        # Emit ONE message per rider entry
        #
        for _ in range(num_entries):
            try:
                self.producer.produce(
                    topic=self.topic_name,
                    key={"timestamp": self.to_epoch_millis(timestamp)},
                    value={
                        "station_id": self.station.station_id,
                        "station_name": self.station.name,
                        "line": self.station.color.name,
                    },
                )

                logger.info(
                    f"Produced turnstile event for station {self.station.name} at {timestamp}"
                )

            except Exception as e:
                logger.error(
                    f"❌ Failed to produce turnstile event for station {self.station.name}: {e}"
                )
                raise   # <-- rethrow exception to break the loop & surface the error
