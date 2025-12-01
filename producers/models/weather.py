"""Methods pertaining to weather data"""
from enum import IntEnum
import json
import logging
from pathlib import Path
import random
import urllib.parse

import requests

from models.producer import Producer


logger = logging.getLogger(__name__)


class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    rest_proxy_url = "http://localhost:8082"

    key_schema = None
    value_schema = None

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month):

        super().__init__(
            topic_name="org.city.weather",
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
            num_partitions=2,
            num_replicas=1,
        )

        # initial temperature
        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

        # Load key schema
        if Weather.key_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                Weather.key_schema = json.load(f)

        # Load value schema
        if Weather.value_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                Weather.value_schema = json.load(f)

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        """Posts a weather event to Kafka via REST Proxy"""

        # update weather before sending
        self._set_weather(month)

        event = {
            "key_schema": json.dumps(Weather.key_schema),
            "value_schema": json.dumps(Weather.value_schema),
            "records": [
                {
                    "key": {"timestamp": int(self.time_millis())},
                    "value": {
                        "temperature": self.temp,
                        "status": self.status.name,  # IMPORTANT: string enum
                    },
                }
            ],
        }

        # Send to REST Proxy
        resp = requests.post(
            f"{Weather.rest_proxy_url}/topics/{self.topic_name}",
            headers={"Content-Type": "application/vnd.kafka.avro.v2+json"},
            data=json.dumps(event),
        )

        # Check if ok
        try:
            resp.raise_for_status()
            logger.info(
                "Weather event sent: temp=%s status=%s",
                self.temp,
                self.status.name,
            )
        except Exception as e:
            logger.error("❌ Failed to send weather event: %s", e)
            logger.error(resp.text)

        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )
