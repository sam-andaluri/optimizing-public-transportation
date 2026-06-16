"""Methods pertaining to weather data"""
from enum import IntEnum
import json
import logging
import os
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

    rest_proxy_url = os.getenv("KAFKA_REST_URL", "http://localhost:8082")

    key_schema = None
    value_schema = None

    winter_months = set((1, 2, 3, 10, 11, 12))
    summer_months = set((6, 7, 8))

    @staticmethod
    def temperature_bounds(month):
        if month in Weather.winter_months:
            return 0.0, 55.0
        if month in Weather.summer_months:
            return 55.0, 100.0
        return 30.0, 85.0

    def __init__(self, month):
        if Weather.key_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                Weather.key_schema = json.load(f)

        if Weather.value_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                Weather.value_schema = json.load(f)

        super().__init__(
            "org.chicago.cta.weather.v1",
            key_schema=None,
            value_schema=None,
            num_partitions=1,
            num_replicas=1,
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        low, high = Weather.temperature_bounds(month)
        delta = max(-20.0, min(20.0, random.triangular(-10.0, 10.0, mode)))
        self.temp = max(low, min(high, self.temp + delta))
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)

        encoded_topic = urllib.parse.quote(self.topic_name, safe="")
        resp = requests.post(
            f"{Weather.rest_proxy_url}/topics/{encoded_topic}",
            headers={
                "Content-Type": "application/vnd.kafka.avro.v2+json",
                "Accept": "application/vnd.kafka.v2+json",
            },
            data=json.dumps(
                {
                    "key_schema": json.dumps(Weather.key_schema),
                    "value_schema": json.dumps(Weather.value_schema),
                    "records": [
                        {
                            "key": {"timestamp": self.time_millis()},
                            "value": {
                                "temperature": self.temp,
                                "status": self.status.name,
                            },
                        }
                    ],
                }
            ),
        )
        resp.raise_for_status()

        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )
