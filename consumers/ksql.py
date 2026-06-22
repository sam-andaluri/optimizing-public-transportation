"""Configures KSQL to combine station and turnstile data"""
import json
import logging
import os

import requests


logger = logging.getLogger(__name__)


KSQL_URL = os.getenv("KSQL_URL", "http://localhost:8088")

KSQL_STATEMENTS = [
    """
CREATE TABLE turnstile (
    station_id INTEGER PRIMARY KEY,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC='org.chicago.cta.station.turnstile.v1',
    KEY_FORMAT='AVRO',
    VALUE_FORMAT='AVRO'
);
""",
    """
CREATE TABLE turnstile_summary
WITH (
    KAFKA_TOPIC='TURNSTILE_SUMMARY',
    VALUE_FORMAT='JSON'
) AS
    SELECT station_id, AS_VALUE(station_id) AS station_id_value, COUNT(station_id) AS count
    FROM turnstile
    GROUP BY station_id;
""",
]


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    logging.debug("executing ksql statement...")

    for statement in KSQL_STATEMENTS:
        resp = requests.post(
            f"{KSQL_URL}/ksql",
            headers={"Content-Type": "application/vnd.ksql.v1+json"},
            data=json.dumps(
                {
                    "ksql": statement,
                    "streamsProperties": {
                        "ksql.streams.auto.offset.reset": "earliest"
                    },
                }
            ),
        )

        if resp.status_code >= 400:
            response_text = resp.text.lower()
            if "already exists" in response_text:
                logger.info("KSQL object already exists, continuing")
                continue
            logger.error("KSQL request failed: %s", resp.text)
            resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()
