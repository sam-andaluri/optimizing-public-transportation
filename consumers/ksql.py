"""Configures KSQL to combine station and turnstile data"""
import json
import logging
import os

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = os.getenv("KSQL_URL", "http://localhost:8088")

KSQL_STATEMENTS = [
    """
CREATE STREAM turnstile_events
WITH (
    KAFKA_TOPIC='org.chicago.cta.station.turnstile.v1',
    VALUE_FORMAT='AVRO'
);
""",
    """
CREATE TABLE turnstile
WITH (
    KAFKA_TOPIC='TURNSTILE',
    VALUE_FORMAT='JSON'
) AS
    SELECT STATION_ID, CAST(COUNT(STATION_ID) AS INTEGER) AS count
    FROM turnstile_events
    GROUP BY STATION_ID
    EMIT CHANGES;
""",
    """
CREATE TABLE turnstile_summary
WITH (
    KAFKA_TOPIC='TURNSTILE_SUMMARY',
    VALUE_FORMAT='JSON'
) AS
    SELECT STATION_ID, CAST(COUNT(STATION_ID) AS INTEGER) AS count
    FROM turnstile_events
    GROUP BY STATION_ID
    EMIT CHANGES;
""",
]


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if (
        topic_check.topic_exists("TURNSTILE") is True
        and topic_check.topic_exists("TURNSTILE_SUMMARY") is True
    ):
        return

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
