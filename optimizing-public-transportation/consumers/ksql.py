"""Configures KSQL to combine station and turnstile data"""
import json
import logging
import requests
from requests.exceptions import HTTPError
import topic_check

logger = logging.getLogger(__name__)

KSQL_URL = "http://localhost:8088"

KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id INT,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC = 'org.chicago.cta.turnstile.v1',
    VALUE_FORMAT = 'avro',
    KEY = 'station_id'
);

CREATE TABLE TURNSTILE_SUMMARY
WITH (value_format='json') AS
SELECT station_id, COUNT(station_id) AS count
FROM turnstile
GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    try:
        resp.raise_for_status()
    except HTTPError as http_error:
        logger.error(f"Error -> {http_error.response.text}")
        raise http_error

if __name__ == "__main__":
    execute_statement()
