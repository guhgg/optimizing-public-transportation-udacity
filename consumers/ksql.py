import json
import logging

import requests
import topic_check

logger = logging.getLogget(__name__)

KSQL_URL = "http://localhost:8088"

KSQL_STATMENT = """CREATE TABLE turnstile(
    station_id INT,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    kafka_topic = 'org.chicago.cta.turnstile.v1',
    value_format = 'avro',
    key = 'station_id'
);

CREATE TABLE TURNSTILE_SUMMARY
WITH (value_format = 'json') AS
SELECT station_id, COUNT(station_id) AS count
FROM turnstile
GROUP BY station_id;
"""

def execute_statement():
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    resp.raise_for_status()

if __name__ == "__main__":
    execute_statement()