"""Configures KSQL to combine station and turnstile data"""
import json
import logging
import requests
import topic_check

# -------------------------------
# Configure logging
# -------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Log everything DEBUG and above
handler = logging.StreamHandler()  # Log to console
formatter = logging.Formatter(
    '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
handler.setFormatter(formatter)
logger.addHandler(handler)

# -------------------------------
# KSQL server URL
# -------------------------------
KSQL_URL = "http://localhost:8088"

# -------------------------------
# KSQL statements
# -------------------------------
KSQL_STATEMENT = """
CREATE TABLE turnstile (
    timestamp BIGINT,
    station_id INTEGER,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC='org.city.turnstile',
    VALUE_FORMAT='AVRO',
    KEY='timestamp' 
);

CREATE TABLE turnstile_summary
WITH (VALUE_FORMAT='JSON') AS
    SELECT
        station_id,
        CAST(COUNT(*) AS INTEGER) AS count
    FROM turnstile
    GROUP BY station_id;
"""

# -------------------------------
# Execute KSQL statements
# -------------------------------
def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    try:
        if topic_check.topic_exists("TURNSTILE_SUMMARY"):
            logger.info("turnstile_summary topic already exists. Skipping KSQL execution.")
            return

        logger.info("Executing KSQL statements...")

        resp = requests.post(
            f"{KSQL_URL}/ksql",
            headers={"Content-Type": "application/vnd.ksql.v1+json"},
            data=json.dumps(
                {
                    "ksql": KSQL_STATEMENT,
                    "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
                }
            ),
            timeout=30
        )
        
         # Log raw server response BEFORE raising error
        logger.error("KSQL server response:\n%s", resp.text)

        # Raise exception for non-2xx responses
        resp.raise_for_status()
        logger.info(f"KSQL executed successfully. Response:\n{resp.text}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error executing KSQL statements: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

# -------------------------------
# Run
# -------------------------------
if __name__ == "__main__":
    execute_statement()
