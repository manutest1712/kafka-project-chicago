"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging
import requests

# Configure logging to show INFO messages in console
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)

KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    # Check if the connector exists already
    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created; skipping recreation")
        return

    # Create connector config
    logger.info("Creating JDBC Source connector for stations...")

    config = {
        "name": CONNECTOR_NAME,
        "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",

            # JSON converters (schemas disabled)
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",

            # ---------------------------------------
            # JDBC connection details
            # ---------------------------------------
            "connection.url": "jdbc:postgresql://localhost:5432/cta",
            "connection.user": "root",
            "connection.password": "",

            # Table to ingest
            "table.whitelist": "stations",

            # Incrementing mode (only new rows pulled)
            "mode": "incrementing",
            "incrementing.column.name": "stop_id",

            # Topic naming
            "topic.prefix": "org.city.",

            # Polling interval (run rarely: 1 minute)
            "poll.interval.ms": "60000"
        }
    }

    # Create the connector
    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(config),
    )

    # Ensure a healthy response was given
    try:
        resp.raise_for_status()
        logger.info("JDBC Source connector created successfully")
    except Exception as e:
        logger.error(f"❌ Failed to create connector: {e}")
        logger.error(resp.text)


if __name__ == "__main__":
    configure_connector()
