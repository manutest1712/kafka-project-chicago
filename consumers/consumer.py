"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        #
        # Broker Configuration
        #
        self.broker_properties = {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"group_{topic_name_pattern}", # Unique group ID per topic pattern
            "auto.offset.reset": "earliest" if offset_earliest else "latest",
            #"enable.auto.commit": False,
        }

        # Create Consumer: Avro or normal
        if is_avro is True:
            logger.info(f"Creating avro consumer for topic {topic_name_pattern}")
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            logger.info(f"Creating normal consumer for topic {topic_name_pattern}")
            self.consumer = Consumer(self.broker_properties)
            pass

        #
        # Subscribe with on_assign callback
        #
        logger.info(f"subscribing for topic {topic_name_pattern}")
        self.consumer.subscribe([topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""

        logger.info(f"on_assign invoked for {self.topic_name_pattern}")
        
         if self.offset_earliest:
            for partition in partitions:
                logger.info(
                    f"Setting offset to earliest for partition {partition.partition}"
                )
                partition.offset = confluent_kafka.OFFSET_BEGINNING


        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        
        try:
            msg = self.consumer.poll(self.consume_timeout)
        except SerializerError as e:
            logger.error(f"Deserialization error: {e}")
            return 0
            
        except Exception as e:
            logger.error(f"Unexpected error while polling Kafka: {e}")
            return 0

        if msg is None:
            logger.info("Message not received")
            return 0

        if msg.error():
            # PARTITION_EOF is not a crash-worthy error, it just means we reached the end of the log
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logger.debug(f"End of partition reached for {msg.topic()}")
            else:
                logger.error(f"Kafka error: {msg.error()}")
            return 0

        # Successful message
        try:
            self.message_handler(msg)
            return 1
        except Exception as e:
            logger.exception(f"Error processing message: {e}")

        return 0


    def close(self):
        """Cleans up the consumer"""
          
        if self.consumer is not None:
            logger.info("Closing consumer for topic pattern %s", self.topic_name_pattern)
            try:
                self.consumer.close()
                logger.info(f"Closed consumer for topic pattern {self.topic_name_pattern}")
            except Exception as e:
                logger.error(f"Error closing consumer: {e}")
        else:
            logger.error("Not closing as consumer is not created for %s", self.topic_name_pattern)