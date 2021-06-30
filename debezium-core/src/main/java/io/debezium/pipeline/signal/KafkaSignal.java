/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Collect;
import io.debezium.util.Threads;

public class KafkaSignal<T extends DataCollectionId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSignal.class);

    private final ExecutorService signalTopicListenerExecutor;
    private final Configuration consumerConfig;
    private final String topicName;
    private final String connectorName;
    private final Duration pollInterval;
    private final int maxRecoveryAttempts;
    private final Map<String, Signal.Action> signalActions = new HashMap<>();

    public static final String CONFIGURATION_FIELD_PREFIX_STRING = "signal.";
    private static final String CONSUMER_PREFIX = CONFIGURATION_FIELD_PREFIX_STRING + "consumer.";

    public static final Field SIGNAL_TOPIC = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "kafka.topic")
            .withDisplayName("Signal topic name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The name of the topic for the signals to the connector")
            .withValidation(Field::isRequired);

    public static final Field BOOTSTRAP_SERVERS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "kafka.bootstrap.servers")
            .withDisplayName("Kafka broker addresses")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("A list of host/port pairs that the connector will use for establishing the initial "
                    + "connection to the Kafka cluster for retrieving signals to the connector."
                    + "This should point to the same Kafka cluster used by the Kafka Connect process.")
            .withValidation(Field::isRequired);

    public static final Field SIGNAL_POLL_INTERVAL_MS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING
            + "signal.kafka.poll.interval.ms")
            .withDisplayName("Poll interval for kafka signals (ms)")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The number of milliseconds to wait while polling signals.")
            .withDefault(5000)
            .withValidation(Field::isNonNegativeInteger);

    public static final Field MAX_RECOVERY_ATTEMPTS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "kafka.recovery.attempts")
            .withDisplayName("Max attempts to read from signals topic in case of an error")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The number of attempts in a row to retry in case of an error reading from signals topic")
            .withDefault(100)
            .withValidation(Field::isInteger);

    public KafkaSignal(Class<? extends SourceConnector> connectorType, CommonConnectorConfig connectorConfig, EventDispatcher<T> dispatcher) {
        String signalName = "kafka-signal";
        connectorName = connectorConfig.getLogicalName();
        signalTopicListenerExecutor = Threads.newSingleThreadExecutor(connectorType, connectorName, signalName, true);
        Configuration signalConfig = connectorConfig.getConfig().subset(CONFIGURATION_FIELD_PREFIX_STRING, false)
                .edit()
                .withDefault(KafkaSignal.SIGNAL_TOPIC, connectorName + "-signal")
                .build();
        signalActions.put(ExecuteSnapshot.NAME, new ExecuteSnapshot(dispatcher));
        this.topicName = signalConfig.getString(SIGNAL_TOPIC);
        this.maxRecoveryAttempts = signalConfig.getInteger(MAX_RECOVERY_ATTEMPTS);
        this.pollInterval = Duration.ofMillis(signalConfig.getInteger(SIGNAL_POLL_INTERVAL_MS));
        String bootstrapServers = signalConfig.getString(BOOTSTRAP_SERVERS);
        this.consumerConfig = signalConfig.subset(CONSUMER_PREFIX, true).edit()
                .withDefault(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                .withDefault(ConsumerConfig.CLIENT_ID_CONFIG, signalName)
                .withDefault(ConsumerConfig.GROUP_ID_CONFIG, signalName)
                .withDefault(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1) // get even smallest message
                .withDefault(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
                .withDefault(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000) // readjusted since 0.10.1.0
                .withDefault(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        OffsetResetStrategy.EARLIEST.toString().toLowerCase())
                .withDefault(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .withDefault(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .build();
    }

    public void start() {
        signalTopicListenerExecutor.submit(this::monitorSignals);
    }

    private void monitorSignals() {
        int recoveryAttempts = 0;
        while (recoveryAttempts < maxRecoveryAttempts) {
            try (KafkaConsumer<String, String> signalsConsumer = new KafkaConsumer<>(consumerConfig.asProperties())) {
                // Subscribe to the only partition for this topic, and seek to the beginning of that partition ...
                LOGGER.debug("Subscribing to signals topic '{}'", topicName);
                signalsConsumer.subscribe(Collect.arrayListOf(topicName));
                while (true) {
                    // DBZ-1361 not using poll(Duration) to keep compatibility with AK 1.x
                    ConsumerRecords<String, String> recoveredRecords = signalsConsumer.poll(this.pollInterval.toMillis());
                    for (ConsumerRecord<String, String> record : recoveredRecords) {
                        try {
                            sendSignal(record);
                            signalsConsumer.commitSync();
                            recoveryAttempts = 0;
                        }
                        catch (final InterruptedException e) {
                            LOGGER.error("Signals processing was interrupted", e);
                            return;
                        }
                        catch (final Exception e) {
                            LOGGER.error("Skipped signal due to an error '{}'", record, e);
                            signalsConsumer.commitSync();
                        }
                    }
                }
            }
            catch (final Exception e) {
                recoveryAttempts++;
                LOGGER.error("Signals processing the error, retried {} times of {}", recoveryAttempts, maxRecoveryAttempts, e);
            }
        }
    }

    private void sendSignal(ConsumerRecord<String, String> record) throws IOException, InterruptedException {
        if (!connectorName.equals(record.key())) {
            LOGGER.info("Signal key '{}' doesn't match the connector's name '{}'", record.key(), connectorName);
            return;
        }
        String value = record.value();
        LOGGER.trace("Processing signal: {}", value);
        final Document jsonData = (value == null || value.isEmpty()) ? Document.create()
                : DocumentReader.defaultReader().read(value);
        String id = Long.toString(record.offset());
        String type = jsonData.getString("type");
        Document data = jsonData.getDocument("data");
        Signal.Payload signalPayload = new Signal.Payload(id, type, data, null, null);
        Signal.Action action = signalActions.get(type);
        action.arrived(signalPayload);
    }
}
