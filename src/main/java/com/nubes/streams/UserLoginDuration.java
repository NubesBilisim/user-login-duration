package com.nubes.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nubes.streams.schema.avro.UserLoginRecord;
import com.nubes.streams.schema.avro.UserWithLoginDuration;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.util.Optional.ofNullable;

public class UserLoginDuration {
    private static final Logger log = LoggerFactory.getLogger(UserLoginDuration.class);

    public static Properties envProps = null;

    public static void main(String[] args) {
        new UserLoginDuration().run(args);
    }

    private void run(String[] args) {
        log.debug("UserLoginDuration stream is starting.");

        final String propFile = args.length > 0 ? args[0] : "config/dev.properties";

        envProps = loadEnvProperties(propFile);
        final Properties streamProperties = buildStreamProperties();
        Topology topology = buildTopology();

        final KafkaStreams kafkaStreams = new KafkaStreams(topology, streamProperties);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("login-duration-streams-shutdown-hook") {
            @Override
            public void run() {
                log.warn("UserLoginDuration stream is closing");
                kafkaStreams.close(Duration.ofSeconds(5));
                latch.countDown();
                log.warn("UserLoginDuration stream is closed");
            }
        });

        try {
            log.debug("UserLoginDuration stream is ready to read from " + Constants.LOGIN_TOPIC + ".");
            kafkaStreams.cleanUp();
            kafkaStreams.start();
            latch.await();
        } catch (Throwable e) {
            log.error("Exception ", e);
            System.exit(1);
        }
        System.exit(0);
    }


    private static Properties loadEnvProperties(String propFile) {
        Properties props = new Properties();
        try (InputStream input = new FileInputStream(propFile)) {
            props.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Unable to find the specified properties file");
        }
        return props;
    }

    protected Properties buildStreamProperties() {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, envProps.getProperty("client.id"));
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        streamsConfiguration.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, envProps.getProperty("default.topic.replication.factor"));
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, envProps.getProperty("offset.reset.policy"));  // TODO: 11/20/2023 check for meaning
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, envProps.getProperty("commit.interval.ms.config"));
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, envProps.getProperty("state.dir.config"));
        //streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        return streamsConfiguration;
    }

    private static Topology buildTopology() {

        final SpecificAvroSerde<UserWithLoginDuration> userWithLoginDurationSerde = getUserWithLoginDurationSerde();

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> loginRecords = builder.stream(Constants.LOGIN_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        getUserLoginDuration(loginRecords, userWithLoginDurationSerde);

        return builder.build();
    }

    protected static void getUserLoginDuration(KStream<String, String> loginRecords, SpecificAvroSerde<UserWithLoginDuration> userWithLoginDurationSerde) {
        KGroupedStream<Long, UserLoginRecord> loginByUserId = loginRecords
                .map((k, v) -> {
                    UserLoginRecord loginRecord = generateUserLoginRecord(v);
                    return new KeyValue<>(loginRecord.getUserId(), loginRecord);
                })
                .filter((k, v) -> v.getIsLoginConfig() || v.getIsRefreshToken())
                .groupByKey();

        loginByUserId
                .aggregate(() -> new UserWithLoginDuration(),
                        (key, value, aggregate) -> {
                            aggregate.setUsername(value.getUsername());
                            aggregate.setUserId(value.getUserId());
                            if (value.getIsRefreshToken()) {
                                int refreshTokenInterval = Integer.parseInt(envProps.getProperty(Constants.REFRESH_TOKEN_INTERVAL_KEY));
                                if (aggregate.getConfigLogReadTime() == null) {
                                    log.warn(key + " : Login time could not be found! Seems that no config log was received.");
                                    aggregate.setConfigLogReadTime(Instant.now());
                                    aggregate.setUsername(value.getUsername());
                                    aggregate.setUserId(value.getUserId());
                                    aggregate.setRefreshTokenCount(1);
                                    aggregate.setDuration(refreshTokenInterval);
                                } else {
                                    int refreshTokenCount = aggregate.getRefreshTokenCount() + 1;
                                    aggregate.setRefreshTokenCount(refreshTokenCount);
                                    aggregate.setDuration(refreshTokenCount * refreshTokenInterval);
                                }
                            } else {
                                aggregate.setUsername(value.getUsername());
                                aggregate.setUserId(value.getUserId());
                                aggregate.setConfigLogReadTime(Instant.now());
                                aggregate.setRefreshTokenCount(0);
                                aggregate.setDuration(0L);
                            }
                            return aggregate;
                        },
                        Materialized.with(Serdes.Long(), userWithLoginDurationSerde)
                )
                .toStream()
                .peek((k, v) -> log.debug(k + " : " + v))
                .filter((k, v) -> v.getDuration() > 0L)
                .mapValues(v -> convertToJson(v))
                .to(Constants.DURATION_TOPIC, Produced.with(Serdes.Long(), Serdes.String()));
    }

    private static String convertToJson(UserWithLoginDuration record) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DatumWriter<IndexedRecord> writer = new SpecificDatumWriter<>(record.getClassSchema());
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getClassSchema(), baos, false);
            writer.write(record, encoder);
            encoder.flush();
            baos.flush();
            return baos.toString(StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.error("Exception occured : ", e);
        }
        return "";
    }


    public static SpecificAvroSerde<UserWithLoginDuration> getUserWithLoginDurationSerde() {
        final SpecificAvroSerde<UserWithLoginDuration> userWithLoginDurationSerde = new SpecificAvroSerde<>();
        userWithLoginDurationSerde.configure(getSerdeConfig(), false);
        return userWithLoginDurationSerde;
    }

    protected static Map<String, String> getSerdeConfig() {
        final HashMap<String, String> map = new HashMap<>();

        final String srUrlConfig = envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG);
        map.put(SCHEMA_REGISTRY_URL_CONFIG, ofNullable(srUrlConfig).orElse(""));
        return map;
    }


    private static UserLoginRecord generateUserLoginRecord(String json) {
        UserLoginRecord loginRecord = new UserLoginRecord();

        long userId = 0;
        String username = null;
        String logLevel = null;
        String grantType = null;
        String path = null;

        Map properties = getPropertiesFromJson(json);

        if (properties != null) {
            userId = ((Number) properties.get(Constants.JSON_USERID_KEY)).longValue();
            username = (String) properties.get(Constants.JSON_USERNAME_KEY);
            path = (String) properties.get(Constants.JSON_PATH_KEY);
            logLevel = (String) properties.get(Constants.JSON_LOG_LEVEL_KEY);
            grantType = (String) properties.get(Constants.JSON_GRANT_TYPE_KEY);

        }

        String refreshTokenPath = envProps.getProperty(Constants.REFRESH_TOKEN_PATH_KEY, Constants.REFRESH_TOKEN_PATH);
        String configLogPath = envProps.getProperty(Constants.CONFIG_LOG_PATH_KEY, Constants.CONFIG_LOG_PATH);

        boolean isRefreshToken = Constants.REFRESH_TOKEN.equals(grantType)
                && refreshTokenPath.equals(path);

        boolean isConfigLog = configLogPath.equals(path)
                && Constants.LOG_LEVEL.RESPONSE.getName().equals(logLevel);

        loginRecord.setUserId(userId);
        loginRecord.setUsername(username);
        loginRecord.setIsRefreshToken(isRefreshToken);
        loginRecord.setIsLoginConfig(isConfigLog);

        log.debug("The record read from " + Constants.LOGIN_TOPIC + " contains userId : " + userId + ", Config Log : " + isConfigLog + ", Refresh Token : " + isRefreshToken);

        return loginRecord;
    }
    private static Map getPropertiesFromJson(String json) {
        ObjectMapper mapper = new ObjectMapper();

        Map jsonMap = new HashMap<>();

        try {
            jsonMap = mapper.readValue(json, Map.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        Map source = (Map) jsonMap.get(Constants.JSON_PROPERTIES_KEY);
        return source;
    }
}
