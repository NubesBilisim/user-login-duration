package com.nubes.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nubes.streams.schema.avro.UserWithLoginDuration;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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

    public static void main(String[] args){
        new UserLoginDuration().run(args);
    }

    private void run(String[] args) {
        log.debug("UserLoginDuration stream is starting.");

        final String propFile = args.length > 0 ? args[0] : "configuration/dev.properties";

        Properties envProperties = loadEnvProperties(propFile);
        final Properties streamProperties = buildStreamProperties(envProperties);
        Topology topology = buildTopology(envProperties);

        final KafkaStreams kafkaStreams = new KafkaStreams(topology, streamProperties);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("login-duration-streams-shutdown-hook"){
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
        }catch (Throwable e){
            log.error("Exception ", e);
            System.exit(1);
        }
        System.exit(0);
    }

    private static Properties loadEnvProperties(String propFile) {
        Properties props = new Properties();
        try(InputStream input = new FileInputStream(propFile)) {
            props.load(input);
        }
        catch(IOException e)
        {
            throw new RuntimeException("Unable to find the specified properties file");
        }
        return props;
    }

    protected Properties buildStreamProperties(Properties envProps) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, envProps.getProperty("client.id"));
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        streamsConfiguration.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, envProps.getProperty("default.topic.replication.factor"));
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, envProps.getProperty("offset.reset.policy"));  // TODO: 11/20/2023 check for meaning
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, envProps.getProperty("commit.interval.ms.config"));
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, envProps.getProperty("state.dir.config"));
        //streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        return streamsConfiguration;
    }

    private static Topology buildTopology(final Properties properties){

        final SpecificAvroSerde<UserWithLoginDuration> userWithLoginDurationSerde = getUserWithLoginDurationSerde(properties);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> loginRecords = builder.stream(Constants.LOGIN_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        getUserLoginDuration(loginRecords,userWithLoginDurationSerde);

        return builder.build();
    }

    protected static void getUserLoginDuration(KStream<String, String> loginRecords, SpecificAvroSerde<UserWithLoginDuration> userWithLoginDurationSerde) {
        KGroupedStream<String, String> loginByUserId = loginRecords
                .map((k,v)-> {
                    String userId = UserLoginDuration.getUserIdFromJson(v);
                    return new KeyValue<>(userId, v);
                })
                .groupByKey();

        loginByUserId
               .aggregate(() -> new UserWithLoginDuration(),
                        (key, value, aggregate) -> {
                            if(UserLoginDuration.isRefreshToken(value)){
                                if(aggregate.getLoginTime() == null){
                                   log.warn(key + " : Login time could not be found! ");
                                    aggregate.setLoginTime(UserLoginDuration.getTimeFromJson(value));
                                    aggregate.setDuration(0L);
                                }else {
                                    Instant refreshTime = UserLoginDuration.getTimeFromJson(value);
                                    Instant loginTime = aggregate.getLoginTime();
                                    long duration = refreshTime.toEpochMilli() - loginTime.toEpochMilli();
                                    aggregate.setDuration(duration);
                                }
                            }else{
                                aggregate.setLoginTime(UserLoginDuration.getTimeFromJson(value));
                                aggregate.setDuration(0L);
                            }
                            return aggregate;
                        },
                        Materialized.with(Serdes.String(), userWithLoginDurationSerde)
                        )
                .toStream()
                .mapValues(userDuration -> userDuration.getDuration())
                .peek((k, v) -> System.out.println(k + " : " + v))
                .peek((k,v) -> Calculate.callStatic())
                .peek((k,v) -> new Calculate().callPublic())
                .to(Constants.DURATION_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }

    public static SpecificAvroSerde<UserWithLoginDuration> getUserWithLoginDurationSerde(Properties properties) {
        final SpecificAvroSerde<UserWithLoginDuration> userWithLoginDurationSerde = new SpecificAvroSerde<>();
        userWithLoginDurationSerde.configure(getSerdeConfig(properties), false);
        return userWithLoginDurationSerde;
    }

    protected static Map<String, String> getSerdeConfig(Properties config) {
        final HashMap<String, String> map = new HashMap<>();

        final String srUrlConfig = config.getProperty(SCHEMA_REGISTRY_URL_CONFIG);
        map.put(SCHEMA_REGISTRY_URL_CONFIG, ofNullable(srUrlConfig).orElse(""));
        return map;
    }

    private static String getUserIdFromJson(String json) {

        Map source = getSourceFromJson(json);

        if (source != null) {
            Map properties = (Map) source.get("Properties");

            if (properties != null) {
                String userId = (String) properties.get("UserId");
                return userId;
            }
        }
        return null;
    }

    private static Instant getTimeFromJson(String json) {

        Map source = getSourceFromJson(json);

        if (source != null) {

            String timestamp = (String) source.get("Timestamp");
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.DATE_TIME_FORMAT);
            ZonedDateTime zdt = ZonedDateTime.parse(timestamp, formatter);
            return zdt.toInstant();

        }
        return null;
    }

    private static boolean isRefreshToken(String json) {

        Map source = getSourceFromJson(json);

        if (source != null) {
            String msgTemplate = (String) source.get("MessageTemplate");

            if (msgTemplate != null) {
                /*String refreshToken = (String) msgTemplate.get("refresh_token");
                if(refreshToken != null)
                    return true;*/
                return msgTemplate.contains("refresh_token");
            }
        }
        return false;
    }

    private static Map getSourceFromJson(String json) {
        ObjectMapper mapper = new ObjectMapper();

        Map jsonMap = new HashMap<>();

        try {
            jsonMap = mapper.readValue(json, Map.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        Map source = (Map) jsonMap.get("_source");
        return source;
    }
}
