package com.nubes.streams;


import com.nubes.streams.schema.avro.UserWithLoginDuration;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertNotNull;

public class UserLoginDurationTest {

    private TopologyTestDriver testDriver;

    private SpecificAvroSerde<UserWithLoginDuration> ratingSpecificAvroSerde;

    @Before
    public void setUp() {

        final Properties mockProps = new Properties();
        mockProps.put("application.id", "user-login-duration");
        mockProps.put("client.id", "user-login-duration-client");
        mockProps.put("bootstrap.servers", "DUMMY_KAFKA_9092");
        mockProps.put("schema.registry.url", "mock://DUMMY_SR_8080");
        mockProps.put("default.topic.replication.factor", "1");
        mockProps.put("offset.reset.policy", "earliest");
        mockProps.put("commit.interval.ms.config", "10000");
        mockProps.put("state.dir.config", "/tmp/kafka-streams");

        final UserLoginDuration userLoginDuration = new UserLoginDuration();
        final Properties streamProps = userLoginDuration.buildStreamProperties(mockProps);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        final SpecificAvroSerde<UserWithLoginDuration> userWithLoginDurationSerde = UserLoginDuration.getUserWithLoginDurationSerde(mockProps);

        final KStream<String, String> loginRecords = streamsBuilder.stream(Constants.LOGIN_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        UserLoginDuration.getUserLoginDuration(loginRecords, userWithLoginDurationSerde);

        final Topology topology = streamsBuilder.build();
        testDriver = new TopologyTestDriver(topology, streamProps);
    }

    @Test
    public void validateIfTestDriverCreated() {
        assertNotNull(testDriver);
    }

    @Test
    public void validateLoginDuration() throws IOException {
        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(Constants.LOGIN_TOPIC,
                new StringSerializer(),
                new StringSerializer());

        List<String> msgs = prepareLoginRecord("John", 1);

        inputTopic.pipeKeyValueList(asList(
                new KeyValue<String, String>("", msgs.get(0)),
                new KeyValue<String, String>("", msgs.get(1))));

        final TestOutputTopic<String, Long> outputTopic = testDriver.createOutputTopic(Constants.DURATION_TOPIC,
                new StringDeserializer(),
                new LongDeserializer());

        final List<KeyValue<String, Long>> keyValues = outputTopic.readKeyValuesToList();
        final KeyValue<String, Long> keyValue = keyValues.get(1);
        MatcherAssert.assertThat(keyValue, equalTo(new KeyValue<>("John", 60000L)));
/*
        final KeyValueStore<String, Long>
                keyValueStore =
                testDriver.getKeyValueStore(Constants.DURATION_TOPIC);
        final Long expected = keyValueStore.get("John");
        System.out.println("expected : " + expected);
        Assert.assertEquals("Message", expected, 60000L, 0.0);

 */
    }

    private static List<String> prepareLoginRecord(String userId, int refreshTokenInterval) throws IOException {
        String refreshFilePath = "config/refresh token.json";
        String configFilePath = "config/config.json";

        String refreshJson = new String(Files.readAllBytes(Paths.get(refreshFilePath)));
        String configJson = new String(Files.readAllBytes(Paths.get(configFilePath)));

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.DATE_TIME_FORMAT)
                .withZone(ZoneId.systemDefault());
        ZonedDateTime zdt = ZonedDateTime.of(LocalDate.now(), LocalTime.of(1, 0, 0), ZoneId.systemDefault());

        String loginMsg = configJson.replace("<userId>", userId).replace("<timestamp>", formatter.format(zdt));
        String refreshMsg = refreshJson.replace("<userId>", userId).replace("<timestamp>", formatter.format(zdt.plus(refreshTokenInterval, ChronoUnit.MINUTES)));
        return asList(loginMsg, refreshMsg);
    }

    @After
    public void tearDown() {
        testDriver.close();
    }
}
