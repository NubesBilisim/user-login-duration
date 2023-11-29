package com.nubes.streams;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;

public class LoginDurationDriver {
    public static void main(String[] args) throws IOException {
        final String bootstrapServers = args.length > 0 ? args[0] : "152.70.188.57:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://152.70.188.57:8081";
        produceInputs(bootstrapServers, schemaRegistryUrl);
        //consumeOutput(bootstrapServers, schemaRegistryUrl);

    }

    private static void produceInputs(final String bootstrapServers, final String schemaRegistryUrl) throws IOException {
        final String[] users = {"erica", "bob", "joe", "damian", "tania", "phil", "sam", "lauren", "joseph"};

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        String refreshFilePath = "config/refresh token.json";
        String configFilePath = "config/config.json";

        String refreshJson = new String(Files.readAllBytes(Paths.get(refreshFilePath)));
        String configJson = new String(Files.readAllBytes(Paths.get(configFilePath)));

        while(true) {
            Scanner in = new Scanner(System.in);
            System.out.println("Enter 1 for Config, 2 for Refresh : ");
            int choice = in.nextInt();
            System.out.println("Enter UserId : ");
            String userId = in.next();
            System.out.println("Enter interval in min :");
            int interval = in.nextInt();

            String msg = refreshJson;
            if (choice == 1) {
                msg = configJson;
            }

            msg = msg.replace("<userId>", userId);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.DATE_TIME_FORMAT)
                    .withZone(ZoneId.systemDefault());
            ZonedDateTime zdt = ZonedDateTime.of(LocalDate.now(), LocalTime.of(1, 0, 0), ZoneId.systemDefault());
            System.out.println("*** " + formatter.format(zdt.plus(interval, ChronoUnit.MINUTES)));
            msg = msg.replace("<timestamp>", formatter.format(zdt.plus(interval, ChronoUnit.MINUTES)));

            try (final KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                for (final String user : users) {
                    producer.send(new ProducerRecord<>(Constants.LOGIN_TOPIC, null, msg));
                    // For each user generate some page views
                }
            }
        }
    }

    private static void consumeOutput(final String bootstrapServers, final String schemaRegistryUrl) {
        final Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.Long().deserializer().getClass());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG,
                "pageview-region-lambda-example-consumer");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (final KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.subscribe(Collections.singleton(Constants.DURATION_TOPIC));
            while (true) {
                final ConsumerRecords<String, Long> consumerRecords = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                for (final ConsumerRecord<String, Long> consumerRecord : consumerRecords) {
                    System.out.println(consumerRecord.key() + ":" + consumerRecord.value());
                }
            }
        }
    }
}
