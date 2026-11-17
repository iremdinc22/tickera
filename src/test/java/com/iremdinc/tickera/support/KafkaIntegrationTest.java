package com.iremdinc.tickera.support;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Testler arasında paylaşılan tek bir Kafka Testcontainer'ı
 * sağlar.
 *
 * Container, Spring context oluşmadan önce başlatılır ve
 * topic açıkça oluşturulur. Böylece uygulama listener'ı
 * UNKNOWN_TOPIC_OR_PARTITION durumuna düşmeden topic'e
 * subscribe olabilir.
 */
public abstract class KafkaIntegrationTest
        extends PostgresIntegrationTest {

    protected static final String BOOKING_CREATED_TOPIC =
            "booking-created";

    private static final KafkaContainer KAFKA =
            startKafka();

    @DynamicPropertySource
    static void configureKafka(
            DynamicPropertyRegistry registry
    ) {

        registry.add(
                "spring.kafka.bootstrap-servers",
                KafkaIntegrationTest::bootstrapServers
        );
    }

    protected static String bootstrapServers() {

        return KAFKA.getBootstrapServers();
    }

    private static KafkaContainer startKafka() {

        KafkaContainer container =
                new KafkaContainer(
                        "apache/kafka:4.1.1"
                )
                        .withStartupTimeout(
                                Duration.ofMinutes(
                                        3
                                )
                        );

        container.start();

        createTopicIfMissing(
                container.getBootstrapServers(),
                BOOKING_CREATED_TOPIC
        );

        return container;
    }

    private static void createTopicIfMissing(
            String bootstrapServers,
            String topic
    ) {

        Properties properties =
                new Properties();

        properties.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        properties.put(
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                "30000"
        );

        properties.put(
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
                "30000"
        );

        try (
                AdminClient adminClient =
                        AdminClient.create(
                                properties
                        )
        ) {

            adminClient
                    .createTopics(
                            List.of(
                                    new NewTopic(
                                            topic,
                                            1,
                                            (short) 1
                                    )
                            )
                    )
                    .all()
                    .get(
                            30,
                            TimeUnit.SECONDS
                    );

        } catch (ExecutionException exception) {

            if (exception.getCause()
                    instanceof TopicExistsException) {

                return;
            }

            throw new IllegalStateException(
                    "Failed to create Kafka topic: "
                            + topic,
                    exception
            );

        } catch (InterruptedException exception) {

            Thread.currentThread()
                    .interrupt();

            throw new IllegalStateException(
                    "Interrupted while creating Kafka topic: "
                            + topic,
                    exception
            );

        } catch (Exception exception) {

            throw new IllegalStateException(
                    "Failed to create Kafka topic: "
                            + topic,
                    exception
            );
        }
    }
}
