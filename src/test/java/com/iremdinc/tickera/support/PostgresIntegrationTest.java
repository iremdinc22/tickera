package com.iremdinc.tickera.support;

import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Duration;

/**
 * PostgreSQL Testcontainer'ı sağlar ve testlerin geliştirici
 * ortamındaki Kafka/Redis servislerine bağlanmasını engeller.
 *
 * Kafka veya Redis'e ihtiyaç duyan testler kendi
 * Testcontainer tabanlı destek sınıflarını kullanır ve
 * ilgili property'leri {@link DynamicPropertySource} ile
 * override eder.
 */
@Testcontainers
@DirtiesContext(
        classMode = DirtiesContext.ClassMode.AFTER_CLASS
)
@TestPropertySource(
        properties = {
                /*
                 * Geliştirici Kafka broker'ına bağlanma.
                 */
                "spring.kafka.bootstrap-servers=127.0.0.1:1",
                "spring.kafka.listener.auto-startup=false",

                /*
                 * Geliştirici Redis'ine bağlanma.
                 */
                "spring.data.redis.host=127.0.0.1",
                "spring.data.redis.port=1",

                /*
                 * Scheduler'ı test süresince etkisizleştir.
                 */
                "tickera.outbox.publish-initial-delay-ms=3600000",
                "tickera.outbox.publish-interval-ms=3600000",
                "tickera.outbox.stale-recovery-initial-delay-ms=3600000",
                "tickera.outbox.stale-recovery-interval-ms=3600000"
        }
)
public abstract class PostgresIntegrationTest {

    @Container
    protected static final PostgreSQLContainer postgres =
            new PostgreSQLContainer("postgres:17")
                    .withDatabaseName("tickera_test")
                    .withUsername("test")
                    .withPassword("test")
                    .withStartupTimeout(
                            Duration.ofMinutes(2)
                    );

    @DynamicPropertySource
    static void configureProperties(
            DynamicPropertyRegistry registry
    ) {

        registry.add(
                "spring.datasource.url",
                postgres::getJdbcUrl
        );

        registry.add(
                "spring.datasource.username",
                postgres::getUsername
        );

        registry.add(
                "spring.datasource.password",
                postgres::getPassword
        );

        registry.add(
                "spring.jpa.hibernate.ddl-auto",
                () -> "create-drop"
        );
    }
}