package com.iremdinc.tickera.support;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * Redis'e ihtiyaç duyan testler için izole bir
 * Testcontainer sağlar.
 *
 * Container testler arasında paylaşılır; her test
 * kendi anahtarlarını temizlemekle sorumludur.
 */
public abstract class RedisIntegrationTest
        extends PostgresIntegrationTest {

    private static final int REDIS_PORT =
            6379;

    @SuppressWarnings("resource")
    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(
                    DockerImageName.parse(
                            "redis:7.4-alpine"
                    )
            )
                    .withExposedPorts(
                            REDIS_PORT
                    )
                    .withStartupTimeout(
                            Duration.ofMinutes(
                                    2
                            )
                    );

    static {
        REDIS.start();
    }

    @DynamicPropertySource
    static void configureRedis(
            DynamicPropertyRegistry registry
    ) {

        registry.add(
                "spring.data.redis.host",
                REDIS::getHost
        );

        registry.add(
                "spring.data.redis.port",
                () -> REDIS.getMappedPort(
                        REDIS_PORT
                )
        );
    }
}
