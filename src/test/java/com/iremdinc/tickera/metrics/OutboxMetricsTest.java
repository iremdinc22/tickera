package com.iremdinc.tickera.metrics;

import com.iremdinc.tickera.enums.OutboxStatus;
import com.iremdinc.tickera.repository.OutboxEventRepository;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Outbox metric'lerinin Prometheus üzerinde hangi
 * isimlerle ve hangi değerlerle yayınlandığını doğrular.
 *
 * Bu test Spring context'i başlatmaz; metric registration
 * ve counter/gauge semantiğini izole biçimde kontrol eder.
 */
class OutboxMetricsTest {

    private static final String PUBLISHED_METRIC =
            "tickera_outbox_published_total";

    private static final String RETRY_METRIC =
            "tickera_outbox_retry_total";

    private static final String STALE_RECOVERY_METRIC =
            "tickera_outbox_stale_recovery_total";

    private static final String PENDING_METRIC =
            "tickera_outbox_pending_events";

    private static final String FAILED_METRIC =
            "tickera_outbox_failed_events";

    private PrometheusMeterRegistry meterRegistry;

    private OutboxEventRepository outboxEventRepository;

    @BeforeEach
    void setUp() {

        meterRegistry =
                new PrometheusMeterRegistry(
                        PrometheusConfig.DEFAULT
                );

        outboxEventRepository =
                mock(OutboxEventRepository.class);

        when(
                outboxEventRepository.countByStatus(
                        OutboxStatus.PENDING
                )
        ).thenReturn(3L);

        when(
                outboxEventRepository.countByStatus(
                        OutboxStatus.FAILED
                )
        ).thenReturn(2L);

        new OutboxMetrics(
                meterRegistry,
                outboxEventRepository
        );
    }

    @Test
    void shouldExposeStablePrometheusMetricNames() {

        String scrape =
                meterRegistry.scrape();

        assertEquals(
                0.0,
                metricValue(
                        scrape,
                        PUBLISHED_METRIC
                )
        );

        assertEquals(
                0.0,
                metricValue(
                        scrape,
                        RETRY_METRIC
                )
        );

        assertEquals(
                0.0,
                metricValue(
                        scrape,
                        STALE_RECOVERY_METRIC
                )
        );

        assertEquals(
                3.0,
                metricValue(
                        scrape,
                        PENDING_METRIC
                )
        );

        assertEquals(
                2.0,
                metricValue(
                        scrape,
                        FAILED_METRIC
                )
        );
    }

    @Test
    void shouldIncrementCountersByExpectedAmounts() {

        OutboxMetrics metrics =
                new OutboxMetrics(
                        meterRegistry,
                        outboxEventRepository
                );

        metrics.incrementPublished();
        metrics.incrementRetry();
        metrics.incrementRetry(
                2
        );
        metrics.incrementStaleRecovery();
        metrics.incrementStaleRecovery(
                3
        );

        String scrape =
                meterRegistry.scrape();

        assertEquals(
                1.0,
                metricValue(
                        scrape,
                        PUBLISHED_METRIC
                )
        );

        assertEquals(
                3.0,
                metricValue(
                        scrape,
                        RETRY_METRIC
                )
        );

        assertEquals(
                4.0,
                metricValue(
                        scrape,
                        STALE_RECOVERY_METRIC
                )
        );
    }

    @Test
    void shouldReflectCurrentGaugeValues() {

        when(
                outboxEventRepository.countByStatus(
                        OutboxStatus.PENDING
                )
        ).thenReturn(7L);

        assertEquals(
                7.0,
                metricValue(
                        meterRegistry.scrape(),
                        PENDING_METRIC
                )
        );
    }

    @Test
    void shouldNotRegisterDuplicateMetricSeries() {

        /*
         * Aynı registry üzerinde ikinci bir OutboxMetrics
         * instance'ı oluşturulması mevcut serileri
         * çoğaltmamalıdır.
         */
        new OutboxMetrics(
                meterRegistry,
                outboxEventRepository
        );

        String scrape =
                meterRegistry.scrape();

        for (String metric : new String[]{
                PUBLISHED_METRIC,
                RETRY_METRIC,
                STALE_RECOVERY_METRIC,
                PENDING_METRIC,
                FAILED_METRIC
        }) {

            assertEquals(
                    1,
                    seriesCount(
                            scrape,
                            metric
                    ),
                    "Expected exactly one series for " + metric
            );
        }
    }

    private long seriesCount(
            String scrape,
            String metric
    ) {

        return Arrays.stream(
                        scrape.split(
                                "\n"
                        )
                )
                .filter(line ->
                        line.startsWith(
                                metric + " "
                        )
                )
                .count();
    }

    private double metricValue(
            String scrape,
            String metric
    ) {

        return Arrays.stream(
                        scrape.split(
                                "\n"
                        )
                )
                .filter(line ->
                        line.startsWith(
                                metric + " "
                        )
                )
                .map(line ->
                        Double.parseDouble(
                                line.substring(
                                        metric.length()
                                                + 1
                                )
                        )
                )
                .findFirst()
                .orElseThrow(() ->
                        new AssertionError(
                                "Metric not found: " + metric
                        )
                );
    }
}
