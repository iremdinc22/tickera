package com.iremdinc.tickera;

import com.iremdinc.tickera.support.PostgresIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Uygulama context'inin izole PostgreSQL Testcontainer'ı
 * ile ayağa kalkabildiğini doğrular.
 *
 * Kafka listener ve scheduler, destek sınıfı tarafından
 * test sırasında devre dışı bırakılır.
 */
@SpringBootTest
class TickeraApplicationTests
        extends PostgresIntegrationTest {

    @Test
    void contextLoads() {
    }
}
