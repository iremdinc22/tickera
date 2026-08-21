package com.iremdinc.tickera;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class TickeraApplication {

    public static void main(String[] args) {
        SpringApplication.run(
                TickeraApplication.class,
                args
        );
    }
}
