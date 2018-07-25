package com.github;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class DbToEsApplication {

    public static void main(String[] args) {
        new SpringApplicationBuilder(DbToEsApplication.class).web(WebApplicationType.NONE).run(args);
    }
}
