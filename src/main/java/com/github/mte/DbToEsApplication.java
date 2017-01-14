package com.github.mte;

import com.github.mte.util.Logs;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.CountDownLatch;

@SpringBootApplication
public class DbToEsApplication {

    @Bean
    public CountDownLatch closeLatch() {
        return new CountDownLatch(1);
    }

    public static void main(String[] args) {
        ConfigurableApplicationContext ctx = new SpringApplicationBuilder()
                .sources(DbToEsApplication.class).web(false).run(args);

        // make main Thread wait
        try {
            ctx.getBean(CountDownLatch.class).await();
        } catch (InterruptedException e) {
            if (Logs.ROOT_LOG.isDebugEnabled())
                Logs.ROOT_LOG.debug("wait main Thread exception", e);
        }
    }
}
