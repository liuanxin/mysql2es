package com.github.config;

import com.github.model.Config;
import com.github.util.A;
import com.github.util.U;
import lombok.AllArgsConstructor;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.aop.interceptor.SimpleAsyncUncaughtExceptionHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
@EnableAsync
@AllArgsConstructor
public class TaskConfig implements AsyncConfigurer {

    private final Config config;

    @Override
    public Executor getAsyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

        if (U.isNotBlank(config) && A.isNotEmpty(config.getRelation())) {
            int size = config.getRelation().size();
            executor.setCorePoolSize(size);
            executor.setMaxPoolSize(size);
            executor.setQueueCapacity(0);
        } else {
            int size = U.PROCESSORS;
            executor.setCorePoolSize(size);
            executor.setMaxPoolSize(size << 2);
            executor.setQueueCapacity(size);
        }

        executor.setThreadNamePrefix("mysql2es-executor-");
        executor.initialize();
        return executor;
    }

    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return new SimpleAsyncUncaughtExceptionHandler();
    }
}
