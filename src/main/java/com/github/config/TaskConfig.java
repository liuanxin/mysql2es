package com.github.config;

import com.github.model.Config;
import com.github.util.A;
import com.github.util.U;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.aop.interceptor.SimpleAsyncUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
@EnableAsync
public class TaskConfig implements AsyncConfigurer {

    static final int PROCESSORS = Runtime.getRuntime().availableProcessors();

    @Autowired
    private Config config;

    @Override
    public Executor getAsyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        if (U.isBlank(config) || A.isEmpty(config.getRelation())) {
            executor.setCorePoolSize(PROCESSORS);
            executor.setMaxPoolSize(PROCESSORS << 2);
            executor.setQueueCapacity(PROCESSORS);
        } else {
            int size = config.getRelation().size();
            executor.setCorePoolSize(size);
            executor.setMaxPoolSize(size << 2);
            executor.setQueueCapacity(size);
        }
        executor.setThreadNamePrefix("async-executor-");
        executor.initialize();
        return executor;
    }

    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return new SimpleAsyncUncaughtExceptionHandler();
    }
}
