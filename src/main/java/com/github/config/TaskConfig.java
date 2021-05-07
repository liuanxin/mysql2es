package com.github.config;

import com.github.util.U;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.aop.interceptor.SimpleAsyncUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

/**
 * spring.task.execution.threadNamePrefix = mysql2es-async-
 * spring.task.execution.pool.coreSize = 8
 * spring.task.execution.pool.maxSize = 8
 * spring.task.execution.pool.queueCapacity = 0
 *
 * @see org.springframework.boot.autoconfigure.task.TaskExecutionProperties
 */
@Configuration
@EnableAsync
public class TaskConfig implements AsyncConfigurer {

    @Value("${db2es.asyncPoolSize:0}")
    private int asyncPoolSize;

    @Override
    public Executor getAsyncExecutor() {
        int size = (asyncPoolSize <= 0) ? (U.PROCESSORS << 2) : asyncPoolSize;

        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(size);
        executor.setMaxPoolSize(size);
        executor.setQueueCapacity(0);
        executor.setThreadNamePrefix("mysql2es-async-");
        executor.initialize();
        return executor;
    }

    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return new SimpleAsyncUncaughtExceptionHandler();
    }
}
