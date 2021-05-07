package com.github.config;

import com.github.util.U;
import lombok.AllArgsConstructor;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.aop.interceptor.SimpleAsyncUncaughtExceptionHandler;
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
@AllArgsConstructor
public class TaskConfig implements AsyncConfigurer {

    @Override
    public Executor getAsyncExecutor() {
        int size = U.PROCESSORS << 2;

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
