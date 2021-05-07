package com.github.run;

import com.github.model.Config;
import com.github.service.SyncDataService;
import com.github.util.U;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.scheduling.support.CronTrigger;

/**
 * spring.task.scheduling.threadNamePrefix = mysql2es-crontab-
 * spring.task.scheduling.pool.size = 8
 *
 * @see org.springframework.boot.autoconfigure.task.TaskSchedulingProperties
 */
@Configuration
@RequiredArgsConstructor
@SuppressWarnings("NullableProblems")
public class Job implements SchedulingConfigurer {

    @Value("${db2es.crontabPoolSize:0}")
    private int crontabPoolSize;

    private final Config config;
    private final SyncDataService syncService;

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        if (config.isEnable()) {
            ThreadPoolTaskScheduler crontab = new ThreadPoolTaskScheduler();
            crontab.setPoolSize((crontabPoolSize <= 0) ? (U.PROCESSORS << 2) : crontabPoolSize);
            crontab.setThreadNamePrefix("mysql2es-crontab-");
            crontab.initialize();
            taskRegistrar.setTaskScheduler(crontab);

            if (config.isEnableCompensate()) {
                taskRegistrar.addTriggerTask(syncService::handleCompensate, new CronTrigger(config.getCompensateCron()));
            }
            taskRegistrar.addTriggerTask(syncService::handle, new CronTrigger(config.getCron()));
        }
    }
}
