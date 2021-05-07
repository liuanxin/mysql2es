package com.github.run;

import com.github.model.Config;
import com.github.service.SyncDataService;
import com.github.util.U;
import lombok.AllArgsConstructor;
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
@AllArgsConstructor
public class Job implements SchedulingConfigurer {

    private final Config config;
    private final SyncDataService syncService;

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        ThreadPoolTaskScheduler crontab = new ThreadPoolTaskScheduler();
        crontab.setPoolSize(U.PROCESSORS << 2);
        crontab.setThreadNamePrefix("mysql2es-crontab-");
        crontab.initialize();
        taskRegistrar.setTaskScheduler(crontab);

        taskRegistrar.addTriggerTask(syncService::handle, new CronTrigger(config.getCron()));
        taskRegistrar.addTriggerTask(syncService::handleCompensate, new CronTrigger(config.getCompensateCron()));
    }
}
