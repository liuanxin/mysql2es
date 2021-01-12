package com.github.run;

import com.github.model.Config;
import com.github.model.Relation;
import com.github.repository.DataRepository;
import com.github.util.Dates;
import com.github.util.Logs;
import com.github.util.U;
import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.scheduling.support.CronTrigger;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Configuration
@AllArgsConstructor
public class Job implements SchedulingConfigurer {

    private final Config config;
    private final DataRepository dataRepository;

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        taskRegistrar.addTriggerTask(() -> {
            long start = System.currentTimeMillis();
            if (Logs.ROOT_LOG.isInfoEnabled()) {
                Logs.ROOT_LOG.info("begin to run task");
            }
            try {
                Map<String, Future<Boolean>> resultMap = Maps.newHashMap();
                for (Relation relation : config.getRelation()) {
                    resultMap.put(relation.useKey(), dataRepository.asyncData(relation));
                }
                for (Map.Entry<String, Future<Boolean>> entry : resultMap.entrySet()) {
                    try {
                        Boolean flag = entry.getValue().get();
                        if (Logs.ROOT_LOG.isDebugEnabled()) {
                            String status = (U.isNotBlank(flag) && flag) ? "success" : "fail";
                            Logs.ROOT_LOG.debug("async db to es({}) {}", entry.getKey(), status);
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        if (Logs.ROOT_LOG.isErrorEnabled()) {
                            Logs.ROOT_LOG.error("async db data to es exception", e);
                        }
                    }
                }
            } finally {
                if (Logs.ROOT_LOG.isInfoEnabled()) {
                    long end = System.currentTimeMillis();
                    Logs.ROOT_LOG.info("end of run task, use time: {}.", Dates.toHuman(end - start));
                }
            }
        }, new CronTrigger(config.getCron()));
    }
}
