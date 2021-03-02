package com.github.run;

import com.github.model.Config;
import com.github.model.IncrementStorageType;
import com.github.model.Relation;
import com.github.repository.DataRepository;
import com.github.util.Dates;
import com.github.util.F;
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
            if (!config.isEnable()) {
                return;
            }

            long start = System.currentTimeMillis();
            if (Logs.ROOT_LOG.isInfoEnabled()) {
                Logs.ROOT_LOG.info("begin to run task");
            }
            try {
                IncrementStorageType incrementType = config.getIncrementType();
                boolean deleteEveryTime = incrementType == IncrementStorageType.TEMP_FILE && config.isDeleteTempEveryTime();
                Map<String, Future<Long>> resultMap = Maps.newHashMap();
                for (Relation relation : config.getRelation()) {
                    resultMap.put(relation.useKey(), dataRepository.asyncData(incrementType, relation));

                    if (deleteEveryTime) {
                        F.delete(relation.getTable(), relation.getIndex());
                    }
                }
                for (Map.Entry<String, Future<Long>> entry : resultMap.entrySet()) {
                    try {
                        Long count = entry.getValue().get();
                        if (U.isNotBlank(count)) {
                            if (Logs.ROOT_LOG.isInfoEnabled()) {
                                Logs.ROOT_LOG.info("async db to es({}) count({}), time({}ms)", entry.getKey(),
                                        count, System.currentTimeMillis() - start);
                            }
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        if (Logs.ROOT_LOG.isErrorEnabled()) {
                            Logs.ROOT_LOG.error("async db to es exception", e);
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
