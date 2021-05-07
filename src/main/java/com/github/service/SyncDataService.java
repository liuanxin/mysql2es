package com.github.service;

import com.github.model.Config;
import com.github.model.IncrementStorageType;
import com.github.model.Relation;
import com.github.repository.DataRepository;
import com.github.repository.EsRepository;
import com.github.util.*;
import com.google.common.collect.Maps;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings("rawtypes")
@Component
@RequiredArgsConstructor
public class SyncDataService {

    private static final AtomicBoolean SYNC_RUN = new AtomicBoolean(false);
    private static final AtomicBoolean COMPENSATE_RUN = new AtomicBoolean(false);


    private final Config config;
    private final EsRepository esRepository;
    private final DataRepository dataRepository;


    public void init() {
        if (!config.isEnable()) {
            return;
        }

        if (config.getIncrementType() == IncrementStorageType.MYSQL) {
            dataRepository.generateIncrementTable();
        }

        long start = System.currentTimeMillis();
        if (Logs.ROOT_LOG.isInfoEnabled()) {
            Logs.ROOT_LOG.info("begin to generate scheme");
        }
        try {
            for (Relation relation : config.getRelation()) {
                Map<String, Map> properties = dataRepository.dbToEsScheme(relation);
                if (relation.isScheme() && A.isNotEmpty(properties)) {
                    String index = relation.useIndex();
                    esRepository.saveScheme(index, properties);
                }
            }
        } finally {
            if (Logs.ROOT_LOG.isInfoEnabled()) {
                long end = System.currentTimeMillis();
                Logs.ROOT_LOG.info("end of generate scheme, use time: {}.", Dates.toHuman(end - start));
            }
        }
    }

    public void handle() {
        if (!config.isEnable()) {
            return;
        }
        if (!SYNC_RUN.compareAndSet(false, true)) {
            if (Logs.ROOT_LOG.isInfoEnabled()) {
                Logs.ROOT_LOG.info("task has be running");
            }
            return;
        }

        if (Logs.ROOT_LOG.isInfoEnabled()) {
            Logs.ROOT_LOG.info("begin to run task");
        }
        long start = System.currentTimeMillis();
        try {
            IncrementStorageType incrementType = config.getIncrementType();
            boolean deleteEveryTime = incrementType == IncrementStorageType.TEMP_FILE && config.isDeleteTempEveryTime();
            Map<String, Future<String>> resultMap = Maps.newHashMap();
            for (Relation relation : config.getRelation()) {
                resultMap.put(relation.useKey(), dataRepository.asyncData(incrementType, relation));

                if (deleteEveryTime) {
                    F.delete(relation.getTable(), relation.getIndex());
                }
            }
            for (Map.Entry<String, Future<String>> entry : resultMap.entrySet()) {
                try {
                    String msg = entry.getValue().get();
                    if (U.isNumber(msg)) {
                        if (Logs.ROOT_LOG.isInfoEnabled()) {
                            Long count = U.toLong(msg);
                            long ms = System.currentTimeMillis() - start;
                            String tps = (U.greater0(count) && ms > 0) ? String.valueOf(count * 1000 / ms) : "0";
                            // equals data will sync multi times, It will larger than db's count
                            Logs.ROOT_LOG.info("async({}) count({}), time({}) tps({})",
                                    entry.getKey(), count, Dates.toHuman(ms), tps);
                        }
                    } else {
                        if (Logs.ROOT_LOG.isErrorEnabled()) {
                            Logs.ROOT_LOG.error(String.format("async(%s) has return (%s), time(%s)",
                                    entry.getKey(), msg, Dates.toHuman(System.currentTimeMillis() - start)));
                        }
                    }
                } catch (InterruptedException | ExecutionException e) {
                    if (Logs.ROOT_LOG.isErrorEnabled()) {
                        Logs.ROOT_LOG.error(String.format("async(%s) Thread exception, time(%s)",
                                entry.getKey(), Dates.toHuman(System.currentTimeMillis() - start)), e);
                    }
                } catch (Exception e) {
                    if (Logs.ROOT_LOG.isErrorEnabled()) {
                        Logs.ROOT_LOG.error(String.format("async(%s) exception, time(%s)",
                                entry.getKey(), Dates.toHuman(System.currentTimeMillis() - start)), e);
                    }
                }
            }
        } finally {
            if (Logs.ROOT_LOG.isInfoEnabled()) {
                Logs.ROOT_LOG.info("end of run task, time({})", Dates.toHuman(System.currentTimeMillis() - start));
            }
            SYNC_RUN.set(false);
        }
    }

    public void handleCompensate() {
        if (!config.isEnable() || !config.isEnableCompensate()) {
            return;
        }
        if (!COMPENSATE_RUN.compareAndSet(false, true)) {
            if (Logs.ROOT_LOG.isInfoEnabled()) {
                Logs.ROOT_LOG.info("compensate task has be running");
            }
            return;
        }

        if (Logs.ROOT_LOG.isInfoEnabled()) {
            Logs.ROOT_LOG.info("compensate begin to run task");
        }
        long start = System.currentTimeMillis();
        try {
            IncrementStorageType incrementType = config.getIncrementType();
            int beginIntervalSecond = config.getBeginIntervalSecond();
            int compensateSecond = config.getCompensateSecond();
            Map<String, Future<String>> resultMap = Maps.newHashMap();
            for (Relation relation : config.getRelation()) {
                resultMap.put(relation.useKey(), dataRepository.asyncCompensateData(incrementType, relation,
                        beginIntervalSecond, compensateSecond));
            }
            for (Map.Entry<String, Future<String>> entry : resultMap.entrySet()) {
                try {
                    String msg = entry.getValue().get();
                    if (U.isNumber(msg)) {
                        if (Logs.ROOT_LOG.isInfoEnabled()) {
                            Long count = U.toLong(msg);
                            long ms = System.currentTimeMillis() - start;
                            String tps = (U.greater0(count) && ms > 0) ? String.valueOf(count * 1000 / ms) : "0";
                            Logs.ROOT_LOG.info("compensate async({}) count({}) time({}) tps({})",
                                    entry.getKey(), count, Dates.toHuman(ms), tps);
                        }
                    } else {
                        if (Logs.ROOT_LOG.isErrorEnabled()) {
                            Logs.ROOT_LOG.error(String.format("compensate async(%s) has return (%s), time(%s)",
                                    entry.getKey(), msg, Dates.toHuman(System.currentTimeMillis() - start)));
                        }
                    }
                } catch (InterruptedException | ExecutionException e) {
                    if (Logs.ROOT_LOG.isErrorEnabled()) {
                        Logs.ROOT_LOG.error(String.format("compensate async(%s) Thread exception, time(%s)",
                                entry.getKey(), Dates.toHuman(System.currentTimeMillis() - start)), e);
                    }
                } catch (Exception e) {
                    if (Logs.ROOT_LOG.isErrorEnabled()) {
                        Logs.ROOT_LOG.error(String.format("compensate async(%s) exception, time(%s)",
                                entry.getKey(), Dates.toHuman(System.currentTimeMillis() - start)), e);
                    }
                }
            }
        } finally {
            if (Logs.ROOT_LOG.isInfoEnabled()) {
                Logs.ROOT_LOG.info("compensate end of run task, time({})", Dates.toHuman(System.currentTimeMillis() - start));
            }
            COMPENSATE_RUN.set(false);
        }
    }
}
