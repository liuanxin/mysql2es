package com.github.service;

import com.github.model.Config;
import com.github.model.IncrementStorageType;
import com.github.model.Relation;
import com.github.repository.DataRepository;
import com.github.repository.EsRepository;
import com.github.util.A;
import com.github.util.Dates;
import com.github.util.Logs;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Map;

@SuppressWarnings("rawtypes")
@Component
@RequiredArgsConstructor
public class SyncDataService {

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

        IncrementStorageType incrementType = config.getIncrementType();
        for (Relation relation : config.getRelation()) {
            dataRepository.asyncData(incrementType, relation);
        }
    }

    public void handleCompensate() {
        if (!config.isEnable() || !config.isEnableCompensate()) {
            return;
        }

        IncrementStorageType incrementType = config.getIncrementType();
        int beginCompensateSecond = config.getBeginCompensateSecond();
        int compensateSecond = config.getCompensateSecond();
        for (Relation relation : config.getRelation()) {
            dataRepository.asyncCompensateData(incrementType, relation, beginCompensateSecond, compensateSecond);
        }
    }
}
