package com.github.run;

import com.github.model.Relation;
import com.github.repository.DataRepository;
import com.github.repository.EsRepository;
import com.github.util.A;
import com.github.util.Dates;
import com.github.util.Logs;
import com.github.util.U;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.List;
import java.util.Map;

@Profile("!test")
@Configuration
@SuppressWarnings("rawtypes")
public class Runner implements ApplicationRunner {

    @Value("${db2es.relation}")
    private List<Relation> relations;

    private final EsRepository esRepository;
    private final DataRepository dataRepository;
    public Runner(EsRepository esRepository, DataRepository dataRepository) {
        this.esRepository = esRepository;
        this.dataRepository = dataRepository;
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void run(ApplicationArguments args) throws Exception {
        U.assertException(A.isEmpty(relations), "must set [db es] relation");
        for (Relation r : relations) {
            r.check();
        }

        long start = System.currentTimeMillis();
        if (Logs.ROOT_LOG.isInfoEnabled()) {
            Logs.ROOT_LOG.info("begin to generate scheme");
        }
        try {
            for (Relation relation : relations) {
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
}
