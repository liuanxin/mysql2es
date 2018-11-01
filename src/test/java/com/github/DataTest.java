package com.github;

import com.github.model.Config;
import com.github.model.Relation;
import com.github.model.Scheme;
import com.github.repository.DataRepository;
import com.github.repository.EsRepository;
import com.github.util.Jsons;
import com.github.util.Logs;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@SpringBootTest
@RunWith(SpringRunner.class)
public class DataTest {

    @Autowired
    private Config config;

    @Autowired
    private EsRepository esRepository;

    @Autowired
    private DataRepository dataRepository;

    @Sql({"classpath:sql/scheme.sql", "classpath:sql/insert.sql"})
    @Sql(value = {"classpath:sql/delete.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    @Test
    public void test() {
        List<Future<Boolean>> resultList = Lists.newArrayList();
        for (Relation relation : config.getRelation()) {
            resultList.add(dataRepository.asyncData(relation));
        }
        for (Future<Boolean> future : resultList) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                if (Logs.ROOT_LOG.isErrorEnabled()) {
                    Logs.ROOT_LOG.error("test: async db data to es exception", e);
                }
            }
        }

        List<Scheme> schemeList = dataRepository.dbToEsScheme();
        try {
            boolean deleteSchemeFlag = esRepository.deleteScheme(schemeList).get();
            if (deleteSchemeFlag) {
                dataRepository.deleteTempFile();
            }
        } catch (InterruptedException | ExecutionException e) {
            if (Logs.ROOT_LOG.isErrorEnabled()) {
                Logs.ROOT_LOG.error(String.format("delete scheme(%s) exception", Jsons.toJson(schemeList)), e);
            }
        }
    }
}
