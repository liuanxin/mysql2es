package com.github;

import com.github.model.Config;
import com.github.model.Relation;
import com.github.repository.DataRepository;
import com.github.repository.EsRepository;
import com.github.util.F;
import com.github.util.Logs;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@SpringBootTest
@RunWith(SpringRunner.class)
public class DataTest {

    private Config config;
    private EsRepository esRepository;
    private DataRepository dataRepository;
    public DataTest(Config config, EsRepository esRepository, DataRepository dataRepository) {
        this.config = config;
        this.esRepository = esRepository;
        this.dataRepository = dataRepository;
    }

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

        for (Relation relation : config.getRelation()) {
            String index = relation.useIndex();
            String type = relation.getType();
            try {
                boolean deleteSchemeFlag = esRepository.deleteScheme(index, type).get();
                if (deleteSchemeFlag) {
                    F.delete(relation.getTable(), index, type);
                }
            } catch(InterruptedException | ExecutionException e){
                if (Logs.ROOT_LOG.isErrorEnabled()) {
                    Logs.ROOT_LOG.error(String.format("delete scheme(%s/%s) exception", index, type), e);
                }
            }
        }
    }
}
