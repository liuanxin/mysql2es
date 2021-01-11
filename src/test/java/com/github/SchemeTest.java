package com.github;

import com.github.model.Relation;
import com.github.repository.DataRepository;
import com.github.repository.EsRepository;
import com.github.util.A;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Map;

@SpringBootTest
@RunWith(SpringRunner.class)
@SuppressWarnings("rawtypes")
public class SchemeTest {

    @Value("${db2es.relation}")
    private List<Relation> relations;

    private final EsRepository esRepository;
    private final DataRepository dataRepository;
    public SchemeTest(EsRepository esRepository, DataRepository dataRepository) {
        this.esRepository = esRepository;
        this.dataRepository = dataRepository;
    }

    @Sql({"classpath:sql/scheme.sql"})
    @Sql(value = {"classpath:sql/delete.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    @Test
    public void test() {
        for (Relation relation : relations) {
            String index = relation.useIndex();

            Map<String, Map> properties = dataRepository.dbToEsScheme(relation);
            if (relation.isScheme() && A.isNotEmpty(properties)) {
                esRepository.saveScheme(index, properties);
            }

            esRepository.deleteScheme(index);
        }
    }
}
