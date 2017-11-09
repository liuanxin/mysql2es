package com.github;

import com.github.service.BondingService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest
@RunWith(SpringRunner.class)
public class DataTest {

    @Autowired
    private BondingService bondingService;

    @Sql({"classpath:sql/scheme.sql", "classpath:sql/insert.sql"})
    @Sql(value = {"classpath:sql/delete.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    @Test
    public void test() {
        boolean flag = bondingService.syncData();
        if (flag) {
            flag = bondingService.deleteScheme();
            if (flag) {
                bondingService.deleteTempFile();
            }
        }
    }
}
