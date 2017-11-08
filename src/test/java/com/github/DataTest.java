package com.github;

import com.github.run.Job;
import com.github.service.BondingService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@SpringBootTest
@RunWith(SpringJUnit4ClassRunner.class)
@ComponentScan(excludeFilters = {
        @ComponentScan.Filter(
                type= FilterType.ASSIGNABLE_TYPE,
                classes = Job.class
        )
})
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
