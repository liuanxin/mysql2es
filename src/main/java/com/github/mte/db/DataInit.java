package com.github.mte.db;

import com.alibaba.druid.pool.DruidDataSource;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class DataInit {

    @Bean(value = "dataSource", initMethod = "init", destroyMethod = "close")
    @ConfigurationProperties(prefix = "db")
    public DataSource setup() {
        return DataSourceBuilder.create().type(DruidDataSource.class).build();
    }
}
