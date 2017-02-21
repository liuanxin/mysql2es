package com.github.mte.db;

import com.alibaba.druid.pool.DruidDataSource;
import com.github.mte.model.Config;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class DataInit {

    @Bean
    @ConfigurationProperties(prefix = "config")
    public Config config() {
        return new Config();
    }

    @Bean(value = "dataSource")
    @ConfigurationProperties(prefix = "db")
    public DataSource setup() {
        return DataSourceBuilder.create().type(DruidDataSource.class).build();
    }
}
