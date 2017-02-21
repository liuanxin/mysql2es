package com.github.mte.db;

import com.alibaba.druid.pool.DruidDataSource;
import com.github.mte.model.Config;
import com.github.mte.util.U;
import org.apache.commons.lang3.math.NumberUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.net.InetSocketAddress;

@Configuration
public class DataInit {

    @Bean
    @ConfigurationProperties(prefix = "config")
    public Config config() {
        return new Config();
    }

    @Bean
    @ConfigurationProperties(prefix = "db")
    public DataSource setup() {
        return DataSourceBuilder.create().type(DruidDataSource.class).build();
    }

    @Bean(destroyMethod = "close")
    public TransportClient connect() {
        Config config = config();
        U.assertNil(config, "no config with mysql and es mapping");
        config.check();

        // es 5.1.1 --> TransportClient is a abstract class
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY);
        for (String ipAndPort : config.getIpPort()) {
            String[] ipPort = ipAndPort.split(":");
            if (U.isNotBlank(ipPort) && ipPort.length >= 2) {
                String ip = ipPort[0];
                int port = NumberUtils.toInt(ipPort[1]);

                if (U.isNotBlank(ip) && U.greater0(port)) {
                    client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(ip, port)));
                }
            }
        }
        return client;
    }
}
