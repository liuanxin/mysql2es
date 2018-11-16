package com.github.config;

import com.github.model.Config;
import com.github.util.U;
import com.google.common.collect.Lists;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class DataInit {

    @Bean
    @ConfigurationProperties(prefix = "config")
    public Config config() {
        return new Config();
    }

//    @Bean
//    public TransportClient connect() {
//        Config config = config();
//        U.assertNil(config, "no config with MariaDB/MySQL and es mapping");
//        config.check();
//
//        Settings settings = Settings.builder()
//                .put("client.transport.sniff", true)
//                .build();
//        TransportClient client = new PreBuiltTransportClient(settings);
//        for (String ipAndPort : config.getIpPort()) {
//            String[] ipPort = ipAndPort.split(":");
//            if (U.isNotBlank(ipPort) && ipPort.length >= 2) {
//                String ip = ipPort[0];
//                int port = U.toInt(ipPort[1]);
//
//                if (U.isNotBlank(ip) && U.greater0(port)) {
//                    client.addTransportAddress(new TransportAddress(new InetSocketAddress(ip, port)));
//                }
//            }
//        }
//        return client;
//    }

    // https://www.elastic.co/guide/en/elasticsearch/client/index.html

    @Bean
    public RestHighLevelClient search() {
        Config config = config();
        U.assertNil(config, "no config with MariaDB/MySQL and es mapping");
        config.check();

        List<HttpHost> hostList = Lists.newArrayList();
        for (String ipAndPort : config.getIpPort()) {
            hostList.add(HttpHost.create(ipAndPort));
        }
        return new RestHighLevelClient(RestClient.builder(hostList.toArray(new HttpHost[hostList.size()])));
    }
}
