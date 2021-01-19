package com.github.config;

import com.github.model.Config;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DataInit {

    @Bean(initMethod = "check")
    @ConfigurationProperties(prefix = "db2es")
    public Config config() {
        return new Config();
    }

//    @Bean
//    public TransportClient connect() {
//        Config config = config();
//        U.assertNil(config, "no config with MariaDB/MySQL and es mapping");
//
//        Settings settings = Settings.builder().put("client.transport.sniff", true).build();
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

//    // https://www.elastic.co/guide/en/elasticsearch/client/index.html

//    // @see org.springframework.boot.autoconfigure.elasticsearch.rest.RestClientConfigurations.RestHighLevelClientConfiguration
//    @Bean
//    public RestHighLevelClient search() {
//        Config config = config();
//        U.assertNil(config, "no config with MariaDB/MySQL and es mapping");
//
//        String searchUrl = config.getIpPort();
//        String username = config.getUsername();
//        String password = config.getPassword();
//
//        String[] ipPortArray = searchUrl.split(",");
//        List<HttpHost> hostList = Lists.newArrayList();
//        for (String ipAndPort : ipPortArray) {
//            if (U.isNotBlank(ipAndPort)) {
//                hostList.add(HttpHost.create(ipAndPort.trim()));
//            }
//        }
//
//        RestClientBuilder builder = RestClient.builder(hostList.toArray(new HttpHost[0]));
//
//        if (U.isNotBlank(username) && U.isNotBlank(password)) {
//            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
//            builder.setHttpClientConfigCallback(httpClientBuilder ->
//                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
//        }
//        return new RestHighLevelClient(builder);
//    }
}
