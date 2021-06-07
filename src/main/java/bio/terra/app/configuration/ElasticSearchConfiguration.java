package bio.terra.app.configuration;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticSearchConfiguration {

    // This Bean isn't injected directly. Its definition enables injecting RestHighLevelClient.
    @Bean
    public RestHighLevelClient client(
            @Value("${elasticsearch.hostname}") String hostname,
            @Value("${elasticsearch.port}") int port
    ) {
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, port));
        return new RestHighLevelClient(builder);
    }
}
