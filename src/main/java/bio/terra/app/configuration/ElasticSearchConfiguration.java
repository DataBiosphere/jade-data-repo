package bio.terra.app.configuration;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticSearchConfiguration {
    @Bean
    public RestClientBuilder restClientBuilder(
            @Value("${elasticsearch.hostname}") String hostname,
            @Value("${elasticsearch.port}") int port
    ) {
        return RestClient.builder(new HttpHost(hostname, port));
    }
}
