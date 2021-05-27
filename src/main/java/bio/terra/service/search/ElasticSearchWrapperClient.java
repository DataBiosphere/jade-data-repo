package bio.terra.service.search;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

@Component
@PropertySource("classpath:application.properties")
public class ElasticSearchWrapperClient {

    private RestHighLevelClient client;

    public ElasticSearchWrapperClient(
            @Value("${elasticsearch.hostname}") String hostname,
            @Value("${elasticsearch.port}") int port
    ){
        final RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, port));
        this.client = new RestHighLevelClient(builder);
    }

    public RestHighLevelClient getClient() {
        return client;
    }
}
