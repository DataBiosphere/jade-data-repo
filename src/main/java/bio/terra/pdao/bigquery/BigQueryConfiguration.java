package bio.terra.pdao.bigquery;


import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * The theory of operation here is that each implementation of a pdao will specify a configuration.
 * That way, implementation specifics can be separated from the interface. We'll see if it works out that way.
 */
@Configuration
@Profile("google")
public class BigQueryConfiguration {
    @Bean("bigQueryProjectId")
    public String bigQueryProjectId() {
        return BigQueryOptions.getDefaultProjectId();
    }

    @Bean("bigQuery")
    public BigQuery bigQuery() {
        return BigQueryOptions.getDefaultInstance().getService();
    }
}
