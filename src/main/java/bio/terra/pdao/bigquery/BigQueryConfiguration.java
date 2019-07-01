package bio.terra.pdao.bigquery;


import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import org.springframework.beans.factory.annotation.Value;
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
    // TODO: This is temporary. When we do the resource manager, the project id will via there
    // for a given dataset. This project id is used here and in the file system (FireStore) to
    // allocate the accessing instance.

    @Value("${google.projectid}")
    private String projectId;

    @Bean("googleProjectId")
    public String googleProjectId() {
        return projectId;
    }

    @Bean("bigQueryProjectId")
    public String bigQueryProjectId() {
        return projectId;
    }

    @Bean("bigQuery")
    public BigQuery bigQuery() {
        return BigQueryOptions.newBuilder()
            .setProjectId(projectId)
            .build()
            .getService();
    }

    @Bean("firestore")
    public Firestore firestore() {
        return FirestoreOptions.newBuilder()
            .setProjectId(projectId)
            .build()
            .getService();
    }

}
