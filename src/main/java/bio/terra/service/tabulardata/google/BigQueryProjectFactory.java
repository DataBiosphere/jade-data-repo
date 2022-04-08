package bio.terra.service.tabulardata.google;

import bio.terra.service.filedata.google.gcs.GcsConfiguration;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Configuration
@Profile("google")
@ConfigurationProperties(prefix = "datarepo.gcs.bq")
public class BigQueryProjectFactory {
  private static final Logger logger = LoggerFactory.getLogger(BigQueryProjectFactory.class);
  private int connectTimeoutSeconds;
  private int readTimeoutSeconds;

  @Autowired
  public BigQueryProjectFactory() {
    // this.gcsConfiguration = gcsConfiguration;
  }

  public static BigQueryProject buildBQProject(String projectId) {
    logger.info("Retrieving Bigquery project for project id: {}", projectId);
    HttpTransportOptions transportOptions = StorageOptions.getDefaultHttpTransportOptions();
    transportOptions =
        transportOptions.toBuilder()
            .setConnectTimeout(connectTimeoutSeconds * 1000)
//            .setReadTimeout(TIMEOUT_SECONDS * 1000)
            .build();
    BigQuery bq =
        BigQueryOptions.newBuilder()
            .setTransportOptions(transportOptions)
            .setProjectId(projectId)
            .build()
            .getService();
    return new BigQueryProject(projectId, bq);
  }

  public int getConnectTimeoutSeconds() {
    return connectTimeoutSeconds;
  }

  public void setConnectTimeoutSeconds(int connectTimeoutSeconds) {
    this.connectTimeoutSeconds = connectTimeoutSeconds;
  }

  public int getReadTimeoutSeconds() {
    return readTimeoutSeconds;
  }

  public void setReadTimeoutSeconds(int readTimeoutSeconds) {
    this.readTimeoutSeconds = readTimeoutSeconds;
  }
}
