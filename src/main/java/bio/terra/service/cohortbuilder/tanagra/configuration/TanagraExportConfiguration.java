package bio.terra.service.cohortbuilder.tanagra.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "tanagra.export")
public class TanagraExportConfiguration {
  private static final Logger LOGGER = LoggerFactory.getLogger(TanagraExportConfiguration.class);

  // Project that export bucket is in.
  private String gcsBucketProjectId;

  // Bucket name without "gs://".
  // When user exports dataset, a CSV file is temporarily stored in this bucket. After CSV file is
  // downloaded, CSV file is no longer needed. It is recommended to set TTL on this bucket.
  // The bucket must be in same region as index:
  // https://cloud.google.com/bigquery/docs/exporting-data#data-locations
  private String gcsBucketName;

  public String getGcsBucketProjectId() {
    return gcsBucketProjectId;
  }

  public String getGcsBucketName() {
    return gcsBucketName;
  }

  public void setGcsBucketProjectId(String gcsBucketProjectId) {
    this.gcsBucketProjectId = gcsBucketProjectId;
  }

  public void setGcsBucketName(String gcsBucketName) {
    this.gcsBucketName = gcsBucketName;
  }

  /** Write the config properties into the log. Add an entry here for each new config property. */
  public void logConfig() {
    LOGGER.info("Export: gcs-bucket-project-id: {}", getGcsBucketProjectId());
    LOGGER.info("Export: gcs-bucket-name: {}", getGcsBucketName());
  }
}
