package bio.terra.service.tabulardata.google;

import static org.junit.jupiter.api.Assertions.*;

import bio.terra.common.AclUtils;
import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotModel;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.cloud.bigquery.BigQueryException;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class BigQueryProjectTest {

  @Test
  void bigQueryAclUpdateShouldRetry() {
    // mock an exception that looks like the following:
    // com.google.cloud.bigquery.BigQueryException: Project id: datarepo-REDACTED
    //	at com.google.cloud.bigquery.BigQueryRetryHelper.runWithRetries(BigQueryRetryHelper.java:59)
    // ...
    // Caused by: com.google.api.client.googleapis.json.GoogleJsonResponseException: 504 Gateway
    // Timeout
    var bigQueryProject = BigQueryProject.from(new SnapshotModel().dataProject("data-project"));
    var bigQueryException =
        new BigQueryException(
            500,
            "Project id: datarepo-REDACTED",
            new HttpResponseException.Builder(
                    HttpStatus.SC_GATEWAY_TIMEOUT, "504 Gateway Timeout", new HttpHeaders() {})
                .build());
    assertThrows(
        AclUtils.AclRetryException.class,
        () -> bigQueryProject.bigQueryAclUpdateShouldRetry(bigQueryException));
  }
}
