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
  void gatewayTimeoutShouldRetry() {
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

  @Test
  void serviceUnavailableShouldRetry() {
    // mock an exception that looks like the following:
    //    Caused by: com.google.cloud.bigquery.BigQueryException: Visibility check was unavailable.
    //    Please retry the request and contact support if the problem persists
    //      at
    // com.google.cloud.bigquery.BigQueryRetryHelper.runWithRetries(BigQueryRetryHelper.java:59)
    //      at com.google.cloud.bigquery.BigQueryImpl.getDataset(BigQueryImpl.java:500)
    //      at
    // bio.terra.service.tabulardata.google.BigQueryProject.lambda$getBQDataset$1(BigQueryProject.java:214)
    //      at bio.terra.common.AclUtils.aclUpdateRetry(AclUtils.java:28)
    //	... 16 common frames omitted
    //    Caused by: com.google.api.client.googleapis.json.GoogleJsonResponseException: 503 Service
    // Unavailable
    var bigQueryProject = BigQueryProject.from(new SnapshotModel().dataProject("data-project"));
    var bigQueryException =
        new BigQueryException(
            500,
            "Visibility check was unavailable",
            new HttpResponseException.Builder(
                    HttpStatus.SC_SERVICE_UNAVAILABLE,
                    "503 Service Unavailable",
                    new HttpHeaders() {})
                .build());
    assertThrows(
        AclUtils.AclRetryException.class,
        () -> bigQueryProject.bigQueryAclUpdateShouldRetry(bigQueryException));
  }
}
