package bio.terra.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.ErrorModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import com.google.cloud.bigquery.BigQueryException;
import java.util.Objects;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

@Tag(Unit.TAG)
class FlightUtilsTest {

  @Test
  void handleGcpAclException() throws Exception {
    FlightContext context = mock(FlightContext.class);
    FlightMap flightMap = new FlightMap();
    when(context.getWorkingMap()).thenReturn(flightMap);

    FlightUtils.handleGcpAclException(context, () -> {});
    verifyNoInteractions(context);

    assertThrows(
        RuntimeException.class,
        () ->
            FlightUtils.handleGcpAclException(
                context,
                () -> {
                  throw new GoogleResourceException("Test exception", new BigQueryException(0, ""));
                }));
    verifyNoInteractions(context);

    assertThrows(
        GoogleResourceException.class,
        () ->
            FlightUtils.handleGcpAclException(
                context,
                () -> {
                  throw new GoogleResourceException(
                      "Error while performing ACL update",
                      new BigQueryException(
                          0,
                          "Too many authorized entities in this dataset. The maximum number of authorized views, routines, and datasets combined is 2500."));
                }));
    var status = flightMap.get(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.class);
    assertThat(status, is(HttpStatus.BAD_REQUEST));
    var errorModel = flightMap.get(JobMapKeys.RESPONSE.getKeyName(), ErrorModel.class);
    assertThat(
        Objects.requireNonNull(errorModel).getMessage(),
        containsString("Resolve this by deleting snapshots or creating a second dataset"));
  }
}
