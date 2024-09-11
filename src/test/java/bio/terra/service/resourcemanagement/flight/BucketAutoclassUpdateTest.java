package bio.terra.service.resourcemanagement.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.mock;

import bio.terra.common.FlightTestUtils;
import bio.terra.common.category.Unit;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightMap;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;

@Tag(Unit.TAG)
class BucketAutoclassUpdateTest {

  @Test
  void testBucketAutoclassUpdate() {
    FlightMap inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.BUCKET_NAME.getKeyName(), "testBucketName");

    var flight = new BucketAutoclassUpdateFlight(inputParameters, mock(ApplicationContext.class));
    var steps = FlightTestUtils.getStepNames(flight);

    assertThat(
        steps,
        contains(
            "RecordBucketAutoclassStep",
            "UpdateBucketAutoclassStep",
            "UpdateDatabaseAutoclassStep"));
  }
}
