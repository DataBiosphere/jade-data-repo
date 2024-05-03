package bio.terra.service.snapshot.flight.lock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.ResourceLocks;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SnapshotLockSetResponseStepTest {
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private SnapshotLockSetResponseStep step;
  @Mock private SnapshotService snapshotService;
  @Mock private FlightContext flightContext;

  @Test
  void doStep() throws InterruptedException {
    // Setup
    step = new SnapshotLockSetResponseStep(snapshotService, SNAPSHOT_ID);
    when(flightContext.getWorkingMap()).thenReturn(new FlightMap());
    var lockName = "lock123";
    var locks = new ResourceLocks().exclusive(lockName);
    var snapshotSummaryModel = new SnapshotSummaryModel().resourceLocks(locks);
    when(snapshotService.retrieveSnapshotSummary(SNAPSHOT_ID)).thenReturn(snapshotSummaryModel);

    // Perform Step
    step.doStep(flightContext);

    // Confirm Response is correctly set
    FlightMap workingMap = flightContext.getWorkingMap();
    assertThat(
        "Response is the ResourceLocks object",
        workingMap.get(JobMapKeys.RESPONSE.getKeyName(), ResourceLocks.class),
        equalTo(locks));
    assertThat(
        "Response Status is HttpStatus.OK",
        workingMap.get(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.class),
        equalTo(HttpStatus.OK));
  }
}
