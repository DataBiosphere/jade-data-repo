package bio.terra.service.snapshot.flight.delete;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.exception.NotFoundException;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.snapshotbuilder.SnapshotAccessRequestModel;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class DeleteSnapshotDeleteSamGroupStepTest {
  @Mock IamService iamService;
  @Mock SnapshotRequestDao snapshotRequestDao;
  private UUID snapshotId;
  @Mock private FlightContext flightContext;
  private DeleteSnapshotDeleteSamGroupStep step;
  private static final String EXPECTED_NAME = "samGroupName";
  private static final SnapshotAccessRequestModel SNAPSHOT_ACCESS_REQUEST_MODEL =
      new SnapshotAccessRequestModel(
          null, null, null, null, null, null, null, null, null, null, null, EXPECTED_NAME, null);

  @BeforeEach
  void beforeEach() {
    snapshotId = UUID.randomUUID();
    step = new DeleteSnapshotDeleteSamGroupStep(iamService, snapshotRequestDao, snapshotId);
  }

  @Test
  void doStep() throws InterruptedException {
    when(snapshotRequestDao.getByCreatedSnapshotId(snapshotId))
        .thenReturn(SNAPSHOT_ACCESS_REQUEST_MODEL);
    var result = step.doStep(flightContext);
    verify(iamService).deleteGroup(EXPECTED_NAME);
    assertThat(result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
  }

  @Test
  void doStepSnapshotNotByRequestId() throws InterruptedException {
    when(snapshotRequestDao.getByCreatedSnapshotId(snapshotId)).thenThrow(NotFoundException.class);
    var result = step.doStep(flightContext);
    verify(iamService, never()).deleteGroup(any());
    assertThat(result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
  }
}
