package bio.terra.service.snapshot.flight.delete;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.EnumerateSnapshotAccessRequest;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotAccessRequestStatus;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepStatus;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class DeleteOutstandingSnapshotAccessRequestsStepTest {
  @Mock SnapshotBuilderService snapshotBuilderService;
  private UUID snapshotId;
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  @Mock private FlightContext flightContext;
  private DeleteOutstandingSnapshotAccessRequestsStep step;

  @BeforeEach
  void beforeEach() {
    snapshotId = UUID.randomUUID();
    step =
        new DeleteOutstandingSnapshotAccessRequestsStep(
            TEST_USER, snapshotId, snapshotBuilderService);
  }

  @Test
  void doStep() throws InterruptedException {
    UUID firstRequestId = UUID.randomUUID();
    UUID secondRequestId = UUID.randomUUID();
    UUID thirdRequestId = UUID.randomUUID();
    when(snapshotBuilderService.enumerateRequestsBySnapshot(snapshotId))
        .thenReturn(
            new EnumerateSnapshotAccessRequest()
                .items(
                    List.of(
                        new SnapshotAccessRequestResponse().id(firstRequestId),
                        new SnapshotAccessRequestResponse().id(secondRequestId),
                        new SnapshotAccessRequestResponse()
                            .id(thirdRequestId)
                            .status(SnapshotAccessRequestStatus.DELETED))));
    var result = step.doStep(flightContext);
    verify(snapshotBuilderService).deleteRequest(TEST_USER, firstRequestId);
    verify(snapshotBuilderService).deleteRequest(TEST_USER, secondRequestId);
    verify(snapshotBuilderService, times(0)).deleteRequest(TEST_USER, thirdRequestId);
    assertThat(result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
  }

  @Test
  void doStepNoRequests() throws InterruptedException {
    when(snapshotBuilderService.enumerateRequestsBySnapshot(snapshotId))
        .thenReturn(new EnumerateSnapshotAccessRequest().items(List.of()));
    var result = step.doStep(flightContext);
    verify(snapshotBuilderService, never()).deleteRequest(any(), any());
    assertThat(result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
  }
}
