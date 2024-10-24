package bio.terra.service.snapshot.flight.create;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshotbuilder.SnapshotAccessRequestModel;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import bio.terra.service.snapshotbuilder.SnapshotBuilderTestData;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class CreateSnapshotByRequestIdInterfaceTest {

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  @Mock FlightContext flightContext;
  @Mock SnapshotService snapshotService;
  @Mock SnapshotBuilderService snapshotBuilderService;
  @Mock SnapshotRequestDao snapshotRequestDao;
  @Mock SnapshotDao snapshotDao;

  @Mock CreateSnapshotByRequestIdInterface createSnapshotByRequestIdInterface;

  @Test
  void prepareAndCreateSnapshot() throws InterruptedException {
    UUID sourceSnapshotId = UUID.randomUUID();
    UUID datasetProfileId = UUID.randomUUID();
    UUID datasetId = UUID.randomUUID();
    String sqlQuery = "query";

    Dataset sourceDataset = new Dataset();
    sourceDataset.name("dataset_name");
    sourceDataset.id(datasetId);
    sourceDataset.defaultProfileId(datasetProfileId);

    Snapshot snapshot = new Snapshot();

    Snapshot sourceSnapshot = new Snapshot();
    sourceSnapshot.snapshotSources(List.of(new SnapshotSource().dataset(sourceDataset)));

    SnapshotAccessRequestModel accessRequestResponse =
        SnapshotBuilderTestData.createSnapshotAccessRequestModel(sourceSnapshotId);
    UUID snapshotAccessRequestId = accessRequestResponse.id();

    SnapshotRequestModel requestModel =
        SnapshotBuilderTestData.createSnapshotRequestByRequestId(snapshotAccessRequestId);

    when(snapshotService.getSnapshotAccessRequestById(snapshotAccessRequestId))
        .thenReturn(accessRequestResponse);
    when(snapshotDao.retrieveSnapshot(sourceSnapshotId)).thenReturn(sourceSnapshot);
    when(snapshotBuilderService.generateRowIdQuery(
            accessRequestResponse, sourceSnapshot, TEST_USER))
        .thenReturn(sqlQuery);

    // mock out call to createSnapshotPrimaryData
    when(createSnapshotByRequestIdInterface.createSnapshot(
            eq(flightContext), any(), eq(snapshot), eq(sqlQuery), any()))
        .thenReturn(StepResult.getStepResultSuccess());
    when(createSnapshotByRequestIdInterface.prepareAndCreateSnapshot(
            any(), any(), any(), any(), any(), any(), any()))
        .thenCallRealMethod();

    assertThat(
        createSnapshotByRequestIdInterface.prepareAndCreateSnapshot(
            flightContext,
            snapshot,
            requestModel,
            snapshotService,
            snapshotBuilderService,
            snapshotDao,
            TEST_USER),
        is(StepResult.getStepResultSuccess()));
  }
}
