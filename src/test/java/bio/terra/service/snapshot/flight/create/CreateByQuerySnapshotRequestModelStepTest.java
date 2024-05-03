package bio.terra.service.snapshot.flight.create;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestIdModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestQueryModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import bio.terra.service.snapshotbuilder.SnapshotBuilderTestData;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
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
class CreateByQuerySnapshotRequestModelStepTest {
  @Mock SnapshotRequestDao snapshotRequestDao;
  @Mock SnapshotDao snapshotDao;
  @Mock SnapshotBuilderService snapshotBuilderService;
  @Mock FlightContext flightContext;

  @Test
  void doStep() throws InterruptedException {
    UUID snapshotAccessRequestId = UUID.randomUUID();
    UUID sourceSnapshotId = UUID.randomUUID();
    UUID datasetProfileId = UUID.randomUUID();

    Dataset sourceDataset = new Dataset();
    sourceDataset.name("dataset_name");
    sourceDataset.defaultProfileId(datasetProfileId);

    Snapshot sourceSnapshot = new Snapshot();
    sourceSnapshot.snapshotSources(List.of(new SnapshotSource().dataset(sourceDataset)));

    SnapshotAccessRequestResponse accessRequestResponse =
        SnapshotBuilderTestData.createSnapshotAccessRequestResponse(sourceSnapshotId);
    accessRequestResponse.id(snapshotAccessRequestId);
    accessRequestResponse.sourceSnapshotId(sourceSnapshotId);

    FlightMap workingMap = new FlightMap();

    SnapshotRequestModel requestModel =
        new SnapshotRequestModel()
            .contents(
                List.of(
                    new SnapshotRequestContentsModel()
                        .requestIdSpec(
                            new SnapshotRequestIdModel().snapshotRequestId(UUID.randomUUID()))));
    AuthenticatedUserRequest user = AuthenticationFixtures.randomUserRequest();

    when(snapshotRequestDao.getById(any())).thenReturn(accessRequestResponse);
    when(snapshotDao.retrieveSnapshot(sourceSnapshotId)).thenReturn(sourceSnapshot);
    when(snapshotBuilderService.generateRowIdQuery(accessRequestResponse, sourceSnapshot, user))
        .thenReturn("query");
    when(flightContext.getWorkingMap()).thenReturn(workingMap);

    Step step =
        new CreateByQuerySnapshotRequestModelStep(
            requestModel, snapshotDao, snapshotBuilderService, snapshotRequestDao, user);
    StepResult stepResult = step.doStep(flightContext);

    String expectedQuery = "query";

    SnapshotRequestModel expected =
        new SnapshotRequestModel()
            .name(accessRequestResponse.getSnapshotName())
            .globalFileIds(true)
            .profileId(sourceDataset.getDefaultProfileId())
            .contents(
                List.of(
                    new SnapshotRequestContentsModel()
                        .datasetName(sourceDataset.getName())
                        .mode(SnapshotRequestContentsModel.ModeEnum.BYQUERY)
                        .querySpec(
                            new SnapshotRequestQueryModel()
                                .query(expectedQuery)
                                .assetName("person_visit"))));

    assertTrue(workingMap.containsKey(SnapshotWorkingMapKeys.BY_QUERY_SNAPSHOT_REQUEST_MODEL));
    assertEquals(
        workingMap.get(
            SnapshotWorkingMapKeys.BY_QUERY_SNAPSHOT_REQUEST_MODEL, SnapshotRequestModel.class),
        expected);
    assertEquals(stepResult, StepResult.getStepResultSuccess());
  }
}
