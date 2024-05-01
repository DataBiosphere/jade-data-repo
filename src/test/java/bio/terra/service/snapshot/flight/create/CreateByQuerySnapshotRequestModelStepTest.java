package bio.terra.service.snapshot.flight.create;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.CloudPlatform;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotBuilderCohort;
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
import bio.terra.service.snapshotbuilder.SnapshotBuilderSettingsDao;
import bio.terra.service.snapshotbuilder.SnapshotBuilderTestData;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import bio.terra.service.snapshotbuilder.utils.QueryBuilderFactory;
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
@Tag("bio.terra.common.category.Unit")
class CreateByQuerySnapshotRequestModelStepTest {
  @Mock SnapshotRequestDao snapshotRequestDao;
  @Mock SnapshotDao snapshotDao;
  @Mock SnapshotBuilderSettingsDao snapshotBuilderSettingsDao;
  @Mock SnapshotBuilderService snapshotBuilderService;
  @Mock FlightContext flightContext;

  @Test
  void doStep() throws InterruptedException {
    SnapshotAccessRequestResponse accessRequestResponse =
        SnapshotBuilderTestData.createSnapshotAccessRequestResponse();
    UUID datasetId = UUID.randomUUID();
    UUID snapshotAccessRequestId = UUID.randomUUID();
    UUID sourceSnapshotId = UUID.randomUUID();
    Dataset dataset = new Dataset();
    accessRequestResponse.datasetId(datasetId);
    accessRequestResponse.id(snapshotAccessRequestId);
    accessRequestResponse.sourceSnapshotId(sourceSnapshotId);
    dataset.id(datasetId);
    dataset.name("dataset_name");
    Snapshot snapshot = new Snapshot();
    snapshot.snapshotSources(List.of(new SnapshotSource().dataset(dataset)));
    SqlRenderContext sqlRenderContext =
        new SqlRenderContext(s -> s, CloudPlatformWrapper.of(CloudPlatform.GCP));
    SqlRenderContext sqlRenderContext2 =
        new SqlRenderContext(s -> s, CloudPlatformWrapper.of(CloudPlatform.GCP));
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
    when(snapshotDao.retrieveSnapshot(sourceSnapshotId)).thenReturn(snapshot);
    when(snapshotBuilderSettingsDao.getBySnapshotId(sourceSnapshotId))
        .thenReturn(SnapshotBuilderTestData.SETTINGS);
    when(snapshotBuilderService.createContext(any(), any())).thenReturn(sqlRenderContext);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);

    Step step =
        new CreateByQuerySnapshotRequestModelStep(
            requestModel,
            snapshotDao,
            snapshotBuilderService,
            snapshotBuilderSettingsDao,
            snapshotRequestDao,
            user);
    StepResult stepResult = step.doStep(flightContext);

    // tested in criteriaQueryBuilderTest
    String expectedQuery =
        new QueryBuilderFactory()
            .criteriaQueryBuilder("person", SnapshotBuilderTestData.SETTINGS)
            .generateRowIdQueryForCriteriaGroupsList(
                accessRequestResponse.getSnapshotSpecification().getCohorts().stream()
                    .map(SnapshotBuilderCohort::getCriteriaGroups)
                    .toList())
            .renderSQL(sqlRenderContext2);

    SnapshotRequestModel expected =
        new SnapshotRequestModel()
            .name(accessRequestResponse.getSnapshotName())
            .globalFileIds(true)
            .profileId(dataset.getDefaultProfileId())
            .contents(
                List.of(
                    new SnapshotRequestContentsModel()
                        .datasetName(dataset.getName())
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
