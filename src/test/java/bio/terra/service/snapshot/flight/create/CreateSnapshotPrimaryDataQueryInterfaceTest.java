package bio.terra.service.snapshot.flight.create;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestIdModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotSource;
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
class CreateSnapshotPrimaryDataQueryInterfaceTest {
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  @Mock FlightContext flightContext;
  @Mock DatasetService datasetService;
  @Mock SnapshotBuilderService snapshotBuilderService;
  @Mock SnapshotRequestDao snapshotRequestDao;
  @Mock SnapshotDao snapshotDao;

  @Mock CreateSnapshotPrimaryDataQueryInterface createSnapshotPrimaryDataQueryInterface;

  @Test
  void prepareQueryAndCreateSnapshotByRequestId() throws InterruptedException {
    UUID snapshotAccessRequestId = UUID.randomUUID();
    UUID sourceSnapshotId = UUID.randomUUID();
    UUID datasetProfileId = UUID.randomUUID();
    UUID datasetId = UUID.randomUUID();
    AssetSpecification asset = new AssetSpecification().name("concept_asset");
    String sqlQuery = "query";

    Dataset sourceDataset = new Dataset();
    sourceDataset.name("dataset_name");
    sourceDataset.id(datasetId);
    sourceDataset.defaultProfileId(datasetProfileId);
    sourceDataset.assetSpecifications(List.of(asset));

    Snapshot snapshot = new Snapshot();

    Snapshot sourceSnapshot = new Snapshot();
    sourceSnapshot.snapshotSources(List.of(new SnapshotSource().dataset(sourceDataset)));

    SnapshotAccessRequestResponse accessRequestResponse =
        SnapshotBuilderTestData.createSnapshotAccessRequestResponse(sourceSnapshotId);
    accessRequestResponse.id(snapshotAccessRequestId);

    SnapshotRequestModel requestModel =
        new SnapshotRequestModel()
            .contents(
                List.of(
                    new SnapshotRequestContentsModel()
                        .mode(SnapshotRequestContentsModel.ModeEnum.BYREQUESTID)
                        .requestIdSpec(
                            new SnapshotRequestIdModel()
                                .snapshotRequestId(snapshotAccessRequestId))));

    when(snapshotRequestDao.getById(snapshotAccessRequestId)).thenReturn(accessRequestResponse);
    when(snapshotDao.retrieveSnapshot(sourceSnapshotId)).thenReturn(sourceSnapshot);
    when(snapshotBuilderService.generateRowIdQuery(
            accessRequestResponse, sourceSnapshot, TEST_USER))
        .thenReturn(sqlQuery);

    // mock out call to createSnapshotPrimaryData
    when(createSnapshotPrimaryDataQueryInterface.createSnapshotPrimaryData(
            eq(flightContext), eq(asset), eq(snapshot), eq(sqlQuery), any()))
        .thenReturn(StepResult.getStepResultSuccess());
    when(createSnapshotPrimaryDataQueryInterface.prepareQueryAndCreateSnapshot(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenCallRealMethod();

    assertThat(
        createSnapshotPrimaryDataQueryInterface.prepareQueryAndCreateSnapshot(
            flightContext,
            snapshot,
            requestModel,
            datasetService,
            snapshotBuilderService,
            snapshotRequestDao,
            snapshotDao,
            TEST_USER),
        is(StepResult.getStepResultSuccess()));
  }
}
