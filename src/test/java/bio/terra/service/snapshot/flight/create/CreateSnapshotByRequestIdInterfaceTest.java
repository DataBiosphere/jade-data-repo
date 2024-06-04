package bio.terra.service.snapshot.flight.create;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import bio.terra.common.Column;
import bio.terra.common.Relationship;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetTable;
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
import org.junit.jupiter.api.BeforeEach;
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
  @Mock SnapshotBuilderService snapshotBuilderService;
  @Mock SnapshotRequestDao snapshotRequestDao;
  @Mock SnapshotDao snapshotDao;

  @Mock CreateSnapshotByRequestIdInterface createSnapshotByRequestIdInterface;

  private UUID snapshotAccessRequestId;
  private UUID sourceSnapshotId;
  private Dataset sourceDataset;

  @BeforeEach
  public void setup() {
    snapshotAccessRequestId = UUID.randomUUID();
    sourceSnapshotId = UUID.randomUUID();
    UUID datasetProfileId = UUID.randomUUID();
    UUID datasetId = UUID.randomUUID();

    sourceDataset = new Dataset();
    sourceDataset.name("dataset_name");
    sourceDataset.id(datasetId);
    sourceDataset.defaultProfileId(datasetProfileId);
    sourceDataset.tables(
        List.of(
            new DatasetTable().name("drug_exposure"),
            new DatasetTable().name("person").columns(List.of(new Column().name("person_id"))),
            new DatasetTable().name("concept").columns(List.of(new Column().name("concept_id")))));
    sourceDataset.relationships(
        List.of(
            new Relationship().name("fpk_drug_person"),
            new Relationship().name("fpk_drug_type_concept"),
            new Relationship().name("fpk_drug_concept"),
            new Relationship().name("fpk_drug_route_concept"),
            new Relationship().name("fpk_drug_concept_s")));
  }

  @Test
  void prepareAndCreateSnapshot() throws InterruptedException {
    String sqlQuery = "query";

    Snapshot snapshot = new Snapshot();

    Snapshot sourceSnapshot = new Snapshot();
    sourceSnapshot.snapshotSources(List.of(new SnapshotSource().dataset(sourceDataset)));

    SnapshotAccessRequestResponse accessRequestResponse =
        SnapshotBuilderTestData.createSnapshotAccessRequestResponse(sourceSnapshotId);
    accessRequestResponse.id(snapshotAccessRequestId);

    SnapshotRequestModel requestModel =
        SnapshotBuilderTestData.createSnapshotRequestByRequestId(snapshotAccessRequestId);

    when(snapshotRequestDao.getById(snapshotAccessRequestId)).thenReturn(accessRequestResponse);
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
            snapshotBuilderService,
            snapshotRequestDao,
            snapshotDao,
            TEST_USER),
        is(StepResult.getStepResultSuccess()));
  }

  @Test
  void pullTables() {
    var accessRequestResponse =
        SnapshotBuilderTestData.createSnapshotAccessRequestResponse(sourceSnapshotId);
    accessRequestResponse.id(snapshotAccessRequestId);

    assertThat(
        CreateSnapshotByRequestIdInterface.pullTables(accessRequestResponse)
            .get(0)
            .getDatasetTableName(),
        is("drug_exposure"));
  }

  @Test
  void buildAssetFromSnapshotAccessRequest() {
    var accessRequestResponse =
        SnapshotBuilderTestData.createSnapshotAccessRequestResponse(sourceSnapshotId);
    accessRequestResponse.id(snapshotAccessRequestId);

    var actualAssetSpec =
        CreateSnapshotByRequestIdInterface.buildAssetFromSnapshotAccessRequest(
            sourceDataset, accessRequestResponse);
    assertThat(actualAssetSpec.getName(), containsString("snapshot-by-request-asset-"));
    assertThat(actualAssetSpec.getRootTable().getTable().getName(), is("person"));
    assertThat(actualAssetSpec.getRootColumn().getDatasetColumn().getName(), is("person_id"));
    assertThat(
        actualAssetSpec.getAssetRelationships().stream()
            .map(r -> r.getDatasetRelationship().getName())
            .toList(),
        containsInAnyOrder(
            "fpk_drug_person",
            "fpk_drug_type_concept",
            "fpk_drug_concept",
            "fpk_drug_route_concept",
            "fpk_drug_concept_s"));
    assertThat(
        actualAssetSpec.getAssetTables().stream().map(at -> at.getTable().getName()).toList(),
        contains("person", "concept", "drug_exposure"));
  }
}
