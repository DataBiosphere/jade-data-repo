package bio.terra.service.snapshot.flight.create;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import bio.terra.service.snapshotbuilder.SnapshotBuilderTestData;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class CreateSnapshotByRequestIdAzureStepTest {

  @Mock private FlightContext mockFlightContext;
  @Mock private SnapshotService snapshotService;
  @Mock private SnapshotBuilderService snapshotBuilderService;
  @Mock private SnapshotRequestDao snapshotRequestDao;
  @Mock private SnapshotDao snapshotDao;
  @Mock private AzureSynapsePdao azureSynapsePdao;
  private SnapshotRequestModel snapshotReq;
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final UUID SNAPSHOT_ACCESS_REQUEST_ID = UUID.randomUUID();
  private CreateSnapshotByRequestIdAzureStep createSnapshotByRequestIdAzureStep;

  @BeforeEach
  void setUp() {
    snapshotReq =
        SnapshotBuilderTestData.createSnapshotRequestByRequestId(SNAPSHOT_ACCESS_REQUEST_ID);
    createSnapshotByRequestIdAzureStep =
        spy(
            new CreateSnapshotByRequestIdAzureStep(
                snapshotReq,
                snapshotBuilderService,
                snapshotService,
                snapshotRequestDao,
                snapshotDao,
                TEST_USER,
                azureSynapsePdao));
  }

  @Test
  void doStep() throws Exception {
    Snapshot snapshot = new Snapshot();
    when(snapshotDao.retrieveSnapshotByName("snapshotRequestName")).thenReturn(snapshot);
    // mock out call to prepareAndCreateSnapshot
    doReturn(StepResult.getStepResultSuccess())
        .when(createSnapshotByRequestIdAzureStep)
        .prepareAndCreateSnapshot(
            mockFlightContext,
            snapshot,
            snapshotReq,
            snapshotBuilderService,
            snapshotRequestDao,
            snapshotDao,
            TEST_USER);
    StepResult result = createSnapshotByRequestIdAzureStep.doStep(mockFlightContext);

    assertEquals(StepResult.getStepResultSuccess(), result);
    assertEquals(
        "target_data_source_null", createSnapshotByRequestIdAzureStep.targetDataSourceName);
    assertEquals(
        "source_dataset_data_source_null",
        createSnapshotByRequestIdAzureStep.sourceDatasetDataSourceName);
  }

  @Test
  void createSnapshot() throws SQLException, InterruptedException {
    FlightMap workingMap = new FlightMap();
    Map<String, Long> rowMap = new HashMap<>();
    UUID snapshotId = UUID.randomUUID();
    Snapshot snapshot = new Snapshot().id(snapshotId).compactIdPrefix("prefix").globalFileIds(true);
    AssetSpecification asset = new AssetSpecification();
    createSnapshotByRequestIdAzureStep.sourceDatasetDataSourceName = "source_dataset_data_source";
    createSnapshotByRequestIdAzureStep.targetDataSourceName = "target_data_source";

    when(mockFlightContext.getWorkingMap()).thenReturn(workingMap);
    when(azureSynapsePdao.createSnapshotParquetFilesByQuery(
            asset,
            snapshotId,
            "source_dataset_data_source",
            "target_data_source",
            "sqlQuery",
            false,
            "prefix"))
        .thenReturn(rowMap);
    StepResult result =
        createSnapshotByRequestIdAzureStep.createSnapshot(
            mockFlightContext, asset, snapshot, "sqlQuery", Instant.now());
    assertTrue(workingMap.containsKey(SnapshotWorkingMapKeys.TABLE_ROW_COUNT_MAP));
    assertEquals(workingMap.get(SnapshotWorkingMapKeys.TABLE_ROW_COUNT_MAP, Map.class), rowMap);
    assertEquals(result, StepResult.getStepResultSuccess());
  }
}
