package bio.terra.service.snapshot.flight.create;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestIdModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.time.Instant;
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
class CreateSnapshotByRequestIdGcpStepTest {
  @Mock private FlightContext flightContext;
  @Mock private SnapshotRequestModel snapshotReq;
  @Mock private SnapshotService snapshotService;
  @Mock private SnapshotBuilderService snapshotBuilderService;
  @Mock private SnapshotRequestDao snapshotRequestDao;
  @Mock private SnapshotDao snapshotDao;
  @Mock private BigQuerySnapshotPdao bigQuerySnapshotPdao;
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private final Snapshot snapshot = new Snapshot();
  private static final UUID SNAPSHOT_ACCESS_REQUEST_ID = UUID.randomUUID();
  private CreateSnapshotByRequestIdGcpStep createSnapshotByRequestIdGcpStep;

  @BeforeEach
  void setUp() {
    snapshotReq =
        new SnapshotRequestModel()
            .name("snapshotRequestName")
            .contents(
                List.of(
                    new SnapshotRequestContentsModel()
                        .mode(SnapshotRequestContentsModel.ModeEnum.BYREQUESTID)
                        .requestIdSpec(
                            new SnapshotRequestIdModel()
                                .snapshotRequestId(SNAPSHOT_ACCESS_REQUEST_ID))));
    createSnapshotByRequestIdGcpStep =
        spy(
            new CreateSnapshotByRequestIdGcpStep(
                snapshotReq,
                snapshotService,
                snapshotBuilderService,
                snapshotRequestDao,
                snapshotDao,
                TEST_USER,
                bigQuerySnapshotPdao));
  }

  @Test
  void createSnapshot() throws InterruptedException {
    AssetSpecification assetSpecification = new AssetSpecification();
    String sqlQuery = "sqlQuery";
    Instant filterBefore = Instant.now();
    StepResult result =
        createSnapshotByRequestIdGcpStep.createSnapshot(
            flightContext, assetSpecification, snapshot, sqlQuery, filterBefore);
    verify(bigQuerySnapshotPdao)
        .queryForRowIds(assetSpecification, snapshot, sqlQuery, filterBefore);
    assertEquals(result, StepResult.getStepResultSuccess());
  }

  @Test
  void doStep() throws InterruptedException {
    when(snapshotDao.retrieveSnapshotByName(snapshotReq.getName())).thenReturn(snapshot);
    // mock out call to prepareAndCreateSnapshot
    doReturn(StepResult.getStepResultSuccess())
        .when(createSnapshotByRequestIdGcpStep)
        .prepareAndCreateSnapshot(
            flightContext,
            snapshot,
            snapshotReq,
            snapshotBuilderService,
            snapshotRequestDao,
            snapshotDao,
            TEST_USER);
    StepResult result = createSnapshotByRequestIdGcpStep.doStep(flightContext);
    assertEquals(result, StepResult.getStepResultSuccess());
  }

  @Test
  void undoStep() throws InterruptedException {
    StepResult result = createSnapshotByRequestIdGcpStep.undoStep(flightContext);
    verify(snapshotService).undoCreateSnapshot(snapshotReq.getName());
    assertEquals(result, StepResult.getStepResultSuccess());
  }
}
