package bio.terra.service.snapshot.flight.create;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.FlightTestUtils;
import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightMap;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationContext;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SnapshotCreateFlightTest {

  @Mock private ApplicationContext context;
  private FlightMap inputParameters;
  private static final List<String> DATA_ACCESS_CONTROL_GROUPS = List.of("group1", "group2");

  @BeforeEach
  void beforeEach() {
    ApplicationConfiguration appConfig = mock(ApplicationConfiguration.class);
    when(appConfig.getMaxStairwayThreads()).thenReturn(1);

    SnapshotService snapshotService = mock(SnapshotService.class);
    DatasetSummary datasetSummary = mock(DatasetSummary.class);

    when(context.getBean(any(Class.class))).thenReturn(null);
    when(context.getBean(anyString(), any(Class.class))).thenReturn(null);
    // Beans that are interacted with directly in flight construction rather than simply passed
    // to steps need to be added to our context mock.
    when(context.getBean(ApplicationConfiguration.class)).thenReturn(appConfig);
    when(context.getBean(SnapshotService.class)).thenReturn(snapshotService);

    inputParameters = new FlightMap();
    SnapshotRequestModel request =
        new SnapshotRequestModel()
            .dataAccessControlGroups(DATA_ACCESS_CONTROL_GROUPS)
            .addContentsItem(
                new SnapshotRequestContentsModel()
                    .mode(SnapshotRequestContentsModel.ModeEnum.BYFULLVIEW));
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), request);
    inputParameters.put(JobMapKeys.SOURCE_DATASET.getKeyName(), new Dataset(datasetSummary));
    inputParameters.put(JobMapKeys.SNAPSHOT_ID.getKeyName(), UUID.randomUUID());
  }

  @Test
  void testSnapshotCreateFlight() {
    var flight = new SnapshotCreateFlight(inputParameters, context);

    assertThat(
        "Snapshot creation flight locks resources, then unlocks them, then writes response",
        FlightTestUtils.getStepNames(flight),
        containsInRelativeOrder(
            "LockDatasetStep",
            "CreateSnapshotMetadataStep", // Also locks the snapshot
            "CreateSnapshotGroupConstraintPolicyStep",
            "AddSnapshotAuthDomainStep",
            "UnlockSnapshotStep",
            "UnlockDatasetStep",
            "CreateSnapshotSetResponseStep"));

    LockDatasetStep lockDatasetStep =
        FlightTestUtils.getStepWithClass(flight, LockDatasetStep.class);
    assertThat(
        "Snapshot creation flight obtains shared dataset lock",
        lockDatasetStep.isSharedLock(),
        is(true));
    assertThat(
        "Dataset lock step does not suppress 'dataset not found' exceptions",
        lockDatasetStep.shouldSuppressNotFoundException(),
        is(false));

    UnlockDatasetStep unlockDatasetStep =
        FlightTestUtils.getStepWithClass(flight, UnlockDatasetStep.class);
    assertThat(
        "Snapshot creation flight removes shared dataset lock",
        unlockDatasetStep.isSharedLock(),
        is(true));
  }
}
