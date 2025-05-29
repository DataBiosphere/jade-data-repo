package bio.terra.service.snapshot.flight.create;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import bio.terra.common.category.Unit;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SnapshotAuthzServiceAccountConsumerStepTest {
  @Mock private SnapshotService snapshotService;
  @Mock private ResourceService resourceService;
  @Mock private IamService iamService;
  @Mock private FlightContext flightContext;

  private static final String SNAPSHOT_NAME = "snapshotName";
  private static final String GOOGLE_PROJECT_ID = "google project id";
  private static final String TDR_SERVICE_ACCOUNT_EMAIL = "tdr-service-account-email";
  private static final Snapshot SNAPSHOT =
      new Snapshot()
          .snapshotSources(
              List.of(
                  new SnapshotSource()
                      .dataset(
                          new Dataset()
                              .projectResource(
                                  new GoogleProjectResource()
                                      .serviceAccount(TDR_SERVICE_ACCOUNT_EMAIL)))))
          .projectResource(new GoogleProjectResource().googleProjectId(GOOGLE_PROJECT_ID));

  private final List<String> addedEmails = new ArrayList<>();
  private SnapshotAuthzServiceAccountConsumerStep step;

  @BeforeEach
  void beforeEach() throws Exception {
    FlightMap workingMap = new FlightMap();
    when(flightContext.getWorkingMap()).thenReturn(workingMap);

    var policyMap = new EnumMap<>(IamRole.class);
    policyMap.put(IamRole.STEWARD, "steward");
    policyMap.put(IamRole.READER, "reader");
    workingMap.put(SnapshotWorkingMapKeys.POLICY_MAP, policyMap);

    var sourceDatasetPolicyMap = new EnumMap<>(IamRole.class);
    sourceDatasetPolicyMap.put(IamRole.CUSTODIAN, "datasetCustodian");
    sourceDatasetPolicyMap.put(IamRole.STEWARD, "datasetSteward");
    workingMap.put(SnapshotWorkingMapKeys.SOURCE_DATASET_POLICY_MAP, sourceDatasetPolicyMap);
    when(snapshotService.retrieveByName(SNAPSHOT_NAME)).thenReturn(SNAPSHOT);

    doAnswer(
            invocation -> {
              List<String> emails = invocation.getArgument(1);
              addedEmails.addAll(emails);
              return null;
            })
        .when(resourceService)
        .grantPoliciesServiceUsageConsumer(eq(GOOGLE_PROJECT_ID), anyList());
  }

  @Test
  void doStep() throws Exception {
    step =
        new SnapshotAuthzServiceAccountConsumerStep(
            snapshotService,
            resourceService,
            SNAPSHOT_NAME,
            TDR_SERVICE_ACCOUNT_EMAIL,
            new Dataset());
    assertThat(step.doStep(flightContext), is(StepResult.getStepResultSuccess()));

    assertThat(addedEmails, containsInAnyOrder("steward", "reader"));
    verifyNoMoreInteractions(resourceService);
    verifyNoInteractions(iamService);
  }

  @Test
  void doStepAddServiceAccount() throws Exception {
    var DEDICATED_SERVICE_ACCOUNT_EMAIL = "different";
    var SNAPSHOT_DEDICATED_SA =
        SNAPSHOT.snapshotSources(
            List.of(
                new SnapshotSource()
                    .dataset(
                        new Dataset()
                            .projectResource(
                                new GoogleProjectResource()
                                    .serviceAccount(DEDICATED_SERVICE_ACCOUNT_EMAIL)))));
    when(snapshotService.retrieveByName(SNAPSHOT_NAME)).thenReturn(SNAPSHOT_DEDICATED_SA);
    step =
        new SnapshotAuthzServiceAccountConsumerStep(
            snapshotService,
            resourceService,
            SNAPSHOT_NAME,
            TDR_SERVICE_ACCOUNT_EMAIL,
            new Dataset());
    assertThat(step.doStep(flightContext), is(StepResult.getStepResultSuccess()));

    assertThat(
        addedEmails, containsInAnyOrder("steward", "reader", DEDICATED_SERVICE_ACCOUNT_EMAIL));
    verifyNoMoreInteractions(resourceService);
    verifyNoInteractions(iamService);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void doStepInheritSteward(boolean inheritSteward) throws Exception {
    var sourceDataset =
        new Dataset(new DatasetSummary().inheritSteward(inheritSteward)).id(UUID.randomUUID());
    step =
        new SnapshotAuthzServiceAccountConsumerStep(
            snapshotService,
            resourceService,
            SNAPSHOT_NAME,
            TDR_SERVICE_ACCOUNT_EMAIL,
            sourceDataset);
    assertThat(step.doStep(flightContext), is(StepResult.getStepResultSuccess()));
    if (inheritSteward) {
      assertThat(
          addedEmails,
          containsInAnyOrder("steward", "reader", "datasetCustodian", "datasetSteward"));
    } else {
      assertThat(addedEmails, containsInAnyOrder("steward", "reader"));
    }
  }

  @Test
  void undoStep() throws InterruptedException {
    reset(snapshotService, flightContext, resourceService);
    step =
        new SnapshotAuthzServiceAccountConsumerStep(
            snapshotService,
            resourceService,
            SNAPSHOT_NAME,
            TDR_SERVICE_ACCOUNT_EMAIL,
            new Dataset());
    assertThat(step.undoStep(flightContext), is(StepResult.getStepResultSuccess()));
    verifyNoInteractions(snapshotService, resourceService, iamService, flightContext);
  }
}
