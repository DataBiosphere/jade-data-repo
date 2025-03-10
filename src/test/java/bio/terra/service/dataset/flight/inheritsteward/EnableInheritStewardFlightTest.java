package bio.terra.service.dataset.flight.inheritsteward;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

import bio.terra.common.FlightTestUtils;
import bio.terra.common.category.Unit;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import bio.terra.stairway.FlightMap;
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
class EnableInheritStewardFlightTest {

  @Mock private ApplicationContext context;
  @Mock private DatasetDao datasetDao;
  @Mock private ResourceService resourceService;
  @Mock private SnapshotService snapshotService;
  @Mock private BigQuerySnapshotPdao bigQuerySnapshotPdao;
  private final FlightMap inputParameters = new FlightMap();

  private static final String CUSTODIAN_EMAIL = "custodian email";
  private static final UUID DATASET_ID = UUID.randomUUID();

  @BeforeEach
  void setUp() {
    inputParameters.put(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), DATASET_ID);
    inputParameters.put(JobMapKeys.CUSTODIAN_EMAIL.getKeyName(), CUSTODIAN_EMAIL);
    when(context.getBean(DatasetDao.class)).thenReturn(datasetDao);
    when(context.getBean(ResourceService.class)).thenReturn(resourceService);
    when(context.getBean(SnapshotService.class)).thenReturn(snapshotService);
    when(context.getBean(BigQuerySnapshotPdao.class)).thenReturn(bigQuerySnapshotPdao);
  }

  @Test
  void allSteps() {
    var flight = new EnableInheritStewardFlight(inputParameters, context);
    var steps = FlightTestUtils.getStepNames(flight);
    assertThat(
        steps,
        contains(
            "InheritStewardSetFlagStep",
            "GetSnapshotGoogleProjectIdsStep",
            "SetAuthBqJobUserStep",
            "SetAuthTabluarAclStep"));
  }

  @Test
  void inheritStewardSetFlagStep() {
    try (var mockStep =
        mockConstruction(
            InheritStewardSetFlagStep.class,
            (mock, context) -> {
              assertThat((DatasetDao) context.arguments().get(0), equalTo(datasetDao));
              assertThat(
                  "The correct datasetId is passed to the step",
                  (UUID) context.arguments().get(1),
                  equalTo(DATASET_ID));
              assertThat(
                  "The correct boolean flag is passed to the step",
                  (boolean) context.arguments().get(2),
                  equalTo(true));
            })) {
      //noinspection ResultOfObjectAllocationIgnored
      new EnableInheritStewardFlight(inputParameters, context);
      assertThat(mockStep.constructed(), hasSize(1));
    }
  }

  @Test
  void getSnapshotGoogleProjectIdsStep() {
    try (var mockStep =
        mockConstruction(
            GetSnapshotGoogleProjectIdsStep.class,
            (mock, context) -> {
              assertThat((SnapshotService) context.arguments().get(0), equalTo(snapshotService));
              assertThat(
                  "The correct datasetId is passed to the step",
                  (UUID) context.arguments().get(1),
                  equalTo(DATASET_ID));
            })) {
      //noinspection ResultOfObjectAllocationIgnored
      new EnableInheritStewardFlight(inputParameters, context);
      assertThat(mockStep.constructed(), hasSize(1));
    }
  }

  @Test
  void setAuthBqJobUserStep() {
    try (var mockStep =
        mockConstruction(
            SetAuthBqJobUserStep.class,
            (mock, context) -> {
              assertThat((ResourceService) context.arguments().get(0), equalTo(resourceService));
              assertThat(
                  "The correct custodian email is passed to the step",
                  (String) context.arguments().get(1),
                  equalTo(CUSTODIAN_EMAIL));
              assertThat(
                  "The correct boolean flag is passed to the step",
                  (boolean) context.arguments().get(2),
                  equalTo(true));
            })) {
      //noinspection ResultOfObjectAllocationIgnored
      new EnableInheritStewardFlight(inputParameters, context);
      assertThat(mockStep.constructed(), hasSize(1));
    }
  }

  @Test
  void setAuthTabluarAclStep() {
    try (var mockStep =
        mockConstruction(
            SetAuthTabularAclStep.class,
            (mock, context) -> {
              assertThat(
                  (BigQuerySnapshotPdao) context.arguments().get(0), equalTo(bigQuerySnapshotPdao));
              assertThat((SnapshotService) context.arguments().get(1), equalTo(snapshotService));
              assertThat(
                  "The correct custodian email is passed to the step",
                  (String) context.arguments().get(2),
                  equalTo(CUSTODIAN_EMAIL));
              assertThat(
                  "The correct boolean flag is passed to the step",
                  (boolean) context.arguments().get(3),
                  equalTo(true));
            })) {
      //noinspection ResultOfObjectAllocationIgnored
      new EnableInheritStewardFlight(inputParameters, context);
      assertThat(mockStep.constructed(), hasSize(1));
    }
  }
}
