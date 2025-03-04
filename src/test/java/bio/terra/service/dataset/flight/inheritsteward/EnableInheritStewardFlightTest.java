package bio.terra.service.dataset.flight.inheritsteward;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

import bio.terra.common.FlightTestUtils;
import bio.terra.common.category.Unit;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.job.JobMapKeys;
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
  @Mock private DatasetService datasetService;
  private FlightMap inputParameters;
  private static final UUID DATASET_ID = UUID.randomUUID();

  @BeforeEach
  void setUp() {
    inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.DATASET);
    inputParameters.put(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), DATASET_ID);
    inputParameters.put(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.SET_INHERIT_STEWARD);
    when(context.getBean(DatasetDao.class)).thenReturn(datasetDao);
    when(context.getBean(DatasetService.class)).thenReturn(datasetService);
  }

  @Test
  void testStepsIncluded() {
    var flight = new EnableInheritStewardFlight(inputParameters, context);
    var steps = FlightTestUtils.getStepNames(flight);
    assertThat(steps, contains("InheritStewardSetFlagStep", "LockDatasetStep"));
  }

  @Test
  void testParametersForSetFlagStep() {
    try (var mockedStep =
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
      new EnableInheritStewardFlight(inputParameters, context);
    }
  }

  @Test
  void testParametersForLockDatasetStep() {
    try (var mockedStep =
        mockConstruction(
            LockDatasetStep.class,
            (mock, context) -> {
              assertThat((DatasetService) context.arguments().get(0), equalTo(datasetService));
              assertThat(
                  "The correct datasetId is passed to the step",
                  (UUID) context.arguments().get(1),
                  equalTo(DATASET_ID));
              assertThat(
                  "The correct shared lock boolean flag is passed to the step",
                  (boolean) context.arguments().get(2),
                  equalTo(false));
            })) {
      new EnableInheritStewardFlight(inputParameters, context);
    }
  }
}
