package bio.terra.service.filedata.flight.ingest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.job.JobService;
import bio.terra.service.load.LoadCandidates;
import bio.terra.service.load.LoadFile;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Stairway;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.Collections;
import java.util.UUID;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class IngestDriverStepTest extends TestCase {

  @MockBean private LoadService loadService;

  @MockBean private ConfigurationService configurationService;

  @MockBean private JobService jobService;

  @Mock private Stairway stairway;

  @Captor private ArgumentCaptor<FlightMap> inputParamsCaptor;

  private final UUID loadUuid = UUID.randomUUID();

  private static final String PARENT_FLIGHT_ID = "parentFlightId";

  private static final String CHILD_FLIGHT_ID = "childFlightId";

  private static final IamResourceType PARENT_RESOURCE_TYPE = IamResourceType.DATASET;
  private static final String PARENT_RESOURCE_ID = UUID.randomUUID().toString();
  private static final IamAction PARENT_RESOURCE_ACTION = IamAction.INGEST_DATA;

  private StepResult runTest(int maxFailedFileLoads) throws Exception {
    given(jobService.getActivePodCount()).willReturn(1);
    given(configurationService.getParameterValue(ConfigEnum.LOAD_CONCURRENT_FILES)).willReturn(1);

    // Start the task with three failed loads and one pending (candidate) file.
    LoadCandidates candidates =
        new LoadCandidates()
            .candidateFiles(Collections.singletonList(new LoadFile()))
            .runningLoads(Collections.emptyList())
            .failedLoads(3);
    given(loadService.findCandidates(loadUuid, 1)).willReturn(candidates);

    IngestDriverStep step =
        new IngestDriverStep(
            loadService,
            configurationService,
            jobService,
            null,
            null,
            maxFailedFileLoads,
            0,
            null,
            CloudPlatform.GCP,
            null);

    FlightContext flightContext = mock(FlightContext.class);
    FlightMap workingMap = new FlightMap();
    workingMap.put(LoadMapKeys.LOAD_ID, loadUuid.toString());

    FlightMap inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), PARENT_RESOURCE_TYPE);
    inputParameters.put(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), PARENT_RESOURCE_ID);
    inputParameters.put(JobMapKeys.IAM_ACTION.getKeyName(), PARENT_RESOURCE_ACTION);

    when(flightContext.getFlightId()).thenReturn(PARENT_FLIGHT_ID);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    when(flightContext.getInputParameters()).thenReturn(inputParameters);
    when(flightContext.getStairway()).thenReturn(stairway);

    when(stairway.createFlightId()).thenReturn(CHILD_FLIGHT_ID);

    // When loadService.setLoadFileRunning() is called with our UUID, update the candidate state so
    // no files are left. Otherwise the step would loop forever.
    doAnswer(invocation -> candidates.candidateFiles(Collections.emptyList()))
        .when(loadService)
        .setLoadFileRunning(loadUuid, null, CHILD_FLIGHT_ID);

    return step.doStep(flightContext);
  }

  @Test
  public void testDoStepAllowFailed() throws Exception {
    // Allow unlimited file load errors.
    StepResult stepResult = runTest(-1);

    assertEquals(StepStatus.STEP_RESULT_SUCCESS, stepResult.getStepStatus());

    // Verify that the step started the candidate file.
    verify(loadService).setLoadFileRunning(loadUuid, null, CHILD_FLIGHT_ID);

    // Verify that the step launched a downstream child job.
    verify(stairway)
        .submitToQueue(
            eq(CHILD_FLIGHT_ID), eq(FileIngestWorkerFlight.class), inputParamsCaptor.capture());

    // Verify that expected parent input parameters were propagated to the child flight.
    FlightMap childInputParameters = inputParamsCaptor.getValue();
    assertThat(
        "Parent flight ID was passed as an input parameter to child flight",
        childInputParameters.get(JobMapKeys.PARENT_FLIGHT_ID.getKeyName(), String.class),
        equalTo(PARENT_FLIGHT_ID));
    assertThat(
        "Parent IamResourceType was passed as an input parameter to child flight",
        childInputParameters.get(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.class),
        equalTo(PARENT_RESOURCE_TYPE));
    assertThat(
        "Parent resource ID was passed as an input parameter to child flight",
        childInputParameters.get(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), String.class),
        equalTo(PARENT_RESOURCE_ID));
    assertThat(
        "Parent IamAction was passed as an input parameter to child flight",
        childInputParameters.get(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.class),
        equalTo(PARENT_RESOURCE_ACTION));
  }

  @Test
  public void testDoStepDisallowFailed() throws Exception {
    // Don't allow any file load errors.
    StepResult stepResult = runTest(0);

    assertEquals(StepStatus.STEP_RESULT_SUCCESS, stepResult.getStepStatus());

    // Verify that the step never started the candidate file.
    verify(loadService, never()).setLoadFileRunning(loadUuid, null, CHILD_FLIGHT_ID);

    // Verify that the step never launched a downstream child job.
    verify(stairway, never())
        .submitToQueue(
            eq(CHILD_FLIGHT_ID), eq(FileIngestWorkerFlight.class), inputParamsCaptor.capture());
  }

  @Test
  public void testPropagateContextToFlightMap() {
    IngestDriverStep step =
        new IngestDriverStep(
            loadService,
            configurationService,
            jobService,
            null,
            null,
            -1,
            0,
            null,
            CloudPlatform.GCP,
            null);

    FlightContext flightContext = mock(FlightContext.class);
    FlightMap inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), PARENT_RESOURCE_TYPE);
    when(flightContext.getInputParameters()).thenReturn(inputParameters);

    FlightMap flightMap = new FlightMap();

    step.propagateContextToFlightMap(
        flightContext, flightMap, "not a key in flightContext.inputParameters", String.class);
    assertThat("Missing value leaves new flight map unchanged", flightMap.isEmpty());

    step.propagateContextToFlightMap(
        flightContext, flightMap, JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamAction.class);
    assertThat(
        "Inability to deserialize value leaves new flight map unchanged", flightMap.isEmpty());

    step.propagateContextToFlightMap(
        flightContext, flightMap, JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.class);
    assertThat(flightMap.getMap().size(), equalTo(1));
    assertThat(
        "Value from context successfully propagated to new flight map",
        flightMap.get(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.class),
        equalTo(PARENT_RESOURCE_TYPE));
  }
}
