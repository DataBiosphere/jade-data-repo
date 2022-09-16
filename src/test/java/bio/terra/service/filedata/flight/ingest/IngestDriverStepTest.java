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

    when(flightContext.getFlightId()).thenReturn(PARENT_FLIGHT_ID);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    when(flightContext.getStairway()).thenReturn(stairway);

    when(stairway.createFlightId()).thenReturn(CHILD_FLIGHT_ID);

    // When loadService.setLoadFileRunning() is called with our UUID, update the candidate state so
    // no files are left. Otherwise the step would loop forever.
    doAnswer(invocation -> candidates.candidateFiles(Collections.emptyList()))
        .when(loadService)
        .setLoadFileRunning(loadUuid, null, CHILD_FLIGHT_ID);

    StepResult stepResult = step.doStep(flightContext);

    verify(stairway)
        .submitToQueue(
            eq(CHILD_FLIGHT_ID), eq(FileIngestWorkerFlight.class), inputParamsCaptor.capture());

    assertThat(
        "Parent flight ID was passed as an input parameter to child flight",
        inputParamsCaptor.getValue().get(JobMapKeys.PARENT_FLIGHT_ID.getKeyName(), String.class),
        equalTo(PARENT_FLIGHT_ID));

    return stepResult;
  }

  @Test
  public void testDoStepAllowFailed() throws Exception {
    // Allow unlimited file load errors.
    StepResult stepResult = runTest(-1);

    assertEquals(StepStatus.STEP_RESULT_SUCCESS, stepResult.getStepStatus());

    // Verify that the step started the candidate file.
    verify(loadService).setLoadFileRunning(loadUuid, null, CHILD_FLIGHT_ID);
  }

  @Test
  public void testDoStepDisallowFailed() throws Exception {
    // Don't allow any file load errors.
    StepResult stepResult = runTest(0);

    assertEquals(StepStatus.STEP_RESULT_SUCCESS, stepResult.getStepStatus());

    // Verify that the step never started the candidate file.
    verify(loadService, never()).setLoadFileRunning(loadUuid, null, CHILD_FLIGHT_ID);
  }
}
