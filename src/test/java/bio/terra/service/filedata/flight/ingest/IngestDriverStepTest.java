package bio.terra.service.filedata.flight.ingest;

import bio.terra.common.category.Unit;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.kubernetes.KubeService;
import bio.terra.service.load.LoadCandidates;
import bio.terra.service.load.LoadFile;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Stairway;
import bio.terra.stairway.StepResult;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(SpringRunner.class)
@Category(Unit.class)
public class IngestDriverStepTest extends TestCase {

    @MockBean
    private LoadService loadService;

    @MockBean
    private ConfigurationService configurationService;

    @MockBean
    private KubeService kubeService;

    private final UUID loadUuid = UUID.randomUUID();

    private void runTest(int maxFailedFileLoads) throws Exception {
        given(kubeService.getActivePodCount()).willReturn(1);
        given(configurationService.getParameterValue(ConfigEnum.AUTH_CACHE_SIZE)).willReturn(1);
        given(configurationService.getParameterValue(ConfigEnum.LOAD_CONCURRENT_FILES)).willReturn(1);
        final LoadFile loadFile = new LoadFile();

        // Start the task with three failed loads and one pending (candidate) file.
        LoadCandidates candidates = new LoadCandidates()
            .candidateFiles(Collections.singletonList(loadFile))
            .runningLoads(Collections.emptyList())
            .failedLoads(3);
        given(loadService.findCandidates(loadUuid, 1)).willReturn(candidates);

        IngestDriverStep step = new IngestDriverStep(loadService, configurationService, kubeService,
            "datasetId", "loadTag", maxFailedFileLoads, 0, "profileId");

        FlightContext flightContext = new FlightContext(new FlightMap(), "", Collections.emptyList());
        flightContext.getWorkingMap().put(LoadMapKeys.LOAD_ID, loadUuid.toString());
        flightContext.setStairway(mock(Stairway.class));

        // When loadService.setLoadFileRunning() is called with our UUID, update the candidate state so no files are
        // left. Otherwise the step would loop forever.
        doAnswer(invocation -> candidates.candidateFiles(Collections.emptyList()))
            .when(loadService).setLoadFileRunning(loadUuid, null, null);

        assertEquals(StepResult.getStepResultSuccess(), step.doStep(flightContext));
    }

    @Test
    public void testDoStepAllowFailed() throws Exception {
        // Allow unlimited file load errors.
        runTest(-1);

        // Verify that the step started the candidate file.
        verify(loadService).setLoadFileRunning(loadUuid, null, null);
    }

    @Test
    public void testDoStepDisallowFailed() throws Exception {
        // Don't allow any file load errors.
        runTest(0);

        // Verify that the step never started the candidate file.
        verify(loadService, never()).setLoadFileRunning(loadUuid, null, null);
    }
}
