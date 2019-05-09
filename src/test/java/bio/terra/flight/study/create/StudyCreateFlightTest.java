package bio.terra.flight.study.create;

import bio.terra.category.Connected;
import bio.terra.dao.StudyDao;
import bio.terra.metadata.Study;
import bio.terra.model.StudyJsonConversion;
import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.pdao.PrimaryDataAccess;
import bio.terra.service.JobMapKeys;
import bio.terra.service.SamClientService;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.Stairway;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@SpringBootTest
@Category(Connected.class)
public class StudyCreateFlightTest {

    @Autowired
    private Stairway stairway;

    @Autowired
    private PrimaryDataAccess pdao;

    @Autowired
    private StudyDao studyDao;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private SamClientService samService;

    private String studyName;
    private StudyRequestModel studyRequest;
    private Study study;

    private StudyRequestModel makeStudyRequest(String studyName) throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        ObjectReader reader = objectMapper.readerFor(StudyRequestModel.class);
        InputStream stream = classLoader.getResourceAsStream("study-minimal.json");
        StudyRequestModel studyRequest = reader.readValue(stream);
        studyRequest.setName(studyName);
        return studyRequest;
    }

    @Before
    public void setup() throws Exception {
        studyName = "scftest" + StringUtils.remove(UUID.randomUUID().toString(), '-');
        studyRequest = makeStudyRequest(studyName);
        study = StudyJsonConversion.studyRequestToStudy(studyRequest);
        when(samService.createDatasetResource(any(), any(), any())).thenReturn("hi");
        doNothing().when(samService).createStudyResource(any(), any());
        doNothing().when(samService).deleteDatasetResource(any(), any());
        doNothing().when(samService).deleteStudyResource(any(), any());
    }

    @After
    public void tearDown() {
        if (pdao.studyExists(studyName)) {
            pdao.deleteStudy(study);
        }
        studyDao.deleteByName(studyName);
    }

    @Test
    public void testHappyPath() {
        FlightMap map = new FlightMap();
        map.put(JobMapKeys.REQUEST.getKeyName(), studyRequest);
        String flightId = stairway.submit(StudyCreateFlight.class, map);
        stairway.waitForFlight(flightId);

        FlightState result = stairway.getFlightState(flightId);
        assertEquals(FlightStatus.SUCCESS, result.getFlightStatus());
        Optional<FlightMap> resultMap = result.getResultMap();
        assertTrue(resultMap.isPresent());
        StudySummaryModel response = resultMap.get().get(JobMapKeys.RESPONSE.getKeyName(), StudySummaryModel.class);
        assertEquals(studyName, response.getName());

        Study createdStudy = studyDao.retrieve(UUID.fromString(response.getId()));
        assertEquals(studyName, createdStudy.getName());

        assertTrue(pdao.studyExists(studyName));
    }

    @Test
    public void testUndoAfterPrimaryDataStep() {
        FlightMap map = new FlightMap();
        map.put(JobMapKeys.REQUEST.getKeyName(), studyRequest);
        String flightId = stairway.submit(UndoStudyCreateFlight.class, map);
        stairway.waitForFlight(flightId);

        FlightState result = stairway.getFlightState(flightId);
        assertNotEquals(result.getFlightStatus(), FlightStatus.SUCCESS);
        Optional<Exception> errorMessage = result.getException();
        assertTrue(errorMessage.isPresent());
        assertThat(errorMessage.get().getMessage(), containsString("TestTriggerUndoStep"));

        boolean deletedSomething = studyDao.deleteByName(studyName);
        assertFalse(deletedSomething);

        assertFalse(pdao.studyExists(studyName));
    }
}
