package bio.terra.flight.study.create;

import bio.terra.category.Connected;
import bio.terra.dao.StudyDao;
import bio.terra.metadata.Study;
import bio.terra.model.StudyJsonConversion;
import bio.terra.model.StudyRequestModel;
import bio.terra.pdao.PrimaryDataAccess;
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
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.*;

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
    public void setup() throws IOException {
        studyName = "scftest" + StringUtils.remove(UUID.randomUUID().toString(), '-');
        studyRequest = makeStudyRequest(studyName);
        study = StudyJsonConversion.studyRequestToStudy(studyRequest);
    }

    @After
    public void tearDown() {
        Optional<Study> createdStudy = studyDao.retrieveByName(studyName);
        if (createdStudy.isPresent()) {
            studyDao.delete(createdStudy.get().getId());
        }
        if (pdao.studyExists(studyName)) {
            pdao.deleteStudy(study);
        }
    }

    @Test
    public void testHappyPath() {
        FlightMap map = new FlightMap();
        map.put("request", studyRequest);
        String flightId = stairway.submit(StudyCreateFlight.class, map);
        stairway.waitForFlight(flightId);
        FlightState result = stairway.getFlightState(flightId);
        assertEquals(result.getFlightStatus(), FlightStatus.SUCCESS);
        Optional<Study> createdStudy = studyDao.retrieveByName(studyName);
        assertTrue(createdStudy.isPresent());
        assertTrue(pdao.studyExists(studyName));
    }

    @Test
    public void testUndoAfterPrimaryDataStep() {
        FlightMap map = new FlightMap();
        map.put("request", studyRequest);
        String flightId = stairway.submit(UndoStudyCreateFlight.class, map);
        stairway.waitForFlight(flightId);
        FlightState result = stairway.getFlightState(flightId);
        assertNotEquals(result.getFlightStatus(), FlightStatus.SUCCESS);
        Optional<String> errorMessage = result.getErrorMessage();
        assertTrue(errorMessage.isPresent());
        assertThat(errorMessage.get(), containsString("TestTriggerUndoStep"));
        Optional<Study> createdStudy = studyDao.retrieveByName(studyName);
        assertFalse(createdStudy.isPresent());
        assertFalse(pdao.studyExists(studyName));
    }
}
