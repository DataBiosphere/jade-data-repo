package bio.terra.flight.study.create;

import bio.terra.category.Connected;
import bio.terra.dao.StudyDao;
import bio.terra.dao.exception.StudyNotFoundException;
import bio.terra.fixtures.ConnectedOperations;
import bio.terra.fixtures.JsonLoader;
import bio.terra.metadata.Study;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.StudyJsonConversion;
import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.pdao.PrimaryDataAccess;
import bio.terra.resourcemanagement.dao.ProfileDao;
import bio.terra.resourcemanagement.service.google.GoogleResourceConfiguration;
import bio.terra.service.JobMapKeys;
import bio.terra.service.SamClientService;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.Stairway;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class StudyCreateFlightTest {

    @Autowired private Stairway stairway;
    @Autowired private PrimaryDataAccess pdao;
    @Autowired private StudyDao studyDao;
    @Autowired private ProfileDao profileDao;
    @Autowired private JsonLoader jsonLoader;
    @Autowired private GoogleResourceConfiguration googleResourceConfiguration;
    @Autowired private ConnectedOperations connectedOperations;

    @MockBean
    private SamClientService samService;

    private String studyName;
    private StudyRequestModel studyRequest;
    private Study study;
    private BillingProfileModel billingProfileModel;

    private StudyRequestModel makeStudyRequest(String studyName, String profileId) throws IOException {
        StudyRequestModel studyRequest = jsonLoader.loadObject("study-minimal.json",
            StudyRequestModel.class);
        return studyRequest
            .name(studyName)
            .defaultProfileId(profileId);
    }

    @Before
    public void setup() throws Exception {
        studyName = "scftest" + StringUtils.remove(UUID.randomUUID().toString(), '-');
        billingProfileModel = connectedOperations.getOrCreateProfileForAccount(
            googleResourceConfiguration.getCoreBillingAccount());
        studyRequest = makeStudyRequest(studyName, billingProfileModel.getId());
        study = StudyJsonConversion.studyRequestToStudy(studyRequest);
        connectedOperations.stubOutSamCalls(samService);
    }

    @After
    public void tearDown() {
        deleteStudy(study);
        studyDao.deleteByName(studyName);
        profileDao.deleteBillingProfileById(UUID.fromString(billingProfileModel.getId()));
    }

    /**
     * Fetches a study from the database based on it's name (handles the case where id isn't filled in)
     * @param study
     * @return
     */
    public boolean deleteStudy(Study study) {
        try {
            Study studyFromDb = studyDao.retrieveByName(study.getName());
            return pdao.deleteStudy(studyFromDb);
        } catch (StudyNotFoundException e) {
            return false;
        }
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

        assertTrue(deleteStudy(study));
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

        assertFalse(deleteStudy(study));
    }
}
