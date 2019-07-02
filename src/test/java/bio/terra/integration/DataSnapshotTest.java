package bio.terra.integration;

import bio.terra.category.Integration;
import bio.terra.fixtures.JsonLoader;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.JobModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.service.SamClientService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@Category(Integration.class)
public class DataSnapshotTest extends UsersBase {
    @Autowired
    private DataRepoClient dataRepoClient;

    @Autowired
    private JsonLoader jsonLoader;

    @Autowired
    private DataRepoFixtures dataRepoFixtures;

    private static Logger logger = LoggerFactory.getLogger(StudyTest.class);
    private StudySummaryModel studySummaryModel;
    private String studyId;
    private List<String> createdDatasetIds = new ArrayList<>();

    @Before
    public void setup() throws Exception {
        super.setup();
        studySummaryModel = dataRepoFixtures.createStudy(steward(), "ingest-test-study.json");
        studyId = studySummaryModel.getId();
        dataRepoFixtures.addStudyPolicyMember(
            steward(), studyId, SamClientService.DataRepoRole.CUSTODIAN, custodian().getEmail());
        dataRepoFixtures.ingestJsonData(
            steward(), studyId, "participant", "ingest-test/ingest-test-participant.json");
        dataRepoFixtures.ingestJsonData(
            steward(), studyId, "sample", "ingest-test/ingest-test-sample.json");
    }

    @After
    public void tearDown() throws Exception {
        createdDatasetIds.forEach(snapshot -> {
            try {
                dataRepoFixtures.deleteDataset(steward(), snapshot);
            } catch (Exception ex) {
                logger.warn("cleanup failed when deleteing snapshot " + snapshot);
                ex.printStackTrace();
            }
        });
        dataRepoFixtures.deleteStudy(steward(), studyId);
    }


    @Test
    public void dataSnapshotUnauthorizedPermissionsTest() throws Exception {
        DataRepoResponse<JobModel> createSnapLaunchResp =
            dataRepoFixtures.createDatasetLaunch(reader(), studySummaryModel, "ingest-test-dataset.json");
        assertThat("Reader is not authorized to create a dataSnapshot",
            createSnapLaunchResp.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        DatasetSummaryModel snapshotSummary =
            dataRepoFixtures.createDataset(custodian(), studySummaryModel, "ingest-test-dataset.json");
        createdDatasetIds.add(snapshotSummary.getId());

        DataRepoResponse<JobModel> deleteSnapResp =
            dataRepoFixtures.deleteDatasetLaunch(reader(), snapshotSummary.getId());
        assertThat("Reader is not authorized to delete a dataSnapshot",
            deleteSnapResp.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        DataRepoResponse<DatasetModel> getSnapResp = dataRepoFixtures.getDataSnapshotRaw(
            discoverer(), snapshotSummary.getId());
        assertThat("Discoverer is not authorized to get a dataSnapshot",
            getSnapResp.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        EnumerateDatasetModel enumSnap = dataRepoFixtures.enumerateSnapshots(discoverer());
        assertThat("Discoverer does not have access to dataSnapshots",
            enumSnap.getTotal(),
            equalTo(0));
    }
}
