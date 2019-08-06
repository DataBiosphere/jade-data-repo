package bio.terra.integration;

import bio.terra.category.Integration;
import bio.terra.fixtures.JsonLoader;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.EnumerateSnapshotModel;
import bio.terra.model.JobModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotSummaryModel;
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
public class SnapshotTest extends UsersBase {
    @Autowired
    private DataRepoClient dataRepoClient;

    @Autowired
    private JsonLoader jsonLoader;

    @Autowired
    private DataRepoFixtures dataRepoFixtures;

    private static Logger logger = LoggerFactory.getLogger(SnapshotTest.class);
    private DatasetSummaryModel datasetSummaryModel;
    private String datasetId;
    private List<String> createdDatasetIds = new ArrayList<>();

    @Before
    public void setup() throws Exception {
        super.setup();
        datasetSummaryModel = dataRepoFixtures.createDataset(steward(), "ingest-test-datset.json");
        datasetId = datasetSummaryModel.getId();
        dataRepoFixtures.addDatasetPolicyMember(
            steward(), datasetId, SamClientService.DataRepoRole.CUSTODIAN, custodian().getEmail());
        dataRepoFixtures.ingestJsonData(
            steward(), datasetId, "participant", "ingest-test/ingest-test-participant.json");
        dataRepoFixtures.ingestJsonData(
            steward(), datasetId, "sample", "ingest-test/ingest-test-sample.json");
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
        dataRepoFixtures.deleteDataset(steward(), datasetId);
    }


    @Test
    public void dataSnapshotUnauthorizedPermissionsTest() throws Exception {
        DataRepoResponse<JobModel> createSnapLaunchResp =
            dataRepoFixtures.createSnapshotLaunch(reader(), datasetSummaryModel, "ingest-test-dataset.json");
        assertThat("Reader is not authorized to create a dataSnapshot",
            createSnapLaunchResp.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        SnapshotSummaryModel snapshotSummary =
            dataRepoFixtures.createSnapshot(custodian(), datasetSummaryModel, "ingest-test-dataset.json");
        createdDatasetIds.add(snapshotSummary.getId());

        DataRepoResponse<JobModel> deleteSnapResp =
            dataRepoFixtures.deleteSnapshotLaunch(reader(), snapshotSummary.getId());
        assertThat("Reader is not authorized to delete a dataSnapshot",
            deleteSnapResp.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        DataRepoResponse<SnapshotModel> getSnapResp = dataRepoFixtures.getSnapshotRaw(
            discoverer(), snapshotSummary.getId());
        assertThat("Discoverer is not authorized to get a dataSnapshot",
            getSnapResp.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        EnumerateSnapshotModel enumSnap = dataRepoFixtures.enumerateSnapshots(discoverer());
        assertThat("Discoverer does not have access to dataSnapshots",
            enumSnap.getTotal(),
            equalTo(0));
    }
}
