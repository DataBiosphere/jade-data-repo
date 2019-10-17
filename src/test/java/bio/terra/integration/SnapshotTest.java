package bio.terra.integration;

import bio.terra.category.Integration;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.EnumerateSnapshotModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.JobModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.iam.SamClientService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
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
@AutoConfigureMockMvc
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
    private List<String> createdSnapshotIds = new ArrayList<>();

    @Before
    public void setup() throws Exception {
        super.setup();
        datasetSummaryModel = dataRepoFixtures.createDataset(steward(), "ingest-test-dataset.json");
        datasetId = datasetSummaryModel.getId();
        dataRepoFixtures.addDatasetPolicyMember(
            steward(), datasetId, SamClientService.DataRepoRole.CUSTODIAN, custodian().getEmail());

        IngestRequestModel request = dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json", IngestRequestModel.StrategyEnum.APPEND);
        dataRepoFixtures.ingestJsonData(steward(), datasetId, request);
        request = dataRepoFixtures.buildSimpleIngest(
            "sample", "ingest-test/ingest-test-sample.json", IngestRequestModel.StrategyEnum.APPEND);
        dataRepoFixtures.ingestJsonData(steward(), datasetId, request);
    }

    @After
    public void tearDown() throws Exception {
        createdSnapshotIds.forEach(snapshot -> {
            try {
                dataRepoFixtures.deleteSnapshot(steward(), snapshot);
            } catch (Exception ex) {
                logger.warn("cleanup failed when deleting snapshot " + snapshot);
                ex.printStackTrace();
            }
        });
        dataRepoFixtures.deleteDataset(steward(), datasetId);
    }


    @Test
    public void snapshotUnauthorizedPermissionsTest() throws Exception {
        DataRepoResponse<JobModel> createSnapLaunchResp =
            dataRepoFixtures.createSnapshotLaunch(reader(), datasetSummaryModel, "ingest-test-snapshot.json");
        assertThat("Reader is not authorized to create a dataSnapshot",
            createSnapLaunchResp.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        SnapshotSummaryModel snapshotSummary =
            dataRepoFixtures.createSnapshot(custodian(), datasetSummaryModel, "ingest-test-snapshot.json");
        createdSnapshotIds.add(snapshotSummary.getId());

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
