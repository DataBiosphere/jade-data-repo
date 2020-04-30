package bio.terra.service.snapshot;

import bio.terra.common.PdaoConstant;
import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.integration.BigQueryFixtures;
import bio.terra.integration.DataRepoClient;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.DataRepoResponse;
import bio.terra.integration.UsersBase;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.EnumerateSnapshotModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.JobModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.iam.IamRole;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableResult;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
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

    @Autowired
    private AuthService authService;

    private static Logger logger = LoggerFactory.getLogger(SnapshotTest.class);
    private DatasetSummaryModel datasetSummaryModel;
    private String datasetId;
    private List<String> createdSnapshotIds = new ArrayList<>();
    private String stewardToken;

    @Before
    public void setup() throws Exception {
        super.setup();
        stewardToken = authService.getDirectAccessAuthToken(steward().getEmail());
        datasetSummaryModel = dataRepoFixtures.createDataset(steward(), "ingest-test-dataset.json");
        datasetId = datasetSummaryModel.getId();
        dataRepoFixtures.addDatasetPolicyMember(
            steward(), datasetId, IamRole.CUSTODIAN, custodian().getEmail());

        IngestRequestModel request = dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json");
        dataRepoFixtures.ingestJsonData(steward(), datasetId, request);
        request = dataRepoFixtures.buildSimpleIngest(
            "sample", "ingest-test/ingest-test-sample.json");
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

    @Test
    public void snapshotRowIdsHappyPathTest() throws Exception {
        // fetch rowIds from the ingested dataset by querying the participant table
        DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
        String datasetProject = dataset.getDataProject();
        String bqDatasetName = PdaoConstant.PDAO_PREFIX + dataset.getName();
        String datasetTable = "participant";
        BigQuery bigQuery = BigQueryFixtures.getBigQuery(dataset.getDataProject(), stewardToken);
        String sql = String.format("SELECT %s FROM `%s.%s.%s`",
            PdaoConstant.PDAO_ROW_ID_COLUMN,
            datasetProject,
            bqDatasetName,
            datasetTable);
        TableResult ids = BigQueryFixtures.query(sql, bigQuery);
        List<String> idList = StreamSupport.stream(ids.getValues().spliterator(), false)
            .map(v -> v.get(0).getStringValue())
            .collect(Collectors.toList());

        // swap in these row ids in the request
        SnapshotRequestModel requestModel =
            jsonLoader.loadObject("ingest-test-snapshot-row-ids-test.json", SnapshotRequestModel.class);
        requestModel
            .getContents().get(0)
            .getRowIdSpec()
            .getTables().get(0)
            .setRowIds(idList);

        SnapshotSummaryModel snapshotSummary =
            dataRepoFixtures.createSnapshotWithRequest(steward(),
                datasetSummaryModel,
                requestModel);
        TimeUnit.SECONDS.sleep(10);
        createdSnapshotIds.add(snapshotSummary.getId());
        SnapshotModel snapshot = dataRepoFixtures.getSnapshot(steward(), snapshotSummary.getId());
        assertEquals("new snapshot has been created", snapshot.getName(), requestModel.getName());
        assertEquals("new snapshot has the correct number of tables",
            requestModel.getContents().get(0).getRowIdSpec().getTables().size(),
            snapshot.getTables().size());
        // TODO: get the snapshot and make sure the number of rows matches with the row ids input
    }

    @Test
    public void snapshotByQueryHappyPathTest() throws Exception {
        DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
        String datasetName = dataset.getName();
        SnapshotRequestModel requestModel =
            jsonLoader.loadObject("ingest-test-snapshot-query.json", SnapshotRequestModel.class);
        // swap in the correct dataset name (with the id at the end)
        requestModel.getContents().get(0).setDatasetName(datasetName);
        requestModel.getContents().get(0).getQuerySpec()
          .setQuery("SELECT " + datasetName + ".sample.datarepo_row_id FROM " + datasetName + ".sample WHERE " + datasetName + ".sample.id ='sample6'");
        SnapshotSummaryModel snapshotSummary =
            dataRepoFixtures.createSnapshotWithRequest(steward(),
                datasetSummaryModel,
                requestModel);
        TimeUnit.SECONDS.sleep(10);
        createdSnapshotIds.add(snapshotSummary.getId());
        SnapshotModel snapshot = dataRepoFixtures.getSnapshot(steward(), snapshotSummary.getId());
        assertEquals("new snapshot has been created", snapshot.getName(), requestModel.getName());
    }
}
