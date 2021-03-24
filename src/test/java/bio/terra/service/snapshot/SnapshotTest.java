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
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamRole;
import com.google.cloud.bigquery.Acl;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
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
import java.util.Collections;
import java.util.List;
import java.util.UUID;
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

    private BigQuery bigQuery;

    private static final Logger logger = LoggerFactory.getLogger(SnapshotTest.class);
    private String profileId;
    private DatasetSummaryModel datasetSummaryModel;
    private String datasetId;
    private final List<String> createdSnapshotIds = new ArrayList<>();
    private String stewardToken;

    @Before
    public void setup() throws Exception {
        super.setup();
        stewardToken = authService.getDirectAccessAuthToken(steward().getEmail());
        profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
        dataRepoFixtures.addPolicyMember(
            steward(),
            profileId,
            IamRole.USER,
            custodian().getEmail(),
            IamResourceType.SPEND_PROFILE);

        datasetSummaryModel = dataRepoFixtures.createDataset(steward(), profileId, "ingest-test-dataset.json");
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

        if (datasetId != null) {
            dataRepoFixtures.deleteDatasetLog(steward(), datasetId);
        }

        if (profileId != null) {
            dataRepoFixtures.deleteProfileLog(steward(), profileId);
        }
    }

    @Test
    public void snapshotUnauthorizedPermissionsTest() throws Exception {
        SnapshotRequestModel requestModel =
            jsonLoader.loadObject("ingest-test-snapshot.json", SnapshotRequestModel.class);

        DataRepoResponse<JobModel> createSnapLaunchResp =
            dataRepoFixtures.createSnapshotRaw(reader(), datasetSummaryModel.getName(), profileId, requestModel);
        assertThat("Reader is not authorized to create a dataSnapshot",
            createSnapLaunchResp.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        SnapshotSummaryModel snapshotSummary =
            dataRepoFixtures.createSnapshot(custodian(),
                datasetSummaryModel.getName(),
                profileId,
                "ingest-test-snapshot.json");

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

        assertThat("Discoverer does not have access to dataSnapshots",
            enumSnap.getTotal(),
            equalTo(0));

        EnumerateSnapshotModel enumSnapByDatasetId = dataRepoFixtures.enumerateSnapshotsByDatasetIds(
            steward(),
            Collections.singletonList(datasetSummaryModel.getId()));

        assertThat("Dataset filters to dataSnapshots",
            enumSnapByDatasetId.getTotal(),
            equalTo(1));

        EnumerateSnapshotModel enumSnapByNoDatasetId = dataRepoFixtures.enumerateSnapshotsByDatasetIds(
            steward(),
            Collections.emptyList());

        assertThat("Dataset filters to dataSnapshots",
            enumSnapByNoDatasetId.getTotal(),
            equalTo(1));

        EnumerateSnapshotModel enumSnapByBadDatasetId = dataRepoFixtures.enumerateSnapshotsByDatasetIds(
            steward(),
            Collections.singletonList(UUID.randomUUID().toString()));

        assertThat("Dataset filters to dataSnapshots",
            enumSnapByBadDatasetId.getTotal(),
            equalTo(0));

        // Delete snapshot as custodian for this test since teardown uses steward
        dataRepoFixtures.deleteSnapshot(custodian(), snapshotSummary.getId());
    }

    @Test
    public void snapshotRowIdsHappyPathTest() throws Exception {
        // fetch rowIds from the ingested dataset by querying the participant table
        DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
        String datasetProject = dataset.getDataProject();
        String bqDatasetName = PdaoConstant.PDAO_PREFIX + dataset.getName();
        String participantTable = "participant";
        String sampleTable = "sample";
        BigQuery bigQuery = BigQueryFixtures.getBigQuery(dataset.getDataProject(), stewardToken);
        String sql = String.format("SELECT %s FROM `%s.%s.%s`",
            PdaoConstant.PDAO_ROW_ID_COLUMN,
            datasetProject,
            bqDatasetName,
            participantTable);
        TableResult participantIds = BigQueryFixtures.query(sql, bigQuery);
        List<String> participantIdList = StreamSupport.stream(participantIds.getValues().spliterator(), false)
            .map(v -> v.get(0).getStringValue())
            .collect(Collectors.toList());
        sql = String.format("SELECT %s FROM `%s.%s.%s`",
            PdaoConstant.PDAO_ROW_ID_COLUMN,
            datasetProject,
            bqDatasetName,
            sampleTable);
        TableResult sampleIds = BigQueryFixtures.query(sql, bigQuery);
        List<String> sampleIdList = StreamSupport.stream(sampleIds.getValues().spliterator(), false)
            .map(v -> v.get(0).getStringValue())
            .collect(Collectors.toList());

        // swap in these row ids in the request
        SnapshotRequestModel requestModel =
            jsonLoader.loadObject("ingest-test-snapshot-row-ids-test.json", SnapshotRequestModel.class);
        requestModel
            .getContents().get(0)
            .getRowIdSpec()
            .getTables().get(0)
            .setRowIds(participantIdList);
        requestModel
            .getContents().get(0)
            .getRowIdSpec()
            .getTables().get(1)
            .setRowIds(sampleIdList);

        SnapshotSummaryModel snapshotSummary =
            dataRepoFixtures.createSnapshotWithRequest(steward(),
                dataset.getName(),
                profileId,
                requestModel);
        TimeUnit.SECONDS.sleep(10);
        createdSnapshotIds.add(snapshotSummary.getId());
        SnapshotModel snapshot = dataRepoFixtures.getSnapshot(steward(), snapshotSummary.getId());
        assertEquals("new snapshot has been created", snapshot.getName(), requestModel.getName());
        assertEquals("new snapshot has the correct number of tables",
            requestModel.getContents().get(0).getRowIdSpec().getTables().size(),
            snapshot.getTables().size());
        // TODO: get the snapshot and make sure the number of rows matches with the row ids input
        assertThat("one relationship comes through", snapshot.getRelationships().size(), equalTo(1));
        assertThat("the right relationship comes through",
            snapshot.getRelationships().get(0).getName(),
            equalTo("sample_participants"));
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
            .setQuery("SELECT " + datasetName + ".sample.datarepo_row_id FROM "
              + datasetName + ".sample WHERE " + datasetName + ".sample.id ='sample6'");
        SnapshotSummaryModel snapshotSummary =
            dataRepoFixtures.createSnapshotWithRequest(steward(),
                datasetName,
                profileId,
                requestModel);
        TimeUnit.SECONDS.sleep(10);
        createdSnapshotIds.add(snapshotSummary.getId());
        SnapshotModel snapshot = dataRepoFixtures.getSnapshot(steward(), snapshotSummary.getId());
        assertEquals("new snapshot has been created", snapshot.getName(), requestModel.getName());
    }

    @Test
    public void snapshotByFullViewHappyPathTest() throws Exception {
        DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
        String datasetName = dataset.getName();
        SnapshotRequestModel requestModel =
            jsonLoader.loadObject("ingest-test-snapshot-fullviews.json", SnapshotRequestModel.class);
        // swap in the correct dataset name (with the id at the end)
        requestModel.getContents().get(0).setDatasetName(datasetName);
        SnapshotSummaryModel snapshotSummary =
            dataRepoFixtures.createSnapshotWithRequest(steward(),
                datasetName,
                profileId,
                requestModel);
        TimeUnit.SECONDS.sleep(10);
        createdSnapshotIds.add(snapshotSummary.getId());
        SnapshotModel snapshot = dataRepoFixtures.getSnapshot(steward(), snapshotSummary.getId());
        assertEquals("new snapshot has been created", snapshot.getName(), requestModel.getName());
        assertEquals("all 5 relationships come through", snapshot.getRelationships().size(), 5);
    }

    @Test
    public void snapshotACLTest() throws Exception {
        DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);

        bigQuery = BigQueryOptions.newBuilder()
            .setProjectId(dataset.getDataProject())
            .build()
            .getService();
        Dataset bq_dataset = bigQuery.getDataset(datasetId);
        List<Acl> beforeAcls = bq_dataset.getAcl();
        logger.info("ACLS: {}", beforeAcls.stream().toString());

        String datasetName = dataset.getName();
        SnapshotRequestModel requestModel =
            jsonLoader.loadObject("ingest-test-snapshot-fullviews.json", SnapshotRequestModel.class);
        // swap in the correct dataset name (with the id at the end)
        requestModel.getContents().get(0).setDatasetName(datasetName);
        SnapshotSummaryModel snapshotSummary =
            dataRepoFixtures.createSnapshotWithRequest(steward(),
                datasetName,
                profileId,
                requestModel);
        TimeUnit.SECONDS.sleep(10);
        createdSnapshotIds.add(snapshotSummary.getId());
        SnapshotModel snapshot = dataRepoFixtures.getSnapshot(steward(), snapshotSummary.getId());
        assertEquals("new snapshot has been created", snapshot.getName(), requestModel.getName());
        assertEquals("all 5 relationships come through", snapshot.getRelationships().size(), 5);
        dataRepoFixtures.deleteSnapshot(steward(), snapshotSummary.getId());

        TimeUnit.SECONDS.sleep(10);
        Dataset after_bq_dataset = bigQuery.getDataset(datasetId);
        List<Acl> afterAcls = after_bq_dataset.getAcl();
        logger.info("After ACLS: {}", afterAcls.stream().toString());
    }
}
