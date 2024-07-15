package bio.terra.service.snapshot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;

import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.integration.BigQueryFixtures;
import bio.terra.integration.DataRepoClient;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.DataRepoResponse;
import bio.terra.integration.TestJobWatcher;
import bio.terra.integration.UsersBase;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.JobModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.cloud.bigquery.Acl;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "integrationtest"})
@Category(Integration.class)
public class SnapshotPermissionsIntegrationTest extends UsersBase {

  private static final Logger logger =
      LoggerFactory.getLogger(SnapshotPermissionsIntegrationTest.class);

  @Autowired private JsonLoader jsonLoader;
  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private DataRepoClient dataRepoClient;
  @Autowired private AuthService authService;
  @Rule @Autowired public TestJobWatcher testWatcher;

  private String stewardToken;
  private UUID profileId;
  private UUID datasetId;
  private DatasetSummaryModel datasetSummaryModel;
  private final List<UUID> createdSnapshotIds = new ArrayList<>();

  @Before
  public void setup() throws Exception {
    super.setup();
    stewardToken = authService.getDirectAccessAuthToken(steward().getEmail());
    profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
    dataRepoFixtures.addPolicyMember(
        steward(), profileId, IamRole.USER, custodian().getEmail(), IamResourceType.SPEND_PROFILE);

    datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "ingest-test-dataset.json");
    datasetId = datasetSummaryModel.getId();
    dataRepoFixtures.addDatasetPolicyMember(
        steward(), datasetId, IamRole.CUSTODIAN, custodian().getEmail());

    IngestRequestModel request =
        dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json");
    dataRepoFixtures.ingestJsonData(steward(), datasetId, request);
    request = dataRepoFixtures.buildSimpleIngest("sample", "ingest-test/ingest-test-sample.json");
    dataRepoFixtures.ingestJsonData(steward(), datasetId, request);
  }

  @After
  public void tearDown() throws Exception {
    createdSnapshotIds.forEach(
        snapshot -> {
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
  public void snapshotInvalidEmailTest() throws Exception {
    SnapshotRequestModel requestModel =
        jsonLoader.loadObject("ingest-test-snapshot.json", SnapshotRequestModel.class);

    requestModel.setReaders(Collections.singletonList("bad-user@not-a-real-domain.com"));
    DataRepoResponse<JobModel> jobResponse =
        dataRepoFixtures.createSnapshotRaw(
            steward(), datasetSummaryModel.getName(), profileId, requestModel, false, false);
    logger.info("Attempting to create the snapshot with the name: {}", requestModel.getName());

    DataRepoResponse<ErrorModel> snapshotResponse =
        dataRepoClient.waitForResponse(steward(), jobResponse, new TypeReference<>() {});

    assertThat("error is present", snapshotResponse.getErrorObject().isPresent(), equalTo(true));
    assertThat(
        "get the correct error",
        snapshotResponse.getErrorObject().get().getMessage(),
        equalTo(
            "Cannot create resource: You have specified an invalid policy: You have "
                + "specified at least one invalid member email: Invalid member email: bad-user@not-a-real-domain.com"));

    // There's not a good way in the test to select the snapshots in the DB.  We want to make sure
    // that we can
    // create the snapshot with the same name, proving that, at least, the database doesn't contain
    // an
    // orphaned entry
    logger.info(
        "Attempting to recreate the snapshot with the same name: {}", requestModel.getName());
    requestModel.setReaders(Collections.emptyList());
    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshotWithRequest(
            steward(), datasetSummaryModel.getName(), profileId, requestModel, false);

    createdSnapshotIds.add(snapshotSummary.getId());
  }

  @Test
  public void snapshotAclTest() throws Exception {
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);

    String datasetName = dataset.getName();

    logger.info("---- Dataset Acls before snapshot create-----");
    int datasetAclCount = fetchSourceDatasetAcls(datasetName).size();

    // -------------------Create Snapshot----------------
    SnapshotRequestModel requestModel =
        jsonLoader.loadObject("ingest-test-snapshot-fullviews.json", SnapshotRequestModel.class);
    // swap in the correct dataset name (with the id at the end)
    requestModel.getContents().get(0).setDatasetName(datasetName);
    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshotWithRequest(steward(), datasetName, profileId, requestModel);
    createdSnapshotIds.add(snapshotSummary.getId());
    SnapshotModel snapshot = dataRepoFixtures.getSnapshot(steward(), snapshotSummary.getId(), null);
    assertEquals("new snapshot has been created", snapshot.getName(), requestModel.getName());
    assertEquals("There should be 1 snapshot relationship", 1, snapshot.getRelationships().size());

    // fetch Acls
    logger.info("---- Dataset Acls after snapshot create-----");
    int datasetPlusSnapshotCount =
        retryAclUpdate(datasetName, datasetAclCount, AclCheck.GREATERTHAN);
    assertThat(
        "There should be more Acls on the dataset after snapshot create",
        datasetPlusSnapshotCount,
        greaterThan(datasetAclCount));

    // -----------delete snapshot------------
    dataRepoFixtures.deleteSnapshot(steward(), snapshotSummary.getId());
    logger.info("---- Dataset Acls after snapshot delete-----");
    int datasetMinusSnapshotAclCount =
        retryAclUpdate(datasetName, datasetAclCount, AclCheck.EQUALTO);
    assertEquals(
        "We should be back to the same number of Acls on the dataset after snapshot delete",
        datasetAclCount,
        datasetMinusSnapshotAclCount);
    // Don't need to tear down snashot
    createdSnapshotIds.remove(snapshotSummary.getId());
  }

  private List<Acl> fetchSourceDatasetAcls(String datasetName) throws Exception {
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    BigQuery bigQuery = BigQueryFixtures.getBigQuery(dataset.getDataProject(), stewardToken);

    // Fetch BQ Dataset
    String bqDatasetName = BigQueryPdao.prefixName(datasetName);
    Dataset bqDataset = bigQuery.getDataset(bqDatasetName);

    // fetch Acls
    List<Acl> acls = bqDataset.getAcl();

    acls.forEach(acl -> logger.info("Acl: {}", acl));
    return acls;
  }

  enum AclCheck {
    GREATERTHAN {
      boolean compare(int snapshotCount, int aclCount) {
        return snapshotCount > aclCount;
      }
    },
    EQUALTO {
      boolean compare(int snapshotCount, int aclCount) {
        return snapshotCount == aclCount;
      }
    };

    abstract boolean compare(int snapshotCount, int aclCount);
  }

  private int retryAclUpdate(String datasetName, int datasetAclCount, AclCheck check)
      throws Exception {
    double maxDelayInSeconds = 60;
    int waitInterval;
    // Google claims it can take up to 7 minutes to update Acls
    int sevenMinutePlusBuffer = (int) TimeUnit.MINUTES.toSeconds(7) + 20;
    int datasetPlusSnapshotCount = 0;
    int totalWaitTime = 0;
    int n = 1;
    while (totalWaitTime < sevenMinutePlusBuffer) {
      datasetPlusSnapshotCount = fetchSourceDatasetAcls(datasetName).size();
      if (check.compare(datasetPlusSnapshotCount, datasetAclCount)) {
        break;
      }
      double delayInSeconds = ((1d / 2d) * (Math.pow(2d, n) - 1d));
      waitInterval = (int) Math.min(maxDelayInSeconds, delayInSeconds);
      totalWaitTime += waitInterval;
      logger.info(
          "retryAclUpdate: sleeping {} seconds, totaling {} seconds waiting",
          waitInterval,
          totalWaitTime);
      TimeUnit.SECONDS.sleep(waitInterval);
      n++;
    }
    return datasetPlusSnapshotCount;
  }
}
