package bio.terra.service.snapshot;

import static org.junit.Assert.assertEquals;

import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.integration.DataRepoClient;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.TestJobWatcher;
import bio.terra.integration.UsersBase;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import java.util.ArrayList;
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
public class SnapshotDataTypeTest extends UsersBase {
  @Autowired private DataRepoClient dataRepoClient;

  @Autowired private JsonLoader jsonLoader;

  @Autowired private DataRepoFixtures dataRepoFixtures;

  @Autowired private AuthService authService;

  private static final Logger logger = LoggerFactory.getLogger(SnapshotDataTypeTest.class);
  private UUID profileId;
  private DatasetSummaryModel datasetSummaryModel;
  private UUID datasetId;
  private final List<UUID> createdSnapshotIds = new ArrayList<>();
  private String stewardToken;

  @Rule @Autowired public TestJobWatcher testWatcher;

  @Before
  public void setup() throws Exception {
    super.setup();
    stewardToken = authService.getDirectAccessAuthToken(steward().getEmail());
    profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
    dataRepoFixtures.addPolicyMember(
        steward(), profileId, IamRole.USER, custodian().getEmail(), IamResourceType.SPEND_PROFILE);

    datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "data-types-dataset-create.json");
    datasetId = datasetSummaryModel.getId();
    dataRepoFixtures.addDatasetPolicyMember(
        steward(), datasetId, IamRole.CUSTODIAN, custodian().getEmail());

    IngestRequestModel request =
        dataRepoFixtures.buildSimpleIngest("table1", "data-types-test/table1.json");
    dataRepoFixtures.ingestJsonData(steward(), datasetId, request);
    request = dataRepoFixtures.buildSimpleIngest("table2", "data-types-test/table2.json");
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
  public void snapshotRowIdsHappyPathTest() throws Exception {
    // fetch rowIds from the ingested dataset by querying the participant table
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);

    // swap in these row ids in the request
    SnapshotRequestModel requestModel =
        jsonLoader.loadObject("data-types-gcp-snapshot.json", SnapshotRequestModel.class);

    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshotWithRequest(
            steward(), dataset.getName(), profileId, requestModel);
    TimeUnit.SECONDS.sleep(10);
    createdSnapshotIds.add(snapshotSummary.getId());
    SnapshotModel snapshot = dataRepoFixtures.getSnapshot(steward(), snapshotSummary.getId(), null);
    assertEquals("new snapshot has been created", snapshot.getName(), requestModel.getName());
    assertEquals("new snapshot has the correct number of tables", 2, snapshot.getTables().size());
  }
}
