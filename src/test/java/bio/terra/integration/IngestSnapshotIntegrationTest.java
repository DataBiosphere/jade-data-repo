package bio.terra.integration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Integration;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamRole;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class IngestSnapshotIntegrationTest extends UsersBase {

  @Autowired private DataRepoFixtures dataRepoFixtures;

  private DatasetSummaryModel datasetSummaryModel;
  private UUID datasetId;
  private UUID profileId;
  private final List<UUID> createdSnapshotIds = new ArrayList<>();

  @Before
  public void setup() throws Exception {
    super.setup();
    profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
    dataRepoFixtures.addPolicyMember(
        steward(), profileId, IamRole.USER, custodian().getEmail(), IamResourceType.SPEND_PROFILE);

    datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "ingest-test-dataset.json");
    datasetId = datasetSummaryModel.getId();
    dataRepoFixtures.addDatasetPolicyMember(
        steward(), datasetId, IamRole.CUSTODIAN, custodian().getEmail());
  }

  @After
  public void teardown() throws Exception {
    for (UUID snapshotId : createdSnapshotIds) {
      dataRepoFixtures.deleteSnapshotLog(custodian(), snapshotId);
    }

    if (datasetId != null) {
      dataRepoFixtures.deleteDatasetLog(steward(), datasetId);
    }

    if (profileId != null) {
      dataRepoFixtures.deleteProfileLog(steward(), profileId);
    }
  }

  @Test
  public void ingestBuildSnapshot() throws Exception {
    IngestRequestModel ingestRequest =
        dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json");
    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(5L));

    ingestRequest =
        dataRepoFixtures.buildSimpleIngest("sample", "ingest-test/ingest-test-sample.json");
    ingestResponse = dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    assertThat("correct sample row count", ingestResponse.getRowCount(), equalTo(7L));

    ingestRequest = dataRepoFixtures.buildSimpleIngest("file", "ingest-test/ingest-test-file.json");
    ingestResponse = dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    assertThat("correct file row count", ingestResponse.getRowCount(), equalTo(1L));

    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshot(
            custodian(), datasetSummaryModel.getName(), profileId, "ingest-test-snapshot.json");
    createdSnapshotIds.add(snapshotSummary.getId());
  }
}
