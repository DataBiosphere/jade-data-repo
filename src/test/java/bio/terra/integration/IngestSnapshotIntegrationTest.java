package bio.terra.integration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.model.TableModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = IntegrationTestConfiguration.class)
@ActiveProfiles({"google", "integrationtest"})
@Tag(Integration.TAG)
class IngestSnapshotIntegrationTest {

  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private Users users;

  private TestConfiguration.User custodian;
  private TestConfiguration.User steward;
  private DatasetSummaryModel datasetSummaryModel;
  private UUID datasetId;
  private UUID profileId;
  private final List<UUID> createdSnapshotIds = new ArrayList<>();

  @BeforeEach
  public void setup() throws Exception {
    custodian = users.custodian();
    steward = users.steward();

    profileId = dataRepoFixtures.createBillingProfile(steward).getId();
    dataRepoFixtures.addPolicyMember(
        steward, profileId, IamRole.USER, custodian.email(), IamResourceType.SPEND_PROFILE);

    datasetSummaryModel =
        dataRepoFixtures.createDataset(steward, profileId, "ingest-test-dataset.json");
    datasetId = datasetSummaryModel.getId();
    dataRepoFixtures.addDatasetPolicyMember(
        steward, datasetId, IamRole.CUSTODIAN, custodian.email());
  }

  @AfterEach
  public void teardown() throws Exception {
    for (UUID snapshotId : createdSnapshotIds) {
      dataRepoFixtures.deleteSnapshotLog(custodian, snapshotId);
    }

    if (datasetId != null) {
      dataRepoFixtures.deleteDatasetLog(steward, datasetId);
    }

    if (profileId != null) {
      dataRepoFixtures.deleteProfileLog(steward, profileId);
    }
  }

  @Test
  void ingestBuildSnapshot() throws Exception {
    IngestRequestModel ingestRequest =
        dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json");
    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward, datasetId, ingestRequest);
    assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(5L));

    ingestRequest =
        dataRepoFixtures.buildSimpleIngest("sample", "ingest-test/ingest-test-sample.json");
    ingestResponse = dataRepoFixtures.ingestJsonData(steward, datasetId, ingestRequest);
    assertThat("correct sample row count", ingestResponse.getRowCount(), equalTo(7L));

    ingestRequest = dataRepoFixtures.buildSimpleIngest("file", "ingest-test/ingest-test-file.json");
    ingestResponse = dataRepoFixtures.ingestJsonData(steward, datasetId, ingestRequest);
    assertThat("correct file row count", ingestResponse.getRowCount(), equalTo(1L));

    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshot(
            custodian, datasetSummaryModel.getName(), profileId, "ingest-test-snapshot.json");

    SnapshotModel snapshot =
        dataRepoFixtures.getSnapshot(custodian, snapshotSummary.getId(), List.of());

    Map<String, TableModel> tableMap =
        snapshot.getTables().stream()
            .collect(Collectors.toMap(TableModel::getName, Function.identity()));
    assertThat(
        "primary key information makes it through in participant table",
        tableMap.get("participant").getPrimaryKey(),
        contains("id"));
    assertThat(
        "there is no primary key in the file table", tableMap.get("file").getPrimaryKey(), empty());

    createdSnapshotIds.add(snapshotSummary.getId());
  }
}
