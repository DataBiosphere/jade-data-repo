package bio.terra.service.dataset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.IntegrationTestConfiguration;
import bio.terra.integration.Users;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import bio.terra.service.resourcemanagement.google.GoogleResourceManagerService;
import com.google.api.services.cloudresourcemanager.model.Project;
import com.google.api.services.cloudresourcemanager.model.ResourceId;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

// TODO move me to integration dir
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = IntegrationTestConfiguration.class)
@ActiveProfiles({"google", "integrationtest"})
@Tag(Integration.TAG)
class SecureMonitoringIntegrationTest {

  @Autowired private Users users;
  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private JsonLoader jsonLoader;
  @Autowired private GoogleResourceManagerService resourceManagerService;
  @Autowired private GoogleResourceConfiguration googleResourceConfiguration;

  private Users.TestUsers testUsers;
  private UUID datasetId;
  private UUID snapshotId;
  private UUID profileId;

  private TestConfiguration.User steward() {
    return testUsers.steward();
  }

  @BeforeEach
  public void setup() throws Exception {
    testUsers = users.testUsers();
    dataRepoFixtures.resetConfig(steward());
    profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
    datasetId = null;
  }

  @AfterEach
  public void teardown() throws Exception {
    dataRepoFixtures.resetConfig(steward());

    if (datasetId != null) {
      dataRepoFixtures.deleteDatasetLog(steward(), datasetId);
    }

    if (snapshotId != null) {
      dataRepoFixtures.deleteSnapshotLog(steward(), snapshotId);
    }

    if (profileId != null) {
      dataRepoFixtures.deleteProfileLog(steward(), profileId);
    }
  }

  @Test
  void testDatasetWithSecureMonitoring() throws Exception {
    DatasetSummaryModel summary = datasetWithSecureMonitoring();
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), summary.getId());

    assertThat(
        "Secure monitoring enabled on the dataset summary model",
        summary.isSecureMonitoringEnabled());

    assertThat(
        "Secure monitoring flag was propagated to the dataset model",
        dataset.isSecureMonitoringEnabled());

    var datasetGoogleDataProject = dataset.getDataProject();
    Project datasetProject = resourceManagerService.getProject(datasetGoogleDataProject);
    ResourceId datasetParent = datasetProject.getParent();
    assertThat(
        "The parent of the dataset project is in the 'secure' folder",
        datasetParent.getId(),
        equalTo(googleResourceConfiguration.secureFolderResourceId()));

    String datasetName = dataset.getName();
    SnapshotRequestModel requestModel =
        jsonLoader.loadObject("ingest-test-snapshot-fullviews.json", SnapshotRequestModel.class);
    // swap in the correct dataset name (with the id at the end)
    requestModel.getContents().get(0).setDatasetName(datasetName);
    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshotWithRequest(steward(), datasetName, profileId, requestModel);
    snapshotId = snapshotSummary.getId();

    assertThat(
        "Snapshot summary denotes secure monitoring enabled",
        snapshotSummary.isSecureMonitoringEnabled());

    SnapshotModel snapshot =
        Awaitility.waitAtMost(Duration.ofSeconds(10))
            .until(
                () -> dataRepoFixtures.getSnapshot(steward(), snapshotSummary.getId(), null),
                Objects::nonNull);

    assertThat(
        "Snapshot model denotes secure monitoring enabled",
        snapshot.getSource().get(0).getDataset().isSecureMonitoringEnabled());

    SnapshotSummaryModel enumeratedModel =
        dataRepoFixtures.enumerateSnapshots(steward()).getItems().stream()
            .filter(s -> s.getId().equals(snapshotId))
            .findFirst()
            .orElseThrow();

    assertThat(
        "Enumerated snapshot model has secure monitoring flag",
        enumeratedModel.isSecureMonitoringEnabled());

    SnapshotSummaryModel enumeratedByDatasetModel =
        dataRepoFixtures
            .enumerateSnapshotsByDatasetIds(steward(), List.of(datasetId))
            .getItems()
            .stream()
            .filter(s -> s.getId().equals(snapshotId))
            .findFirst()
            .orElseThrow();

    assertThat(
        "Enumerated by dataset id snapshot model has secure monitoring flag",
        enumeratedByDatasetModel.isSecureMonitoringEnabled());

    var snapshotGoogleProject = snapshot.getDataProject();
    Project snapshotProject = resourceManagerService.getProject(snapshotGoogleProject);
    ResourceId snapshotParent = snapshotProject.getParent();
    assertThat(
        "The parent of the snapshot project is in the 'secure' folder",
        snapshotParent.getId(),
        equalTo(googleResourceConfiguration.secureFolderResourceId()));
  }

  private DatasetSummaryModel datasetWithSecureMonitoring() throws Exception {
    DatasetRequestModel requestModel =
        jsonLoader.loadObject("ingest-test-dataset.json", DatasetRequestModel.class);
    requestModel.setDefaultProfileId(profileId);
    requestModel.setName(Names.randomizeName(requestModel.getName()));
    requestModel.setCloudPlatform(CloudPlatform.GCP);
    requestModel.setEnableSecureMonitoring(true);
    requestModel.dedicatedIngestServiceAccount(false);
    DatasetSummaryModel summaryModel =
        dataRepoFixtures.createDataset(steward(), requestModel, false);
    datasetId = summaryModel.getId();

    IngestRequestModel request =
        dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json");
    dataRepoFixtures.ingestJsonData(steward(), datasetId, request);
    request = dataRepoFixtures.buildSimpleIngest("sample", "ingest-test/ingest-test-sample.json");
    dataRepoFixtures.ingestJsonData(steward(), datasetId, request);
    return summaryModel;
  }
}
