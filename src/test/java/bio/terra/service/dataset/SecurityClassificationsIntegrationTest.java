package bio.terra.service.dataset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.UsersBase;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

// TODO move me to integration dir
@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class SecurityClassificationsIntegrationTest extends UsersBase {

  private static Logger logger =
      LoggerFactory.getLogger(SecurityClassificationsIntegrationTest.class);

  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private AuthService authService;
  @Autowired private JsonLoader jsonLoader;

  private UUID datasetId;
  private UUID snapshotId;
  private UUID profileId;

  @Before
  public void setup() throws Exception {
    super.setup();
    dataRepoFixtures.resetConfig(steward());
    profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
    datasetId = null;
  }

  @After
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
  public void testDatasetWithSecureMonitoring() throws Exception {
    DatasetSummaryModel summary = datasetWithSecureMonitoring(true);
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), summary.getId());

    assertThat(
        "Secure monitoring enabled on the dataset summary model",
        summary.isSecureMonitoringEnabled(),
        is(true));

    assertThat(
        "Secure monitoring flag was propagated to the dataset model",
        dataset.isSecureMonitoringEnabled(),
        is(true));

    String datasetName = dataset.getName();
    SnapshotRequestModel requestModel =
        jsonLoader.loadObject("ingest-test-snapshot-fullviews.json", SnapshotRequestModel.class);
    // swap in the correct dataset name (with the id at the end)
    requestModel.getContents().get(0).setDatasetName(datasetName);
    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshotWithRequest(steward(), datasetName, profileId, requestModel);
    TimeUnit.SECONDS.sleep(10);
    snapshotId = snapshotSummary.getId();

    assertThat(
        "Snapshot summary denotes secure monitoring enabled",
        snapshotSummary.isSecureMonitoringEnabled(),
        is(true));

    SnapshotModel snapshot = dataRepoFixtures.getSnapshot(steward(), snapshotId, List.of());

    assertThat(
        "Snapshot model denotes secure monitoring enabled",
        snapshot.isSecureMonitoringEnabled(),
        is(true));

    Optional<SnapshotSummaryModel> enumeratedModel =
        dataRepoFixtures.enumerateSnapshots(steward()).getItems().stream()
            .filter(s -> s.getId().equals(snapshotId))
            .findFirst();

    assertThat(
        "Enumerated snapshot model has secure monitoring flag",
        enumeratedModel.get().isSecureMonitoringEnabled(),
        is(true));

    Optional<SnapshotSummaryModel> enumeratedByDatasetModel =
        dataRepoFixtures
            .enumerateSnapshotsByDatasetIds(steward(), List.of(datasetId))
            .getItems()
            .stream()
            .filter(s -> s.getId().equals(snapshotId))
            .findFirst();

    assertThat(
        "Enumerated by dataset id snapshot model has secure monitoring flag",
        enumeratedByDatasetModel.get().isSecureMonitoringEnabled(),
        is(true));
  }

  private DatasetSummaryModel datasetWithSecureMonitoring(boolean secureMonitoringEnabled)
      throws Exception {
    DatasetRequestModel requestModel =
        jsonLoader.loadObject("ingest-test-dataset.json", DatasetRequestModel.class);
    requestModel.setDefaultProfileId(profileId);
    requestModel.setName(Names.randomizeName(requestModel.getName()));
    requestModel.setCloudPlatform(CloudPlatform.GCP);
    requestModel.setSecureMonitoringEnabled(secureMonitoringEnabled);
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
