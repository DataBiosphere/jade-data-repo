package bio.terra.service.snapshotbuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.common.exception.NotFoundException;
import bio.terra.common.fixtures.DaoOperations;
import bio.terra.model.SnapshotBuilderSettings;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.snapshot.Snapshot;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Tag(Unit.TAG)
@EmbeddedDatabaseTest
class SnapshotBuilderSettingsDaoTest {

  @Autowired private DaoOperations daoOperations;
  @Autowired private SnapshotBuilderSettingsDao snapshotBuilderSettingsDao;
  private Dataset dataset;
  private Snapshot snapshot;

  @BeforeEach
  void setup() throws IOException {
    dataset = daoOperations.createDataset("dataset-minimal.json");
    snapshotBuilderSettingsDao.upsertByDatasetId(dataset.getId(), SnapshotBuilderTestData.SETTINGS);
    snapshot = daoOperations.createAndIngestSnapshot(dataset, "snapshot-from-dataset-minimal.json");
    snapshotBuilderSettingsDao.upsertBySnapshotId(
        snapshot.getId(), SnapshotBuilderTestData.SETTINGS);
  }

  @Test
  void getByDatasetIdReturnsSettings() {
    assertThat(
        "Snapshot builder settings should be the same as the example",
        snapshotBuilderSettingsDao.getByDatasetId(dataset.getId()),
        equalTo(SnapshotBuilderTestData.SETTINGS));
  }

  @Test
  void getBySnapshotIdReturnsSettings() {
    assertThat(
        "Snapshot builder settings should be the same as the example",
        snapshotBuilderSettingsDao.getBySnapshotId(snapshot.getId()),
        equalTo(SnapshotBuilderTestData.SETTINGS));
  }

  @Test
  void getByDatasetIdThatDoesNotExistErrors() {
    UUID unusedUUID = UUID.randomUUID();
    assertThrows(
        NotFoundException.class, () -> snapshotBuilderSettingsDao.getByDatasetId(unusedUUID));
  }

  @Test
  void getBySnapshotIdThatDoesNotExistErrors() {
    UUID unusedUUID = UUID.randomUUID();
    assertThrows(
        NotFoundException.class, () -> snapshotBuilderSettingsDao.getBySnapshotId(unusedUUID));
  }

  @Test
  void upsertByDatasetIdUpdatesWhenExisting() {
    SnapshotBuilderSettings newSettings =
        SnapshotBuilderTestData.SETTINGS.datasetConceptSets(List.of());
    assertThat(
        "Snapshot builder settings should be the new upserted value",
        snapshotBuilderSettingsDao.upsertByDatasetId(dataset.getId(), newSettings),
        // the new settings should be returned, not the original settings set in setup()
        equalTo(newSettings));
  }

  @Test
  void upsertBySnapshotIdUpdatesWhenExisting() {
    SnapshotBuilderSettings newSettings =
        SnapshotBuilderTestData.SETTINGS.datasetConceptSets(List.of());
    assertThat(
        "Snapshot builder settings should be the new upserted value",
        snapshotBuilderSettingsDao.upsertBySnapshotId(snapshot.getId(), newSettings),
        // the new settings should be returned, not the original settings set in setup()
        equalTo(newSettings));
  }

  @Test
  void upsertByDatasetIdCreatesWhenNotExisting() {
    snapshotBuilderSettingsDao.deleteByDatasetId(dataset.getId());
    assertThat(
        "Snapshot builder settings should be the same as the example",
        snapshotBuilderSettingsDao.upsertByDatasetId(
            dataset.getId(), SnapshotBuilderTestData.SETTINGS),
        equalTo(SnapshotBuilderTestData.SETTINGS));
  }

  @Test
  void upsertBySnapshotIdCreatesWhenNotExisting() {
    snapshotBuilderSettingsDao.deleteBySnapshotId(snapshot.getId());
    assertThat(
        "Snapshot builder settings should be the same as the example",
        snapshotBuilderSettingsDao.upsertBySnapshotId(
            snapshot.getId(), SnapshotBuilderTestData.SETTINGS),
        equalTo(SnapshotBuilderTestData.SETTINGS));
  }

  @Test
  void deleteByDatasetId() {
    snapshotBuilderSettingsDao.deleteByDatasetId(dataset.getId());
    UUID datasetId = dataset.getId();
    assertThrows(
        NotFoundException.class,
        () -> snapshotBuilderSettingsDao.getByDatasetId(datasetId),
        "There are no snapshot builder settings for the dataset");
  }

  @Test
  void deleteBySnapshotId() {
    snapshotBuilderSettingsDao.deleteBySnapshotId(snapshot.getId());
    UUID snapshotId = snapshot.getId();
    assertThrows(
        NotFoundException.class,
        () -> snapshotBuilderSettingsDao.getBySnapshotId(snapshotId),
        "There are no snapshot builder settings for the snapshot");
  }
}
