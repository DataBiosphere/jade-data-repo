package bio.terra.service.snapshotbuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.common.exception.NotFoundException;
import bio.terra.common.fixtures.DaoOperations;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.snapshot.Snapshot;
import java.io.IOException;
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
    snapshotBuilderSettingsDao.upsertSnapshotBuilderSettingsByDataset(
        dataset.getId(), SnapshotBuilderTestData.SETTINGS);
    snapshot =
        daoOperations.createSnapshotFromDataset(dataset, "snapshot-from-dataset-minimal.json");
    snapshotBuilderSettingsDao.upsertSnapshotBuilderSettingsBySnapshotId(
        snapshot.getId(), SnapshotBuilderTestData.SETTINGS);
  }

  @Test
  void getSettingsByDatasetIdReturnsSettings() {
    assertThat(
        "Snapshot builder settings should be the same as the example",
        snapshotBuilderSettingsDao.getSnapshotBuilderSettingsByDatasetId(dataset.getId()),
        equalTo(SnapshotBuilderTestData.SETTINGS));
  }

  @Test
  void getSettingsBySnapshotIdReturnsSettings() {
    assertThat(
        "Snapshot builder settings should be the same as the example",
        snapshotBuilderSettingsDao.getSnapshotBuilderSettingsBySnapshotId(snapshot.getId()),
        equalTo(SnapshotBuilderTestData.SETTINGS));
  }

  @Test
  void getSettingsForDatasetThatDoesNotExistErrors() {
    UUID unusedUUID = UUID.randomUUID();
    assertThrows(
        NotFoundException.class,
        () -> snapshotBuilderSettingsDao.getSnapshotBuilderSettingsByDatasetId(unusedUUID));
  }

  @Test
  void getSettingsForSnapshotThatDoesNotExistErrors() {
    UUID unusedUUID = UUID.randomUUID();
    assertThrows(
        NotFoundException.class,
        () -> snapshotBuilderSettingsDao.getSnapshotBuilderSettingsBySnapshotId(unusedUUID));
  }

  @Test
  void upsertSettingsByDatasetIdUpdatesWhenExisting() {
    assertThat(
        "Snapshot builder settings should be the new upserted value",
        snapshotBuilderSettingsDao.upsertSnapshotBuilderSettingsByDataset(
            dataset.getId(), SnapshotBuilderTestData.SETTINGS),
        equalTo(SnapshotBuilderTestData.SETTINGS));
  }

  @Test
  void upsertSettingsBySnapshotIdUpdatesWhenExisting() {
    assertThat(
        "Snapshot builder settings should be the new upserted value",
        snapshotBuilderSettingsDao.upsertSnapshotBuilderSettingsBySnapshotId(
            snapshot.getId(), SnapshotBuilderTestData.SETTINGS),
        equalTo(SnapshotBuilderTestData.SETTINGS));
  }

  @Test
  void upsertSettingsByDatasetIdCreatesWhenNotExisting() {
    snapshotBuilderSettingsDao.deleteByDatasetId(dataset.getId());
    assertThat(
        "Snapshot builder settings should be the same as the example",
        snapshotBuilderSettingsDao.upsertSnapshotBuilderSettingsByDataset(
            dataset.getId(), SnapshotBuilderTestData.SETTINGS),
        equalTo(SnapshotBuilderTestData.SETTINGS));
  }

  @Test
  void upsertSettingsBySnapshotIdCreatesWhenNotExisting() {
    snapshotBuilderSettingsDao.deleteBySnapshotId(snapshot.getId());
    assertThat(
        "Snapshot builder settings should be the same as the example",
        snapshotBuilderSettingsDao.upsertSnapshotBuilderSettingsBySnapshotId(
            snapshot.getId(), SnapshotBuilderTestData.SETTINGS),
        equalTo(SnapshotBuilderTestData.SETTINGS));
  }

  @Test
  void deleteSettingsByDatasetId() {
    snapshotBuilderSettingsDao.deleteByDatasetId(dataset.getId());
    assertThrows(
        NotFoundException.class,
        () -> snapshotBuilderSettingsDao.getSnapshotBuilderSettingsByDatasetId(snapshot.getId()),
        "There are no snapshot builder settings for the snapshot");
  }

  @Test
  void deleteSettingsBySnapshotId() {
    snapshotBuilderSettingsDao.deleteBySnapshotId(snapshot.getId());
    assertThrows(
        NotFoundException.class,
        () -> snapshotBuilderSettingsDao.getSnapshotBuilderSettingsBySnapshotId(snapshot.getId()),
        "There are no snapshot builder settings for the snapshot");
  }
}
