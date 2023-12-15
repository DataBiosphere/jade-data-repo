package bio.terra.service.snapshotbuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.common.exception.NotFoundException;
import bio.terra.common.fixtures.DaoOperations;
import bio.terra.service.dataset.Dataset;
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

  @BeforeEach
  void setup() throws IOException {
    dataset = daoOperations.createMinimalDataset();
    snapshotBuilderSettingsDao.upsertSnapshotBuilderSettingsByDataset(
        dataset.getId(), SnapshotBuilderTestData.SETTINGS);
  }

  @Test
  void getSnapshotBuilderSettingsReturnsSettings() {
    assertThat(
        "Snapshot builder settings should be the same as the example",
        snapshotBuilderSettingsDao.getSnapshotBuilderSettingsByDatasetId(dataset.getId()),
        equalTo(SnapshotBuilderTestData.SETTINGS));
  }

  @Test
  void getSnapshotBuilderSettingsForDatasetThatDoesNotExistErrors() {
    UUID unusedUUID = UUID.randomUUID();
    assertThrows(
        NotFoundException.class,
        () -> snapshotBuilderSettingsDao.getSnapshotBuilderSettingsByDatasetId(unusedUUID));
  }

  @Test
  void upsertSnapshotBuilderSettingsUpdatesWhenExisting() {
    assertThat(
        "Snapshot builder settings should be the new upserted value",
        snapshotBuilderSettingsDao.upsertSnapshotBuilderSettingsByDataset(
            dataset.getId(), SnapshotBuilderTestData.SETTINGS),
        equalTo(SnapshotBuilderTestData.SETTINGS));
  }

  @Test
  void upsertSnapshotBuilderSettingsCreatesWhenNotExisting() {
    snapshotBuilderSettingsDao.delete(dataset.getId());
    assertThat(
        "Snapshot builder settings should be the same as the example",
        snapshotBuilderSettingsDao.upsertSnapshotBuilderSettingsByDataset(
            dataset.getId(), SnapshotBuilderTestData.SETTINGS),
        equalTo(SnapshotBuilderTestData.SETTINGS));
  }
}
