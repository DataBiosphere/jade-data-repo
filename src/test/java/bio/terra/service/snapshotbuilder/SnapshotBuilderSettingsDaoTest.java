package bio.terra.service.snapshotbuilder;

import static bio.terra.service.snapshotbuilder.SnapshotBuilderTestData.SAMPLE_SNAPSHOT_BUILDER_SETTINGS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.common.exception.NotFoundException;
import bio.terra.common.fixtures.DaoOperations;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.SnapshotBuilderSettings;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
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
  @Autowired private ProfileDao profileDao;
  @Autowired private GoogleResourceDao resourceDao;
  @Autowired private SnapshotBuilderSettingsDao snapshotBuilderSettingsDao;

  private BillingProfileModel billingProfile;
  private GoogleProjectResource projectResource;
  private Dataset dataset;

  @BeforeEach
  void setup() throws IOException {
    BillingProfileRequestModel profileRequest = ProfileFixtures.randomBillingProfileRequest();
    billingProfile = profileDao.createBillingProfile(profileRequest, "testUser");

    projectResource = ResourceFixtures.randomProjectResource(billingProfile);
    UUID projectId = resourceDao.createProject(projectResource);
    projectResource.id(projectId);

    dataset = daoOperations.createMinimalDataset(billingProfile.getId(), projectId);
    snapshotBuilderSettingsDao.upsertSnapshotBuilderSettingsByDataset(
        dataset.getId(), SAMPLE_SNAPSHOT_BUILDER_SETTINGS);
  }

  @Test
  void getSnapshotBuilderSettingsReturnsSettings() {
    assertThat(
        "Snapshot builder settings should be the same as the example",
        snapshotBuilderSettingsDao.getSnapshotBuilderSettingsByDatasetId(dataset.getId()),
        equalTo(SAMPLE_SNAPSHOT_BUILDER_SETTINGS));
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
            dataset.getId(), new SnapshotBuilderSettings()),
        equalTo(new SnapshotBuilderSettings()));
  }

  @Test
  void upsertSnapshotBuilderSettingsCreatesWhenNotExisting() {
    snapshotBuilderSettingsDao.delete(dataset.getId());
    assertThat(
        "Snapshot builder settings should be the same as the example",
        snapshotBuilderSettingsDao.upsertSnapshotBuilderSettingsByDataset(
            dataset.getId(), SAMPLE_SNAPSHOT_BUILDER_SETTINGS),
        equalTo(SAMPLE_SNAPSHOT_BUILDER_SETTINGS));
  }
}
