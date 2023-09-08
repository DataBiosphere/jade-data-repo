package bio.terra.service.snapshotbuilder;

import static bio.terra.service.snapshotbuilder.SnapshotBuilderTestData.SAMPLE_SNAPSHOT_BUILDER_SETTINGS;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.DaoOperations;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.SnapshotBuilderSettings;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetBucketDaoTest;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import java.io.IOException;
import java.util.UUID;
import javax.ws.rs.NotFoundException;
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

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
@EmbeddedDatabaseTest
public class SnapshotBuilderSettingsDaoTest {
  private static final Logger logger = LoggerFactory.getLogger(DatasetBucketDaoTest.class);

  @Autowired private DaoOperations daoOperations;
  @Autowired private ProfileDao profileDao;
  @Autowired private GoogleResourceDao resourceDao;
  @Autowired private DatasetDao datasetDao;
  @Autowired private SnapshotBuilderSettingsDao snapshotBuilderSettingsDao;

  private BillingProfileModel billingProfile;
  private GoogleProjectResource projectResource;
  private Dataset dataset;
  private SnapshotBuilderSettings snapshotBuilderSettings;

  @Before
  public void setup() throws IOException {
    BillingProfileRequestModel profileRequest = ProfileFixtures.randomBillingProfileRequest();
    billingProfile = profileDao.createBillingProfile(profileRequest, "testUser");

    projectResource = ResourceFixtures.randomProjectResource(billingProfile);
    UUID projectId = resourceDao.createProject(projectResource);
    projectResource.id(projectId);

    dataset = daoOperations.createMinimalDataset(billingProfile.getId(), projectId);
    snapshotBuilderSettings =
        snapshotBuilderSettingsDao.upsertSnapshotBuilderSettingsByDataset(
            dataset.getId(), SAMPLE_SNAPSHOT_BUILDER_SETTINGS);
  }

  @After
  public void teardown() {
    try {
      snapshotBuilderSettingsDao.delete(dataset.getId());
    } catch (Exception ex) {
      logger.error(
          "[CLEANUP] Unable to delete snapshot builder settings for dataset {}", dataset.getId());
    }
    try {
      datasetDao.delete(dataset.getId());
    } catch (Exception ex) {
      logger.error("[CLEANUP] Unable to delete dataset {}", dataset.getId());
    }
    try {
      resourceDao.deleteProject(projectResource.getId());
    } catch (Exception ex) {
      logger.error(
          "[CLEANUP] Unable to delete entry in database for projects {}", projectResource.getId());
    }
    try {
      profileDao.deleteBillingProfileById(billingProfile.getId());
    } catch (Exception ex) {
      logger.error("[CLEANUP] Unable to billing profile {}", billingProfile.getId());
    }
  }

  @Test
  public void getSnapshotBuilderSettingsReturnsSettings() {
    assertThat(
        "Snapshot builder settings should be the same as the example",
        snapshotBuilderSettingsDao.getSnapshotBuilderSettingsByDatasetId(dataset.getId()),
        equalTo(SAMPLE_SNAPSHOT_BUILDER_SETTINGS));
  }

  @Test
  public void getSnapshotBuilderSettingsForDatasetThatDoesNotExistErrors() {
    assertThrows(
        NotFoundException.class,
        () -> snapshotBuilderSettingsDao.getSnapshotBuilderSettingsByDatasetId(UUID.randomUUID()));
  }

  @Test
  public void upsertSnapshotBuilderSettingsUpdatesWhenExisting() {
    assertThat(
        "Snapshot builder settings should be the new upserted value",
        snapshotBuilderSettingsDao.upsertSnapshotBuilderSettingsByDataset(
            dataset.getId(), new SnapshotBuilderSettings()),
        equalTo(new SnapshotBuilderSettings()));
  }

  @Test
  public void upsertSnapshotBuilderSettingsCreatesWhenNotExisting() {
    snapshotBuilderSettingsDao.delete(dataset.getId());
    assertThat(
        "Snapshot builder settings should be the same as the example",
        snapshotBuilderSettingsDao.upsertSnapshotBuilderSettingsByDataset(
            dataset.getId(), SAMPLE_SNAPSHOT_BUILDER_SETTINGS),
        equalTo(SAMPLE_SNAPSHOT_BUILDER_SETTINGS));
  }
}
