package bio.terra.service.snapshotbuilder;

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
import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.model.SnapshotBuilderDatasetConceptSets;
import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.model.SnapshotBuilderFeatureValueGroup;
import bio.terra.model.SnapshotBuilderListOption;
import bio.terra.model.SnapshotBuilderProgramDataOption;
import bio.terra.model.SnapshotBuilderSettings;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetBucketDaoTest;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import java.io.IOException;
import java.util.List;
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
    snapshotBuilderSettingsDao.upsertSnapshotBuilderSettingsByDataset(
        dataset.getId(), new SnapshotBuilderSettings());
    assertThat(
        "Snapshot builder settings should be the new upserted value",
        snapshotBuilderSettingsDao.getSnapshotBuilderSettingsByDatasetId(dataset.getId()),
        equalTo(new SnapshotBuilderSettings()));
  }

  @Test
  public void upsertSnapshotBuilderSettingsCreatesWhenNotExisting() {
    snapshotBuilderSettingsDao.delete(dataset.getId());
    snapshotBuilderSettingsDao.upsertSnapshotBuilderSettingsByDataset(
        dataset.getId(), SAMPLE_SNAPSHOT_BUILDER_SETTINGS);
    assertThat(
        "Snapshot builder settings should be the same as the example",
        snapshotBuilderSettingsDao.getSnapshotBuilderSettingsByDatasetId(dataset.getId()),
        equalTo(SAMPLE_SNAPSHOT_BUILDER_SETTINGS));
  }

  private static final SnapshotBuilderSettings SAMPLE_SNAPSHOT_BUILDER_SETTINGS =
      new SnapshotBuilderSettings()
          .domainOptions(
              List.of(
                  new SnapshotBuilderDomainOption()
                      .id(10)
                      .category("Condition")
                      .conceptCount(18000)
                      .participantCount(12500)
                      .root(
                          new SnapshotBuilderConcept()
                              .id(100)
                              .name("Condition")
                              .count(100)
                              .hasChildren(true)),
                  new SnapshotBuilderDomainOption()
                      .id(11)
                      .category("Procedure")
                      .conceptCount(22500)
                      .participantCount(11328)
                      .root(
                          new SnapshotBuilderConcept()
                              .id(200)
                              .name("Procedure")
                              .count(100)
                              .hasChildren(true)),
                  new SnapshotBuilderDomainOption()
                      .id(12)
                      .category("Observation")
                      .conceptCount(12300)
                      .participantCount(23223)
                      .root(
                          new SnapshotBuilderConcept()
                              .id(300)
                              .name("Observation")
                              .count(100)
                              .hasChildren(true))))
          .programDataOptions(
              List.of(
                  new SnapshotBuilderProgramDataOption()
                      .id(1)
                      .name("Year of birth")
                      .kind(SnapshotBuilderProgramDataOption.KindEnum.RANGE)
                      .min(1900)
                      .max(2023),
                  new SnapshotBuilderProgramDataOption()
                      .id(2)
                      .name("Ethnicity")
                      .kind(SnapshotBuilderProgramDataOption.KindEnum.LIST)
                      .values(
                          List.of(
                              new SnapshotBuilderListOption().name("Hispanic or Latino").id(20),
                              new SnapshotBuilderListOption().name("Not Hispanic or Latino").id(21),
                              new SnapshotBuilderListOption().name("No Matching Concept").id(0))),
                  new SnapshotBuilderProgramDataOption()
                      .id(3)
                      .name("Gender identity")
                      .kind(SnapshotBuilderProgramDataOption.KindEnum.LIST)
                      .values(
                          List.of(
                              new SnapshotBuilderListOption().name("FEMALE").id(22),
                              new SnapshotBuilderListOption().name("MALE").id(23),
                              new SnapshotBuilderListOption().name("NON BINARY").id(24),
                              new SnapshotBuilderListOption().name("GENDERQUEER").id(25),
                              new SnapshotBuilderListOption().name("TWO SPIRIT").id(26),
                              new SnapshotBuilderListOption().name("AGENDER").id(27),
                              new SnapshotBuilderListOption().name("No Matching Concept").id(0))),
                  new SnapshotBuilderProgramDataOption()
                      .id(4)
                      .name("Race")
                      .kind(SnapshotBuilderProgramDataOption.KindEnum.LIST)
                      .values(
                          List.of(
                              new SnapshotBuilderListOption()
                                  .name("American Indian or Alaska Native")
                                  .id(28),
                              new SnapshotBuilderListOption().name("Asian").id(29),
                              new SnapshotBuilderListOption().name("Black").id(30),
                              new SnapshotBuilderListOption().name("White").id(31)))))
          .featureValueGroups(
              List.of(
                  new SnapshotBuilderFeatureValueGroup()
                      .id(0)
                      .name("Condition")
                      .values(List.of("Condition Column 1", "Condition Column 2")),
                  new SnapshotBuilderFeatureValueGroup()
                      .id(1)
                      .name("Observation")
                      .values(List.of("Observation Column 1", "Observation Column 2")),
                  new SnapshotBuilderFeatureValueGroup()
                      .id(2)
                      .name("Procedure")
                      .values(List.of("Procedure Column 1", "Procedure Column 2")),
                  new SnapshotBuilderFeatureValueGroup()
                      .id(3)
                      .name("Surveys")
                      .values(List.of("Surveys Column 1", "Surveys Column 2")),
                  new SnapshotBuilderFeatureValueGroup()
                      .id(4)
                      .name("Person")
                      .values(List.of("Demographics Column 1", "Demographics Column 2"))))
          .datasetConceptSets(
              List.of(
                  new SnapshotBuilderDatasetConceptSets()
                      .name("Demographics")
                      .featureValueGroupName("Person"),
                  new SnapshotBuilderDatasetConceptSets()
                      .name("All surveys")
                      .featureValueGroupName("Surveys")));
}
