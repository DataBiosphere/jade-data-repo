package bio.terra.service.resourcemanagement;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

import bio.terra.buffer.model.CloudResourceUid;
import bio.terra.buffer.model.GoogleProjectUid;
import bio.terra.buffer.model.ResourceInfo;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetUtils;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNamingException;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import java.io.IOException;
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
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class OneProjectPerResourceUnitTest {

  @Autowired private JsonLoader jsonLoader;

  @Autowired private DatasetDao datasetDao;

  @Autowired private ProfileDao profileDao;

  @Autowired private GoogleResourceDao resourceDao;

  @Autowired private DataLocationSelector dataLocationSelector;

  @MockBean private BufferService bufferService;

  @Autowired private GoogleResourceConfiguration resourceConfiguration;

  private List<BillingProfileModel> billingProfiles;
  private List<GoogleProjectResource> projects;
  private List<Dataset> datasets;
  private String dataProjectPrefix;

  @Before
  public void setup() throws IOException, InterruptedException, GoogleResourceNamingException {
    dataProjectPrefix = resourceConfiguration.getDataProjectPrefix();
    resourceConfiguration.setDataProjectPrefix("tdr-int");

    // Initialize lists
    billingProfiles = new ArrayList<>();
    projects = new ArrayList<>();
    datasets = new ArrayList<>();

    // Two billing profiles
    BillingProfileRequestModel profileRequest1 = ProfileFixtures.randomBillingProfileRequest();
    billingProfiles.add(profileDao.createBillingProfile(profileRequest1, "testUser"));

    BillingProfileRequestModel profileRequest2 = ProfileFixtures.randomBillingProfileRequest();
    billingProfiles.add(profileDao.createBillingProfile(profileRequest2, "testUser"));

    // two google project resources
    GoogleProjectResource projectResource1 =
        ResourceFixtures.randomProjectResource(billingProfiles.get(0));
    String projectId1 = ProfileFixtures.randomizeName("fake-test-project-id");
    projectResource1.googleProjectId(projectId1);
    UUID projectResourceId1 = resourceDao.createProject(projectResource1);
    projectResource1.id(projectResourceId1);
    projectResource1.profileId(billingProfiles.get(0).getId());
    projects.add(projectResource1);

    GoogleProjectResource projectResource2 =
        ResourceFixtures.randomProjectResource(billingProfiles.get(1));
    String projectId2 = ProfileFixtures.randomizeName("fake-test-project-id");
    projectResource2.googleProjectId(projectId2);
    UUID projectResourceId2 = resourceDao.createProject(projectResource2);
    projectResource2.id(projectResourceId2);
    projectResource2.profileId(billingProfiles.get(1).getId());
    projects.add(projectResource2);
  }

  @After
  public void teardown() {
    for (Dataset dataset : datasets) {
      datasetDao.delete(dataset.getId());
    }
    for (GoogleProjectResource project : projects) {
      resourceDao.deleteProject(project.getId());
    }
    for (BillingProfileModel billingProfile : billingProfiles) {
      profileDao.deleteBillingProfileById(billingProfile.getId());
    }
    resourceConfiguration.setDataProjectPrefix(dataProjectPrefix);
  }

  @Test
  public void bucketPerDatasetPerBilling() throws Exception {
    Dataset dataset = createDataset(billingProfiles.get(0), projects.get(0));
    String datasetProjectId = projects.get(0).getGoogleProjectId();
    datasets.add(dataset);

    // Same billing profile as source dataset, so same project id
    String fileProjectId =
        dataLocationSelector.projectIdForFile(dataset, datasetProjectId, billingProfiles.get(0));
    assertThat(
        "For same billing, dataset and file project are the same",
        fileProjectId,
        equalTo(datasetProjectId));
    String bucketProjectId =
        dataLocationSelector.bucketForFile(projects.get(0).getGoogleProjectId());
    assertThat(
        "File project are the same, plus bucket suffix",
        bucketProjectId,
        equalTo(fileProjectId + "-bucket"));

    // Different billing profile than source dataset
    BillingProfileModel newBillingProfile = billingProfiles.get(0).id(UUID.randomUUID());

    var rbsProjectId = "foobar";
    given(bufferService.handoutResource(any()))
        .willReturn(
            new ResourceInfo()
                .cloudResourceUid(
                    new CloudResourceUid()
                        .googleProjectUid(new GoogleProjectUid().projectId(rbsProjectId))));
    String diffFileProjectId =
        dataLocationSelector.projectIdForFile(dataset, datasetProjectId, newBillingProfile);
    assertThat(
        "For different billing, dataset and file project live in different projects",
        diffFileProjectId,
        not(datasetProjectId));
    assertThat(
        "The new project id for file was retrieved from RBS",
        diffFileProjectId,
        equalTo(rbsProjectId));

    String diffBucketProjectId = dataLocationSelector.bucketForFile(diffFileProjectId);
    assertThat(
        "File project are the same, plus bucket suffix",
        diffBucketProjectId,
        equalTo(diffFileProjectId + "-bucket"));
  }

  @Test
  public void twoDatasetsTwoBuckets() throws Exception {
    // Two dataset, two billing profiles
    datasets.add(createDataset(billingProfiles.get(0), projects.get(0)));
    datasets.add(createDataset(billingProfiles.get(1), projects.get(1)));

    String bucketProject1 =
        dataLocationSelector.projectIdForFile(
            datasets.get(0), projects.get(0).getGoogleProjectId(), billingProfiles.get(0));
    String bucketName1 = dataLocationSelector.bucketForFile(bucketProject1);
    String bucketProject2 =
        dataLocationSelector.projectIdForFile(
            datasets.get(1), projects.get(1).getGoogleProjectId(), billingProfiles.get(1));
    String bucketName2 = dataLocationSelector.bucketForFile(bucketProject2);

    assertNotEquals("Buckets should be named differently", bucketName1, bucketName2);
  }

  @Test
  public void oneDatasetsTwoBilling() throws Exception {
    // One dataset, two billing profiles
    datasets.add(createDataset(billingProfiles.get(0), projects.get(0)));

    String bucketProject1 =
        dataLocationSelector.projectIdForFile(
            datasets.get(0), projects.get(0).getGoogleProjectId(), billingProfiles.get(0));
    assertThat(
        "Dataset and bucket project should match since using same billing profile.",
        bucketProject1,
        equalTo(projects.get(0).getGoogleProjectId()));
    String bucketName1 = dataLocationSelector.bucketForFile(bucketProject1);
    assertThat(
        "Bucket project and bucket name should be the same plus -bucket suffix",
        bucketProject1 + "-bucket",
        equalTo(bucketName1));

    // Different billing profile
    var rbsProjectId = "foobar";
    given(bufferService.handoutResource(any()))
        .willReturn(
            new ResourceInfo()
                .cloudResourceUid(
                    new CloudResourceUid()
                        .googleProjectUid(new GoogleProjectUid().projectId(rbsProjectId))));
    String bucketProject2 =
        dataLocationSelector.projectIdForFile(
            datasets.get(0), projects.get(1).getGoogleProjectId(), billingProfiles.get(1));
    assertThat(
        "Dataset and bucket project should NOT match since they are using different billing profiles.",
        bucketProject2,
        not(projects.get(0).getGoogleProjectId()));
    String bucketName2 = dataLocationSelector.bucketForFile(bucketProject2);

    assertThat(
        "Project ID is retrieved from new RBS project", bucketProject2, equalTo(rbsProjectId));

    assertThat(
        "Bucket project and bucket name should be the same plus -bucket suffix",
        bucketProject2 + "-bucket",
        equalTo(bucketName2));
  }

  @Test(expected = GoogleResourceNamingException.class)
  public void noUpperCaseBucketNames() throws GoogleResourceNamingException {
    dataLocationSelector.bucketForFile("PROJECTID");
  }

  @Test(expected = GoogleResourceNamingException.class)
  public void tooLongBucketName() throws GoogleResourceNamingException {
    String bucketName =
        dataLocationSelector.bucketForFile(
            "project.test_test_project.test_test_project.test_test_project.test_test");
    assertNotNull("bucket name should not be null", bucketName);
  }

  @Test
  public void allowedBucketName() throws GoogleResourceNamingException {
    dataLocationSelector.bucketForFile("project.test_test_project.test_test_project.test");
  }

  private Dataset createDataset(BillingProfileModel billingProfile, GoogleProjectResource project)
      throws IOException {
    Dataset dataset;
    DatasetRequestModel datasetRequest1 =
        jsonLoader.loadObject("dataset-minimal.json", DatasetRequestModel.class);
    datasetRequest1
        .name(datasetRequest1.getName() + UUID.randomUUID())
        .defaultProfileId(billingProfile.getId())
        .cloudPlatform(CloudPlatform.GCP);
    dataset = DatasetUtils.convertRequestWithGeneratedNames(datasetRequest1);
    dataset.projectResource(project);
    dataset.projectResourceId(project.getId());
    String createFlightId1 = UUID.randomUUID().toString();
    dataset.id(UUID.randomUUID());
    datasetDao.createAndLock(dataset, createFlightId1);
    datasetDao.unlockExclusive(dataset.getId(), createFlightId1);
    return dataset;
  }
}
