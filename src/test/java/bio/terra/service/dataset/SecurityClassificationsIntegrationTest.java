package bio.terra.service.dataset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.UsersBase;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSecurityClassification;
import bio.terra.model.DatasetSummaryModel;
import java.util.UUID;
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

    if (profileId != null) {
      dataRepoFixtures.deleteProfileLog(steward(), profileId);
    }
  }

  @Test
  public void testDatasetSecurityClassifications() throws Exception {
    DatasetSecurityClassification classification = DatasetSecurityClassification.CONTAINS_PHI;
    DatasetSummaryModel summary = datasetWithSecurityClassification(classification);
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), summary.getId());

    assertThat(
        String.format(
            "'%s' security classification was applied to summary model", classification.name()),
        summary.getSecurityClassification(),
        equalTo(classification));

    assertThat(
        String.format(
            "'%s' security classification was propagated to model", classification.name()),
        dataset.getSecurityClassification(),
        equalTo(classification));
  }

  private DatasetSummaryModel datasetWithSecurityClassification(
      DatasetSecurityClassification securityClassification) throws Exception {
    DatasetRequestModel requestModel =
        jsonLoader.loadObject("it-dataset-omop.json", DatasetRequestModel.class);
    requestModel.setDefaultProfileId(profileId);
    requestModel.setName(Names.randomizeName(requestModel.getName()));
    requestModel.setCloudPlatform(CloudPlatform.GCP);
    requestModel.setSecurityClassification(securityClassification);
    DatasetSummaryModel summaryModel =
        dataRepoFixtures.createDataset(steward(), requestModel, false);
    datasetId = summaryModel.getId();
    return summaryModel;
  }
}
