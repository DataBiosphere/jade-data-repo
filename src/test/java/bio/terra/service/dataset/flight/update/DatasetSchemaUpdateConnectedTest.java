package bio.terra.service.dataset.flight.update;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetDao;
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
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
@EmbeddedDatabaseTest
public class DatasetSchemaUpdateConnectedTest {

  @Autowired private MockMvc mvc;
  @Autowired private JsonLoader jsonLoader;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private DatasetDao datasetDao;
  @Autowired private ConfigurationService configService;
  @Autowired private ConnectedTestConfiguration testConfig;

  @MockBean private IamProviderInterface samService;

  private BillingProfileModel billingProfile;
  private DatasetSummaryModel summaryModel;
  private static final Logger logger =
      LoggerFactory.getLogger(DatasetSchemaUpdateConnectedTest.class);

  @Before
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    configService.reset();
    billingProfile =
        connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());
    // create a dataset and check that it succeeds
    String resourcePath = "snapshot-test-dataset.json";
    DatasetRequestModel datasetRequest =
        jsonLoader.loadObject(resourcePath, DatasetRequestModel.class);
    datasetRequest
        .name(Names.randomizeName(datasetRequest.getName()))
        .defaultProfileId(billingProfile.getId());
    summaryModel = connectedOperations.createDataset(datasetRequest);
    logger.info("--------begin test---------");
  }

  @After
  public void tearDown() throws Exception {
    logger.info("--------start of tear down---------");

    configService.reset();
    connectedOperations.teardown();
  }

  @Test
  public void testTableAdditions() throws Exception {}
}
