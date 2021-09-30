package bio.terra.service.filedata.azure.tables;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.AzureUtils;
import bio.terra.common.SynapseUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.Names;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.FileService;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import com.azure.data.tables.models.TableEntity;
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
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class TableServiceClientUtilsTest {
  private static final Logger logger = LoggerFactory.getLogger(TableServiceClientUtilsTest.class);
  private TableServiceClient tableServiceClient;
  private String tableName;

  @Autowired AzureSynapsePdao azureSynapsePdao;
  @Autowired ConnectedOperations connectedOperations;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired DatasetService datasetService;
  @MockBean private IamProviderInterface samService;
  @Autowired SynapseUtils synapseUtils;
  @Autowired AzureAuthService azureAuthService;
  @Autowired TableDirectoryDao tableDirectoryDao;
  @Autowired TableDao tableDao;
  @Autowired AzureBlobStorePdao azureBlobStorePdao;
  @Autowired FileService fileService;
  @Autowired AzureUtils azureUtils;

  @Before
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    tableServiceClient =
        new TableServiceClientBuilder()
            .credential(
                new AzureNamedKeyCredential(
                    testConfig.getSourceStorageAccountName(),
                    azureUtils.getSourceStorageAccountPrimarySharedKey()))
            .endpoint(
                "https://" + testConfig.getSourceStorageAccountName() + ".table.core.windows.net")
            .buildClient();
  }

  @After
  public void cleanup() throws Exception {
    if (tableName != null) {
      try {
        TableClient tableClient = tableServiceClient.getTableClient(tableName);
        tableClient.deleteTable();
      } catch (Exception ex) {
        logger.error("Unable to delete table {}", tableName, ex);
      }
    }

    connectedOperations.teardown();
  }

  @Test
  public void testUtils() {
    tableName = Names.randomizeName("testTable123").replaceAll("_", "");
    TableClient tableClient = tableServiceClient.getTableClient(tableName);

    boolean tableExists = TableServiceClientUtils.tableExists(tableServiceClient, tableName);
    assertThat("table should not exist", tableExists, equalTo(false));

    tableServiceClient.createTableIfNotExists(tableName);
    boolean tableExistsNow = TableServiceClientUtils.tableExists(tableServiceClient, tableName);
    assertThat("table should exist", tableExistsNow, equalTo(true));

    boolean tableNoEntries = TableServiceClientUtils.tableHasEntries(tableServiceClient, tableName);
    assertThat("table should have no entries", tableNoEntries, equalTo(false));

    int tableZeroEntryCount =
        TableServiceClientUtils.getTableEntryCount(tableServiceClient, tableName, null);
    assertThat("table should have zero entries", tableZeroEntryCount, equalTo(0));

    // add an entry to the table
    tableClient.createEntity(new TableEntity("test1", UUID.randomUUID().toString()));
    boolean tableHasEntries =
        TableServiceClientUtils.tableHasEntries(tableServiceClient, tableName);
    assertThat("table should have one entry", tableHasEntries, equalTo(true));

    int tableOneEntryCount =
        TableServiceClientUtils.getTableEntryCount(tableServiceClient, tableName, null);
    assertThat("table should have one entry", tableOneEntryCount, equalTo(1));

    // add a second entry to the table
    tableClient.createEntity(new TableEntity("test2", UUID.randomUUID().toString()));
    int tableTwoEntryCount =
        TableServiceClientUtils.getTableEntryCount(tableServiceClient, tableName, null);
    assertThat("table should have two entries", tableTwoEntryCount, equalTo(2));

    tableClient.deleteTable();
    boolean tableExistsAfterDelete =
        TableServiceClientUtils.tableExists(tableServiceClient, tableName);
    assertThat("table should not exist after delete", tableExistsAfterDelete, equalTo(false));
  }
}
