package bio.terra.service.filedata.azure.tables;

import static org.hamcrest.MatcherAssert.assertThat;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.AzureUtils;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.Names;
import bio.terra.service.auth.iam.IamProviderInterface;
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
@EmbeddedDatabaseTest
public class TableServiceClientUtilsTest {
  private static final Logger logger = LoggerFactory.getLogger(TableServiceClientUtilsTest.class);
  private TableServiceClient tableServiceClient;
  private String tableName;

  @Autowired ConnectedOperations connectedOperations;
  @Autowired private ConnectedTestConfiguration testConfig;
  @MockBean private IamProviderInterface samService;
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
    assertThat("table should not exist", !tableExists);

    tableServiceClient.createTableIfNotExists(tableName);
    boolean tableExistsNow = TableServiceClientUtils.tableExists(tableServiceClient, tableName);
    assertThat("table should exist", tableExistsNow);

    boolean tableNoEntries = TableServiceClientUtils.tableHasEntries(tableServiceClient, tableName);
    assertThat("table should have no entries", !tableNoEntries);

    boolean tableZeroEntry =
        TableServiceClientUtils.tableHasSingleEntry(tableServiceClient, tableName, null);
    assertThat("table should have zero entries", !tableZeroEntry);

    // add an entry to the table
    tableClient.createEntity(new TableEntity("test1", UUID.randomUUID().toString()));
    boolean tableHasEntries =
        TableServiceClientUtils.tableHasEntries(tableServiceClient, tableName);
    assertThat("table should have one entry", tableHasEntries);

    boolean tableOneEntry =
        TableServiceClientUtils.tableHasSingleEntry(tableServiceClient, tableName, null);
    assertThat("table should have one entry", tableOneEntry);

    // add a second entry to the table
    tableClient.createEntity(new TableEntity("test2", UUID.randomUUID().toString()));
    boolean tableTwoEntry =
        TableServiceClientUtils.tableHasSingleEntry(tableServiceClient, tableName, null);
    assertThat("table should have two entries", !tableTwoEntry);

    tableClient.deleteTable();
    boolean tableExistsAfterDelete =
        TableServiceClientUtils.tableExists(tableServiceClient, tableName);
    assertThat("table should not exist after delete", !tableExistsAfterDelete);
  }
}
