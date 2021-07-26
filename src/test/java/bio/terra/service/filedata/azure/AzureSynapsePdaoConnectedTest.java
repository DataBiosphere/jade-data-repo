package bio.terra.service.filedata.azure;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.stairway.ShortUUID;
import java.security.InvalidParameterException;
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
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class AzureSynapsePdaoConnectedTest {
  private static Logger logger = LoggerFactory.getLogger(AzureSynapsePdaoConnectedTest.class);
  private String randomFlightId;
  @Autowired AzureSynapsePdao azureSynapsePdao;
  @Autowired ConnectedOperations connectedOperations;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired DatasetService datasetService;

  @Before
  public void setup() {
    randomFlightId = ShortUUID.get();
  }

  @After
  public void cleanup() {
    azureSynapsePdao.cleanSynapseEntries(randomFlightId);
  }

  @Test
  public void testSynapseQuery() throws Exception {
    // ----- ingest Input parameters----
    String ingestFileLocation =
        "https://tdrsynapse1.blob.core.windows.net/shelbycontainerexample"
            + "?sp=racwdlmeop&st=2021-07-26T14:42:54Z&se=2021-07-26T22:42:54Z"
            + "&spr=https&sv=2020-08-04&sr=c&sig=WI9KnwY0dVCaoMQ1dpwWAZ1L%2FoT1v0GOu3bAPksUsDA%3D";
    String ingestFileName = "azure-simple-dataset-ingest-request.csv";
    String destinationTableName = "participant";
    String destinationParquetFile = "parquet/flightId.parquet";

    // ---- ingest steps ---
    // 1 - Create external data source for the ingest control file
    azureSynapsePdao.createExternalDataSource(ingestFileLocation, randomFlightId);

    // 2 - Create a test dataset so that we have a schema to build the query with

    BillingProfileModel profileModel =
        connectedOperations.createProfileForAccount(
            testConfig.getGoogleBillingAccountId(), CloudPlatform.AZURE);
    DatasetSummaryModel datasetSummary =
        connectedOperations.createDataset(profileModel, "azure-simple-dataset.json");

    DatasetTable destinationTable =
        datasetService
            .retrieve(datasetSummary.getId())
            .getTableByName(destinationTableName)
            .orElseThrow(
                () -> new InvalidParameterException("Destination table project not valid"));

    // 3 - Create parquet files via external table
    azureSynapsePdao.createParquetFiles(
        destinationTable, ingestFileName, destinationParquetFile, randomFlightId);
  }
}
