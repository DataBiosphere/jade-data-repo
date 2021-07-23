package bio.terra.service.filedata.azure;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.category.Connected;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.model.CloudPlatform;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
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
import java.security.InvalidParameterException;
import java.util.Optional;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class AzureSynapsePdaoConnectedTest {
  private static Logger logger = LoggerFactory.getLogger(AzureSynapsePdaoConnectedTest.class);
  @Autowired AzureSynapsePdao azureSynapsePdao;
  @Autowired ConnectedOperations connectedOperations;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired DatasetService datasetService;

  @Test
  public void testSynapseQuery() throws Exception {
    // ----- ingest Input parameters----
    String ingestFileLocation =
        "https://tdrsynapse1.blob.core.windows.net/shelbycontainerexample"
            + "?sp=racwdlmeop&st=2021-07-23T20:04:26Z&se=2021-07-24T04:04:26Z&spr=https"
            + "&sv=2020-08-04&sr=c&sig=NNvOV5LuCUhy0KgZrSn13mp99QFqHYa9Hr4tAeuoE6I%3D";
    String ingestFileName = "example.csv";
    String destinationTableName = "participant";

        // ---- ingest steps ---
    // 1 - Create external data source for the ingest control file
    azureSynapsePdao.createExternalDataSource(ingestFileLocation);

    // 2 - Create a test dataset so that we have a schema to build the query with

    BillingProfileModel profileModel =
        connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId(),
            CloudPlatform.AZURE);
    DatasetSummaryModel datasetSummary =
        connectedOperations.createDataset(profileModel, "azure-simple-dataset.json");

    DatasetTable destinationTable = datasetService.retrieve(datasetSummary.getId())
        .getTableByName(destinationTableName).orElseThrow(() ->
            new InvalidParameterException("Destination table project not valid"));

    // 3 - Create parquet files via external table
    azureSynapsePdao.createParquetFiles(destinationTable,
        ingestFileLocation,
        ingestFileName);

  }
}
