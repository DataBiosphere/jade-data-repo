package bio.terra.service.filedata.azure;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.iam.IamProviderInterface;
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
import org.springframework.boot.test.mock.mockito.MockBean;
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
  private BillingProfileModel billingProfile;
  private DatasetSummaryModel datasetSummary;

  @Autowired AzureSynapsePdao azureSynapsePdao;
  @Autowired ConnectedOperations connectedOperations;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired DatasetService datasetService;
  @MockBean private IamProviderInterface samService;

  @Before
  public void setup() throws Exception {
    randomFlightId = ShortUUID.get();
    connectedOperations.stubOutSamCalls(samService);
    billingProfile =
        connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());
    //TODO - we don't really need a full dataset
    // Replace this with just an entry in the db w/ the table schema
    datasetSummary = connectedOperations.createDataset(
        billingProfile, "azure-simple-dataset.json");
  }

  @After
  public void cleanup() throws Exception {
    azureSynapsePdao.cleanSynapseEntries(randomFlightId);

    //TODO - Clean out test parquet files

    connectedOperations.teardown();
  }

  @Test
  public void testSynapseQuery() throws Exception {
    // ----- ingest Input parameters----
    // TODO - Once we add ability to sign urls, remove the signature from this test url
    String ingestFileLocation =
        "https://tdrsynapse1.blob.core.windows.net/shelbycontainerexample"
            + "?sp=racwdlmeop&st=2021-07-27T11:13:48Z&se=2021-07-27T19:13:48Z"
            + "&spr=https&sv=2020-08-04&sr=c&sig=i5f%2FlB2snH0CjgDfYjNzLTKSq1tkAniBszOlyiXT3t0%3D";
    String ingestFileName = "azure-simple-dataset-ingest-request.csv";
    String destinationTableName = "participant";
    String destinationParquetFile = "parquet/" + randomFlightId + ".parquet";

    // ---- ingest steps ---
    // 1 - Create external data source for the ingest control file
    azureSynapsePdao.createExternalDataSource(ingestFileLocation, randomFlightId);

    // TODO - Add basic check to make sure the data source is created succesfully
    // Maybe a basic query?

    // 2 - Retrieve info about database schema so that we can populate the parquet create query

    DatasetTable destinationTable =
        datasetService
            .retrieve(datasetSummary.getId())
            .getTableByName(destinationTableName)
            .orElseThrow(
                () -> new InvalidParameterException("Destination table project not valid"));

    // 3 - Create parquet files via external table
    azureSynapsePdao.createParquetFiles(
        destinationTable, ingestFileName, destinationParquetFile, randomFlightId);

    //TODO - Add check that the parquet files were successfully created.
    // How do we query the parquet files?

    // 4 - clean out synapse
      // we'll do this in the test cleanup method, but it will be a step in the normal flight
      //azureSynapsePdao.cleanSynapseEntries(randomFlightId);

  }
}
