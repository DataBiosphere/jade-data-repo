package bio.terra.service.filedata.azure;

import static bio.terra.service.filedata.azure.util.BlobContainerClientFactory.SasPermission;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestRequestModel.FormatEnum;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.stairway.ShortUUID;
import java.security.InvalidParameterException;
import java.util.Arrays;
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
public class AzureSynapsePdaoConnectedTest {
  private static Logger logger = LoggerFactory.getLogger(AzureSynapsePdaoConnectedTest.class);
  private String randomFlightId;
  private BillingProfileModel billingProfile;
  private DatasetSummaryModel datasetSummary;

  private static final String CONTROL_FILE_SCOPED_CREDENTIAL_PREFIX = "cfsas_";
  private static final String DESTINATION_SCOPED_CREDENTIAL_PREFIX = "dsas_";
  private static final String CONTROL_FILE_DATA_SOURCE_PREFIX = "cfds_";
  private static final String DESTINATION_DATA_SOURCE_PREFIX = "dds_";
  private static final String TABLE_NAME_PREFIX = "ingest_";

  private String controlFileScopedCredentialName;
  private String destinationScopedCredentialName;
  private String controlFileDataSourceName;
  private String destinationDataSourceName;
  private String tableName;

  @Autowired AzureSynapsePdao azureSynapsePdao;
  @Autowired ConnectedOperations connectedOperations;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired DatasetService datasetService;
  @MockBean private IamProviderInterface samService;

  @Before
  public void setup() throws Exception {
    randomFlightId = ShortUUID.get();
    controlFileScopedCredentialName = CONTROL_FILE_SCOPED_CREDENTIAL_PREFIX + randomFlightId;
    destinationScopedCredentialName = DESTINATION_SCOPED_CREDENTIAL_PREFIX + randomFlightId;
    controlFileDataSourceName = CONTROL_FILE_DATA_SOURCE_PREFIX + randomFlightId;
    destinationDataSourceName = DESTINATION_DATA_SOURCE_PREFIX + randomFlightId;
    tableName = TABLE_NAME_PREFIX + randomFlightId;

    connectedOperations.stubOutSamCalls(samService);
    billingProfile =
        connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());
    // TODO - we don't really need a full dataset
    // Replace this with just an entry in the db w/ the table schema
    datasetSummary = connectedOperations.createDataset(billingProfile, "azure-simple-dataset.json");
  }

  @After
  public void cleanup() throws Exception {
    azureSynapsePdao.cleanSynapseEntries(
        Arrays.asList(tableName),
        Arrays.asList(destinationDataSourceName, controlFileDataSourceName),
        Arrays.asList(destinationScopedCredentialName, controlFileScopedCredentialName));

    // TODO - Clean out test parquet files

    connectedOperations.teardown();
  }

  @Test
  public void testSynapseQueryCSV() throws Exception {
    IngestRequestModel ingestRequestModel =
        new IngestRequestModel().format(FormatEnum.CSV).csvSkipLeadingRows(2);
    String ingestFileName = "azure-simple-dataset-ingest-request.csv";
    testSynapseQuery(ingestRequestModel, ingestFileName);
  }

  @Test
  public void testSynapseQueryJSON() throws Exception {
    IngestRequestModel ingestRequestModel = new IngestRequestModel().format(FormatEnum.JSON);
    String ingestFileName = "azure-simple-dataset-ingest-request.json";
    testSynapseQuery(ingestRequestModel, ingestFileName);
  }

  private void testSynapseQuery(IngestRequestModel ingestRequestModel, String ingestFileName)
      throws Exception {
    // Currently, the parquet files will live in the same location as the ingest control file
    String destinationTableName = "participant";
    String destinationParquetFile = "parquet/" + randomFlightId + ".parquet";

    UUID tenantId = testConfig.getTargetTenantId();

    // ---- ingest steps ---
    // 1 - Create external data source for the ingest control file
    String ingestFileLocation = "https://tdrconnectedsrc1.blob.core.windows.net/synapsetestdata";

    azureSynapsePdao.createExternalDataSource(
        ingestFileLocation,
        tenantId,
        controlFileScopedCredentialName,
        controlFileDataSourceName,
        SasPermission.READ_ONLY);

    // 2 - Create the external data source for the destination
    // where we'll write the resulting parquet files
    String parquetDestinationLocation =
        "https://tdrconnectedsrc1.blob.core.windows.net/synapsetestdata";
    azureSynapsePdao.createExternalDataSource(
        parquetDestinationLocation,
        tenantId,
        destinationScopedCredentialName,
        destinationDataSourceName,
        SasPermission.WRITE_PARQUET);

    // TODO - Add basic check to make sure the data source is created successfully
    // Maybe a basic query?

    // 3 - Retrieve info about database schema so that we can populate the parquet create query

    DatasetTable destinationTable =
        datasetService
            .retrieve(datasetSummary.getId())
            .getTableByName(destinationTableName)
            .orElseThrow(
                () -> new InvalidParameterException("Destination table project not valid"));

    // 4 - Create parquet files via external table
    azureSynapsePdao.createParquetFiles(
        ingestRequestModel.getFormat(),
        destinationTable,
        ingestFileName,
        destinationParquetFile,
        destinationDataSourceName,
        controlFileDataSourceName,
        tableName,
        ingestRequestModel.getCsvSkipLeadingRows());

    // TODO - Add check that the parquet files were successfully created.
    // How do we query the parquet files?

    // 4 - clean out synapse
    // we'll do this in the test cleanup method, but it will be a step in the normal flight
    // azureSynapsePdao.cleanSynapseEntries(randomFlightId);

  }
}
