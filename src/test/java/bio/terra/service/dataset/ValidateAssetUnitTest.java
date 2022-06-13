package bio.terra.service.dataset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

import bio.terra.app.usermetrics.UserLoggingMetrics;
import bio.terra.common.Relationship;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.DatasetFixtures;
import bio.terra.model.AssetModel;
import bio.terra.model.TableDataType;
import bio.terra.service.dataset.exception.InvalidAssetException;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobService;
import bio.terra.service.load.LoadService;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.tabulardata.azure.StorageTableService;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryTransactionPdao;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {DatasetService.class})
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class ValidateAssetUnitTest {
  @MockBean private DatasetDao datasetDao;

  @Autowired private DatasetService datasetService;

  @MockBean private JobService jobService;
  @MockBean private LoadService loadService;
  @MockBean private ProfileDao profileDao;
  @MockBean private StorageTableService storageTableService;
  @MockBean private BigQueryTransactionPdao bigQueryTransactionPdao;
  @MockBean private BigQueryDatasetPdao bigQueryDatasetPdao;
  @MockBean private MetadataDataAccessUtils metadataDataAccessUtils;
  @MockBean private ResourceService resourceService;
  @MockBean private GcsPdao gcsPdao;
  @MockBean private ObjectMapper objectMapper;
  @MockBean private AzureBlobStorePdao azureBlobStorePdao;
  @MockBean private ProfileService profileService;
  @MockBean private UserLoggingMetrics loggingMetrics;

  private String tableName = "tableName1";
  private String tableName2 = "tableName2";
  private String col1Name = "col1";
  private String col2Name = "col2";
  private String col3Name = "col3";
  private String relationshipName = "rel1";
  private Dataset dataset;
  private AssetModel assetModel;

  @Before
  public void setup() {
    dataset =
        new Dataset()
            .tables(
                Arrays.asList(
                    DatasetFixtures.generateDatasetTable(
                        tableName, TableDataType.STRING, List.of(col1Name, col2Name)),
                    DatasetFixtures.generateDatasetTable(
                        tableName2, TableDataType.STRING, List.of(col3Name))))
            .relationships(Arrays.asList(new Relationship().name(relationshipName)));

    assetModel =
        new AssetModel()
            .name("assetName")
            .rootTable(tableName)
            .rootColumn(col1Name)
            .tables(
                Arrays.asList(
                    DatasetFixtures.generateAssetTable(tableName, List.of(col1Name, col2Name))))
            .follow(Arrays.asList(relationshipName));
  }

  @Test
  public void validateAssetModel() {
    datasetService.validateDatasetAssetSpecification(dataset, assetModel);
  }

  @Test
  public void testTwoTables() {
    assetModel.tables(
        Arrays.asList(
            DatasetFixtures.generateAssetTable(tableName, List.of(col1Name, col2Name)),
            DatasetFixtures.generateAssetTable(tableName2, List.of(col3Name))));
    datasetService.validateDatasetAssetSpecification(dataset, assetModel);
  }

  @Test(expected = InvalidAssetException.class)
  public void testTwoTablesInvalidOverlap() {
    // col3 is only in table2, not table2
    assetModel.tables(
        Arrays.asList(
            DatasetFixtures.generateAssetTable(tableName, List.of(col1Name, col3Name)),
            DatasetFixtures.generateAssetTable(tableName2, List.of(col3Name))));
    testAssetModel(
        "invalid column", "Column " + col3Name + " does not exist in table " + tableName);
  }

  @Test(expected = InvalidAssetException.class)
  public void testInvalidColumn() {
    String invalidColumn = "InvalidCol";
    assetModel.tables(
        Arrays.asList(
            DatasetFixtures.generateAssetTable(tableName, List.of(col1Name, invalidColumn))));
    testAssetModel(
        "invalid column", "Column " + invalidColumn + " does not exist in table " + tableName);
  }

  @Test(expected = InvalidAssetException.class)
  public void testInvalidTable() {
    String invalidTable = "invalidTable";
    assetModel.tables(
        Arrays.asList(
            DatasetFixtures.generateAssetTable(invalidTable, List.of(col1Name, col2Name))));
    testAssetModel("invalid table", "Table " + invalidTable + " does not exist in dataset.");
  }

  @Test(expected = InvalidAssetException.class)
  public void testInvalidRootTable() {
    String invalidRootTable = "InvalidRootTable";
    assetModel.rootTable(invalidRootTable);
    testAssetModel(
        "invalid root table", "Root table " + invalidRootTable + " does not exist in dataset.");
  }

  @Test(expected = InvalidAssetException.class)
  public void testInvalidRootColumn() {
    String invalidRootColumn = "InvalidRootColumn";
    assetModel.rootColumn(invalidRootColumn);
    testAssetModel(
        "invalid root column",
        "Root column " + invalidRootColumn + " does not exist in table " + tableName);
  }

  @Test(expected = InvalidAssetException.class)
  public void testInvalidFollowRelationship() {
    String invalidFollowRelationship = "InvalidRelationship";
    assetModel.follow(List.of(relationshipName, invalidFollowRelationship));
    testAssetModel(
        "invalid relationship",
        "Relationship specified in follow list '"
            + invalidFollowRelationship
            + "' does not exist in dataset's list of relationships");
  }

  private void testAssetModel(String itemChecked, String expectedErrorMessage) {
    try {
      datasetService.validateDatasetAssetSpecification(dataset, assetModel);
    } catch (InvalidAssetException ex) {
      assertThat(
          "Validation of asset model should fail on " + itemChecked,
          ex.getMessage(),
          containsString(expectedErrorMessage));
      throw ex;
    }
  }
}
