package bio.terra.service.dataset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.Column;
import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.AccessInfoModel;
import bio.terra.model.AssetModel;
import bio.terra.model.AssetTableModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestAccessIncludeModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSpecificationModel;
import bio.terra.model.TableDataType;
import bio.terra.model.TableModel;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class DatasetJsonConversionTest {
  private AuthenticatedUserRequest testUser =
      AuthenticatedUserRequest.builder()
          .setSubjectId("DatasetUnit")
          .setEmail("dataset@unit.com")
          .setToken("token")
          .build();

  private static final UUID DATASET_PROFILE_ID = UUID.randomUUID();
  private static final String DATASET_NAME = "dataset_name";
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final Instant DATASET_CREATION_DATE = Instant.now();
  private static final String DATASET_DESCRIPTION = "dataset description";
  private static final String DATASET_DATA_PROJECT = "dataset_name_data";
  private static final String DATASET_TABLE_NAME = "table_a";
  private static final UUID DATASET_TABLE_ID = UUID.randomUUID();
  private static final String DATASET_COLUMN_NAME = "column_a";
  private static final TableDataType DATASET_COLUMN_TYPE = TableDataType.STRING;
  private static final UUID DATASET_COLUMN_ID = UUID.randomUUID();
  private static final String DATASET_ASSET_NAME = "asset1";
  private static final UUID DATASET_ASSET_ID = UUID.randomUUID();

  private Dataset dataset;
  private DatasetModel datasetModel;
  private MetadataDataAccessUtils metadataDataAccessUtils;

  @Before
  public void setUp() throws Exception {
    Column datasetColumn =
        new Column()
            .id(DATASET_COLUMN_ID)
            .name(DATASET_COLUMN_NAME)
            .arrayOf(false)
            .type(DATASET_COLUMN_TYPE)
            .required(true);
    DatasetTable datasetTable =
        new DatasetTable()
            .id(DATASET_TABLE_ID)
            .name(DATASET_TABLE_NAME)
            .rawTableName(DATASET_TABLE_NAME)
            .columns(List.of(datasetColumn))
            .primaryKey(
                List.of(
                    new Column()
                        .id(DATASET_COLUMN_ID)
                        .name(DATASET_COLUMN_NAME)
                        .type(DATASET_COLUMN_TYPE)
                        .required(true)))
            .bigQueryPartitionConfig(BigQueryPartitionConfigV1.none());

    AssetColumn assetColumn =
        new AssetColumn()
            .id(DATASET_COLUMN_ID)
            .datasetTable(datasetTable)
            .datasetColumn(datasetColumn);

    AssetTable assetTable =
        new AssetTable().datasetTable(datasetTable).columns(List.of(assetColumn));

    dataset =
        new Dataset()
            .id(DATASET_ID)
            .createdDate(DATASET_CREATION_DATE)
            .name(DATASET_NAME)
            .description(DATASET_DESCRIPTION)
            .tables(List.of(datasetTable))
            .assetSpecifications(
                List.of(
                    new AssetSpecification()
                        .id(DATASET_ASSET_ID)
                        .name(DATASET_ASSET_NAME)
                        .assetTables(List.of(assetTable))
                        .rootTable(assetTable)
                        .rootColumn(assetColumn)
                        .assetRelationships(Collections.emptyList())))
            .defaultProfileId(DATASET_PROFILE_ID)
            .projectResource(new GoogleProjectResource().googleProjectId(DATASET_DATA_PROJECT));

    dataset
        .getDatasetSummary()
        .storage(
            List.of(
                new GoogleStorageResource(
                    UUID.randomUUID(), GoogleCloudResource.BUCKET, GoogleRegion.US_CENTRAL1)));

    datasetModel =
        new DatasetModel()
            .name(DATASET_NAME)
            .id(DATASET_ID)
            .description(DATASET_DESCRIPTION)
            .createdDate(DATASET_CREATION_DATE.toString())
            .defaultProfileId(DATASET_PROFILE_ID)
            .schema(
                new DatasetSpecificationModel()
                    .addTablesItem(
                        new TableModel()
                            .name(DATASET_TABLE_NAME)
                            .addPrimaryKeyItem(DATASET_COLUMN_NAME)
                            .addColumnsItem(
                                new ColumnModel()
                                    .name(DATASET_COLUMN_NAME)
                                    .datatype(DATASET_COLUMN_TYPE)
                                    .arrayOf(false)
                                    .required(true)))
                    .relationships(Collections.emptyList())
                    .assets(
                        List.of(
                            new AssetModel()
                                .name(DATASET_ASSET_NAME)
                                .addTablesItem(
                                    new AssetTableModel()
                                        .name(DATASET_TABLE_NAME)
                                        .addColumnsItem(DATASET_COLUMN_NAME))
                                .rootTable(DATASET_TABLE_NAME)
                                .rootColumn(DATASET_COLUMN_NAME)
                                .follow(Collections.emptyList()))))
            .dataProject(DATASET_DATA_PROJECT);

    metadataDataAccessUtils = new MetadataDataAccessUtils(null, null, null);
  }

  @Test
  public void populateDatasetModelFromDataset() {
    assertThat(
        DatasetJsonConversion.populateDatasetModelFromDataset(
            dataset,
            List.of(
                DatasetRequestAccessIncludeModel.SCHEMA,
                DatasetRequestAccessIncludeModel.PROFILE,
                DatasetRequestAccessIncludeModel.DATA_PROJECT),
            metadataDataAccessUtils,
            testUser),
        equalTo(datasetModel));
  }

  @Test
  public void populateDatasetModelFromDatasetNone() throws IOException {
    assertThat(
        DatasetJsonConversion.populateDatasetModelFromDataset(
            dataset,
            List.of(
                DatasetRequestAccessIncludeModel.NONE, DatasetRequestAccessIncludeModel.PROFILE),
            metadataDataAccessUtils,
            testUser),
        equalTo(datasetModel.dataProject(null).defaultProfileId(null).schema(null)));
  }

  @Test
  public void populateDatasetModelFromDatasetAccessInfo() {
    String expectedDatasetName = "datarepo_" + DATASET_NAME;
    assertThat(
        DatasetJsonConversion.populateDatasetModelFromDataset(
            dataset,
            List.of(DatasetRequestAccessIncludeModel.ACCESS_INFORMATION),
            metadataDataAccessUtils,
            testUser),
        equalTo(
            datasetModel
                .dataProject(null)
                .defaultProfileId(null)
                .schema(null)
                .accessInformation(
                    new AccessInfoModel()
                        .bigQuery(
                            new bio.terra.model.AccessInfoBigQueryModel()
                                .datasetName(expectedDatasetName)
                                .projectId(DATASET_DATA_PROJECT)
                                .datasetId(DATASET_DATA_PROJECT + ":" + expectedDatasetName)
                                .link(
                                    "https://console.cloud.google.com/bigquery?project="
                                        + DATASET_DATA_PROJECT
                                        + "&ws=!"
                                        + expectedDatasetName
                                        + "&d="
                                        + expectedDatasetName
                                        + "&p="
                                        + DATASET_DATA_PROJECT
                                        + "&page=dataset")
                                .tables(
                                    List.of(
                                        new bio.terra.model.AccessInfoBigQueryModelTable()
                                            .name(DATASET_TABLE_NAME)
                                            .qualifiedName(
                                                DATASET_DATA_PROJECT
                                                    + "."
                                                    + expectedDatasetName
                                                    + "."
                                                    + DATASET_TABLE_NAME)
                                            .link(
                                                "https://console.cloud.google.com/bigquery?project="
                                                    + DATASET_DATA_PROJECT
                                                    + "&ws=!"
                                                    + expectedDatasetName
                                                    + "&d="
                                                    + expectedDatasetName
                                                    + "&p="
                                                    + DATASET_DATA_PROJECT
                                                    + "&page=table&t="
                                                    + DATASET_TABLE_NAME)
                                            .id(
                                                DATASET_DATA_PROJECT
                                                    + ":"
                                                    + expectedDatasetName
                                                    + "."
                                                    + DATASET_TABLE_NAME)
                                            .sampleQuery(
                                                "SELECT * FROM `"
                                                    + DATASET_DATA_PROJECT
                                                    + "."
                                                    + expectedDatasetName
                                                    + "."
                                                    + DATASET_TABLE_NAME
                                                    + "` LIMIT 1000")))))));
  }

  @Test
  public void testRequiredColumns() throws Exception {
    var defaultColumnName = "NOT_PRIMARY_KEY_COLUMN";
    var requiredColumnName = "REQUIRED_COLUMN";
    DatasetRequestModel datasetRequestModel =
        new DatasetRequestModel()
            .name(DATASET_NAME)
            .defaultProfileId(DATASET_PROFILE_ID)
            .description(DATASET_DESCRIPTION)
            .cloudPlatform(CloudPlatform.GCP)
            .region(GoogleRegion.DEFAULT_GOOGLE_REGION.name())
            .schema(
                new DatasetSpecificationModel()
                    .tables(
                        List.of(
                            new TableModel()
                                .name(DATASET_TABLE_NAME)
                                .primaryKey(List.of(DATASET_COLUMN_NAME))
                                .columns(
                                    List.of(
                                        new ColumnModel()
                                            .name(DATASET_COLUMN_NAME)
                                            .datatype(DATASET_COLUMN_TYPE)
                                            .arrayOf(false),
                                        new ColumnModel()
                                            .name(defaultColumnName)
                                            .datatype(DATASET_COLUMN_TYPE)
                                            .arrayOf(false),
                                        new ColumnModel()
                                            .name(requiredColumnName)
                                            .datatype(DATASET_COLUMN_TYPE)
                                            .required(true)
                                            .arrayOf(false))))));

    var dataset = DatasetJsonConversion.datasetRequestToDataset(datasetRequestModel);
    var columnMap = dataset.getTables().get(0).getColumnsMap();
    var pkColumn = columnMap.get(DATASET_COLUMN_NAME);
    var defaultColumn = columnMap.get(defaultColumnName);
    var requiredColumn = columnMap.get(requiredColumnName);

    assertThat("Primary key columns are marked as required", pkColumn.isRequired(), is(true));
    assertThat("Regular columns default to not required", defaultColumn.isRequired(), is(false));
    assertThat("Required columns are marked as required", requiredColumn.isRequired(), is(true));
  }
}
