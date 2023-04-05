package bio.terra.service.dataset;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.Column;
import bio.terra.common.PdaoConstant;
import bio.terra.common.Relationship;
import bio.terra.common.Table;
import bio.terra.common.TagUtils;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.AssetModel;
import bio.terra.model.AssetTableModel;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestAccessIncludeModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSpecificationModel;
import bio.terra.model.DatePartitionOptionsModel;
import bio.terra.model.IntPartitionOptionsModel;
import bio.terra.model.RelationshipModel;
import bio.terra.model.RelationshipTermModel;
import bio.terra.model.TableModel;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public final class DatasetJsonConversion {

  // only allow use of static methods
  private DatasetJsonConversion() {}

  public static Dataset datasetRequestToDataset(
      DatasetRequestModel datasetRequest, UUID datasetId) {
    Map<String, DatasetTable> tablesMap = new HashMap<>();
    Map<String, Relationship> relationshipsMap = new HashMap<>();
    List<AssetSpecification> assetSpecifications = new ArrayList<>();
    UUID defaultProfileId = datasetRequest.getDefaultProfileId();
    DatasetSpecificationModel datasetSpecification = datasetRequest.getSchema();
    datasetSpecification
        .getTables()
        .forEach(tableModel -> tablesMap.put(tableModel.getName(), tableModelToTable(tableModel)));
    // Relationships are optional, so there might not be any here
    if (datasetSpecification.getRelationships() != null) {
      datasetSpecification
          .getRelationships()
          .forEach(
              relationship ->
                  relationshipsMap.put(
                      relationship.getName(),
                      relationshipModelToDatasetRelationship(relationship, tablesMap)));
    }
    List<AssetModel> assets = datasetSpecification.getAssets();
    if (assets != null) {
      assets.forEach(
          asset ->
              assetSpecifications.add(
                  assetModelToAssetSpecification(asset, tablesMap, relationshipsMap)));
    }

    var cloudPlatform = CloudPlatformWrapper.of(datasetRequest.getCloudPlatform());

    final List<? extends StorageResource<?, ?>> storageResources =
        cloudPlatform.createStorageResourceValues(datasetRequest);

    boolean enableSecureMonitoring =
        Objects.requireNonNullElse(datasetRequest.isEnableSecureMonitoring(), false);

    return new Dataset(
            new DatasetSummary()
                .id(datasetId)
                .name(datasetRequest.getName())
                .description(datasetRequest.getDescription())
                .storage(storageResources)
                .defaultProfileId(defaultProfileId)
                .secureMonitoringEnabled(enableSecureMonitoring)
                .phsId(datasetRequest.getPhsId())
                .selfHosted(datasetRequest.isExperimentalSelfHosted())
                .properties(datasetRequest.getProperties())
                .predictableFileIds(datasetRequest.isExperimentalPredictableFileIds())
                .tags(TagUtils.getDistinctTags(datasetRequest.getTags())))
        .tables(new ArrayList<>(tablesMap.values()))
        .relationships(new ArrayList<>(relationshipsMap.values()))
        .assetSpecifications(assetSpecifications);
  }

  public static DatasetModel populateDatasetModelFromDataset(
      Dataset dataset,
      List<DatasetRequestAccessIncludeModel> include,
      MetadataDataAccessUtils metadataDataAccessUtils,
      AuthenticatedUserRequest userRequest) {
    DatasetModel datasetModel =
        new DatasetModel()
            .id(dataset.getId())
            .name(dataset.getName())
            .description(dataset.getDescription())
            .createdDate(dataset.getCreatedDate().toString())
            .secureMonitoringEnabled(dataset.isSecureMonitoringEnabled())
            .phsId(dataset.getPhsId())
            .selfHosted(dataset.isSelfHosted())
            .predictableFileIds(dataset.hasPredictableFileIds())
            .tags(dataset.getTags());

    if (include.contains(DatasetRequestAccessIncludeModel.NONE)) {
      return datasetModel;
    }

    if (include.contains(DatasetRequestAccessIncludeModel.PROFILE)) {
      datasetModel.defaultProfileId(dataset.getDefaultProfileId());
    }

    if (include.contains(DatasetRequestAccessIncludeModel.PROPERTIES)) {
      datasetModel.properties(dataset.getProperties());
    }

    if (include.contains(DatasetRequestAccessIncludeModel.SCHEMA)) {
      datasetModel.schema(datasetSpecificationModelFromDatasetSchema(dataset));
    }

    if (include.contains(DatasetRequestAccessIncludeModel.DATA_PROJECT)
        && dataset.getProjectResource() != null) {
      datasetModel.dataProject(dataset.getProjectResource().getGoogleProjectId());
      datasetModel.ingestServiceAccount(dataset.getProjectResource().getServiceAccount());
    }

    if (include.contains(DatasetRequestAccessIncludeModel.STORAGE)) {
      datasetModel.storage(dataset.getDatasetSummary().toStorageResourceModel());
    }

    if (include.contains(DatasetRequestAccessIncludeModel.ACCESS_INFORMATION)) {
      datasetModel.accessInformation(
          metadataDataAccessUtils.accessInfoFromDataset(dataset, userRequest));
    }

    return datasetModel;
  }

  public static DatasetSpecificationModel datasetSpecificationModelFromDatasetSchema(
      Dataset dataset) {
    return new DatasetSpecificationModel()
        .tables(
            dataset.getTables().stream()
                .map(table -> tableModelFromTable(table))
                .collect(Collectors.toList()))
        .relationships(
            dataset.getRelationships().stream()
                .map(rel -> relationshipModelFromDatasetRelationship(rel))
                .collect(Collectors.toList()))
        .assets(
            dataset.getAssetSpecifications().stream()
                .map(spec -> assetModelFromAssetSpecification(spec))
                .collect(Collectors.toList()));
  }

  public static DatasetTable tableModelToTable(TableModel tableModel) {
    Map<String, Column> columnMap = new HashMap<>();
    List<Column> columns = new ArrayList<>();
    DatasetTable datasetTable = new DatasetTable().name(tableModel.getName());
    List<String> primaryKeys =
        Optional.ofNullable(tableModel.getPrimaryKey()).orElse(Collections.emptyList());

    for (ColumnModel columnModel : tableModel.getColumns()) {
      Column column = columnModelToDatasetColumn(columnModel, primaryKeys).table(datasetTable);
      columnMap.put(column.getName(), column);
      columns.add(column);
    }

    List<Column> primaryKeyColumns =
        primaryKeys.stream().map(columnMap::get).collect(Collectors.toList());
    datasetTable.primaryKey(primaryKeyColumns);

    BigQueryPartitionConfigV1 partitionConfig;
    switch (tableModel.getPartitionMode()) {
      case DATE:
        String column = tableModel.getDatePartitionOptions().getColumn();
        boolean useIngestDate = column.equals(PdaoConstant.PDAO_INGEST_DATE_COLUMN_ALIAS);
        partitionConfig =
            useIngestDate
                ? BigQueryPartitionConfigV1.ingestDate()
                : BigQueryPartitionConfigV1.date(column);
        break;
      case INT:
        IntPartitionOptionsModel options = tableModel.getIntPartitionOptions();
        partitionConfig =
            BigQueryPartitionConfigV1.intRange(
                options.getColumn(), options.getMin(), options.getMax(), options.getInterval());
        break;
      default:
        partitionConfig = BigQueryPartitionConfigV1.none();
        break;
    }

    return datasetTable.bigQueryPartitionConfig(partitionConfig).columns(columns);
  }

  public static TableModel tableModelFromTable(DatasetTable datasetTable) {
    BigQueryPartitionConfigV1 config = datasetTable.getBigQueryPartitionConfig();
    TableModel.PartitionModeEnum partitionMode;
    DatePartitionOptionsModel dateOptions = null;
    IntPartitionOptionsModel intOptions = null;
    switch (config.getMode()) {
      case INGEST_DATE:
        partitionMode = TableModel.PartitionModeEnum.DATE;
        dateOptions =
            new DatePartitionOptionsModel().column(PdaoConstant.PDAO_INGEST_DATE_COLUMN_ALIAS);
        break;
      case DATE:
        partitionMode = TableModel.PartitionModeEnum.DATE;
        dateOptions = new DatePartitionOptionsModel().column(config.getColumnName());
        break;
      case INT_RANGE:
        partitionMode = TableModel.PartitionModeEnum.INT;
        intOptions =
            new IntPartitionOptionsModel()
                .column(config.getColumnName())
                .min(config.getIntMin())
                .max(config.getIntMax())
                .interval(config.getIntInterval());
        break;
      default:
        partitionMode = TableModel.PartitionModeEnum.NONE;
        break;
    }

    return new TableModel()
        .name(datasetTable.getName())
        .primaryKey(
            datasetTable.getPrimaryKey().stream().map(Column::getName).collect(Collectors.toList()))
        .partitionMode(partitionMode)
        .datePartitionOptions(dateOptions)
        .intPartitionOptions(intOptions)
        .columns(
            datasetTable.getColumns().stream()
                .map(Column::toColumnModel)
                .collect(Collectors.toList()));
  }

  public static Column columnModelToDatasetColumn(
      ColumnModel columnModel, List<String> primaryKeys) {
    boolean required =
        primaryKeys.contains(columnModel.getName())
            || Boolean.TRUE.equals(columnModel.isRequired());
    return new Column()
        .name(columnModel.getName())
        .type(columnModel.getDatatype())
        .arrayOf(columnModel.isArrayOf())
        .required(required);
  }

  public static Relationship relationshipModelToDatasetRelationship(
      RelationshipModel relationshipModel, Map<String, DatasetTable> tables) {
    Table fromTable = tables.get(relationshipModel.getFrom().getTable());
    Table toTable = tables.get(relationshipModel.getTo().getTable());
    return new Relationship()
        .name(relationshipModel.getName())
        .fromTable(fromTable)
        .fromColumn(fromTable.getColumnsMap().get(relationshipModel.getFrom().getColumn()))
        .toTable(toTable)
        .toColumn(toTable.getColumnsMap().get(relationshipModel.getTo().getColumn()));
  }

  public static RelationshipModel relationshipModelFromDatasetRelationship(
      Relationship datasetRel) {
    return new RelationshipModel()
        .name(datasetRel.getName())
        .from(
            relationshipTermModelFromColumn(datasetRel.getFromTable(), datasetRel.getFromColumn()))
        .to(relationshipTermModelFromColumn(datasetRel.getToTable(), datasetRel.getToColumn()));
  }

  protected static RelationshipTermModel relationshipTermModelFromColumn(Table table, Column col) {
    return new RelationshipTermModel().table(table.getName()).column(col.getName());
  }

  public static AssetSpecification assetModelToAssetSpecification(
      AssetModel assetModel,
      Map<String, DatasetTable> tables,
      Map<String, Relationship> datasetRelationships) {
    AssetSpecification spec = new AssetSpecification().name(assetModel.getName());
    List<String> assetRelationships = Objects.requireNonNullElse(assetModel.getFollow(), List.of());
    spec.assetTables(processAssetTables(spec, assetModel, tables));
    spec.assetRelationships(processAssetRelationships(assetRelationships, datasetRelationships));
    return spec;
  }

  private static List<AssetTable> processAssetTables(
      AssetSpecification spec, AssetModel assetModel, Map<String, DatasetTable> tables) {
    List<AssetTable> newAssetTables = new ArrayList<>();
    assetModel
        .getTables()
        .forEach(
            tblMod -> {
              boolean processingRootTable = false;
              String tableName = tblMod.getName();
              DatasetTable datasetTable = tables.get(tableName);
              // not sure if we need to set the id on the new table
              AssetTable newAssetTable = new AssetTable().datasetTable(datasetTable);
              if (assetModel.getRootTable().equals(tableName)) {
                spec.rootTable(newAssetTable);
                processingRootTable = true;
              }
              Map<String, Column> allTableColumns = datasetTable.getColumnsMap();
              List<String> colNamesToInclude = tblMod.getColumns();
              if (colNamesToInclude.isEmpty()) {
                colNamesToInclude = new ArrayList<>(allTableColumns.keySet());
              }
              Map<String, AssetColumn> assetColumnsMap =
                  colNamesToInclude.stream()
                      .collect(
                          Collectors.toMap(
                              colName -> colName,
                              colName ->
                                  new AssetColumn().datasetColumn(allTableColumns.get(colName))));
              newAssetTable.columns(new ArrayList<>(assetColumnsMap.values()));
              if (processingRootTable) {
                spec.rootColumn(assetColumnsMap.get(assetModel.getRootColumn()));
              }
              newAssetTables.add(newAssetTable);
            });
    return newAssetTables;
  }

  private static List<AssetRelationship> processAssetRelationships(
      List<String> assetRelationshipNames, Map<String, Relationship> relationships) {
    return Collections.unmodifiableList(
        relationships.entrySet().stream()
            .filter(map -> assetRelationshipNames.contains(map.getKey()))
            .map(entry -> new AssetRelationship().datasetRelationship(entry.getValue()))
            .collect(Collectors.toList()));
  }

  public static AssetModel assetModelFromAssetSpecification(AssetSpecification spec) {
    return new AssetModel()
        .name(spec.getName())
        .rootTable(spec.getRootTable().getTable().getName())
        .rootColumn(spec.getRootColumn().getDatasetColumn().getName())
        .tables(
            spec.getAssetTables().stream()
                .map(
                    table ->
                        new AssetTableModel()
                            .name(table.getTable().getName())
                            .columns(
                                table.getColumns().stream()
                                    .map(column -> column.getDatasetColumn().getName())
                                    .collect(Collectors.toList())))
                .collect(Collectors.toList()))
        .follow(
            spec.getAssetRelationships().stream()
                .map(assetRelationship -> assetRelationship.getDatasetRelationship().getName())
                .collect(Collectors.toList()));
  }
}
