package bio.terra.service.dataset;

import bio.terra.common.Column;
import bio.terra.common.PdaoConstant;
import bio.terra.common.Relationship;
import bio.terra.common.Table;
import bio.terra.model.AssetModel;
import bio.terra.model.AssetTableModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSpecificationModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DatePartitionOptionsModel;
import bio.terra.model.GoogleCloudResource;
import bio.terra.model.GoogleRegion;
import bio.terra.model.IntPartitionOptionsModel;
import bio.terra.model.RelationshipModel;
import bio.terra.model.RelationshipTermModel;
import bio.terra.model.StorageResourceModel;
import bio.terra.model.TableModel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public final class DatasetJsonConversion {

    private static final GoogleRegion DEFAULT_GOOGLE_REGION = GoogleRegion.US_CENTRAL1;
    private static final CloudPlatform DEFAULT_CLOUD_PLATFORM = CloudPlatform.GCP;

    // only allow use of static methods
    private DatasetJsonConversion() {}

    public static Dataset datasetRequestToDataset(DatasetRequestModel datasetRequest) {
        Map<String, DatasetTable> tablesMap = new HashMap<>();
        Map<String, Relationship> relationshipsMap = new HashMap<>();
        List<AssetSpecification> assetSpecifications = new ArrayList<>();
        UUID defaultProfileId = UUID.fromString(datasetRequest.getDefaultProfileId());
        DatasetSpecificationModel datasetSpecification = datasetRequest.getSchema();
        datasetSpecification.getTables().forEach(tableModel ->
                tablesMap.put(tableModel.getName(), tableModelToTable(tableModel)));
        // Relationships are optional, so there might not be any here
        if (datasetSpecification.getRelationships() != null) {
            datasetSpecification.getRelationships().forEach(relationship ->
                    relationshipsMap.put(
                            relationship.getName(),
                            relationshipModelToDatasetRelationship(relationship, tablesMap)));
        }
        List<AssetModel> assets = datasetSpecification.getAssets();
        if (assets != null) {
            assets.forEach(asset ->
                assetSpecifications.add(assetModelToAssetSpecification(asset, tablesMap, relationshipsMap)));
        }

        CloudPlatform cloudPlatform = Optional.ofNullable(datasetRequest.getCloudPlatform())
            .orElse(DEFAULT_CLOUD_PLATFORM);

        final List<StorageResource> storageResources;
        if (cloudPlatform == CloudPlatform.GCP) {
            storageResources = instantiateGcpResources(datasetRequest.getRegion());
        } else {
            throw new UnsupportedOperationException(cloudPlatform + " is not a valid Cloud Platform");
        }

        return new Dataset(new DatasetSummary()
                .name(datasetRequest.getName())
                .description(datasetRequest.getDescription())
                .storage(storageResources)
                .defaultProfileId(defaultProfileId))
                .tables(new ArrayList<>(tablesMap.values()))
                .relationships(new ArrayList<>(relationshipsMap.values()))
                .assetSpecifications(assetSpecifications);
    }

    private static List<StorageResource> instantiateGcpResources(String providedRegion) {
        final GoogleRegion region = Optional.ofNullable(providedRegion)
            .map(s -> GoogleRegion.fromValue(s.toLowerCase()))
            .orElse(DEFAULT_GOOGLE_REGION);
        return Arrays.stream(GoogleCloudResource.values()).map(resource -> new StorageResource()
            .cloudPlatform(CloudPlatform.GCP)
            .region(region.toString())
            .cloudResource(resource.toString()))
            .collect(Collectors.toList());
    }

    public static DatasetSummaryModel datasetSummaryModelFromDatasetSummary(DatasetSummary datasetSummary) {
        return new DatasetSummaryModel()
                .id(datasetSummary.getId().toString())
                .name(datasetSummary.getName())
                .description(datasetSummary.getDescription())
                .createdDate(datasetSummary.getCreatedDate().toString())
                .defaultProfileId(datasetSummary.getDefaultProfileId().toString())
                .storage(storageResourceModelFromDatasetSummary(datasetSummary));

    }

    public static DatasetModel populateDatasetModelFromDataset(Dataset dataset) {
        return new DatasetModel()
                .id(dataset.getId().toString())
                .name(dataset.getName())
                .description(dataset.getDescription())
                .defaultProfileId(dataset.getDefaultProfileId().toString())
                .createdDate(dataset.getCreatedDate().toString())
                .schema(datasetSpecificationModelFromDatasetSchema(dataset))
                .dataProject(dataset.getProjectResource().getGoogleProjectId())
                .storage(storageResourceModelFromDatasetSummary(dataset.getDatasetSummary()));
    }

    private static List<StorageResourceModel> storageResourceModelFromDatasetSummary(DatasetSummary datasetSummary) {
        return datasetSummary.getStorage().stream().map(storage ->
            new StorageResourceModel()
                .cloudResource(storage.getCloudResource())
                .region(storage.getRegion())
                .cloudPlatform(storage.getCloudPlatform()))
            .collect(Collectors.toList());
    }

    public static DatasetSpecificationModel datasetSpecificationModelFromDatasetSchema(Dataset dataset) {
        return new DatasetSpecificationModel()
                .tables(dataset.getTables()
                        .stream()
                        .map(table -> tableModelFromTable(table))
                        .collect(Collectors.toList()))
                .relationships(dataset.getRelationships()
                        .stream()
                        .map(rel -> relationshipModelFromDatasetRelationship(rel))
                        .collect(Collectors.toList()))
                .assets(dataset.getAssetSpecifications()
                        .stream()
                        .map(spec -> assetModelFromAssetSpecification(spec))
                        .collect(Collectors.toList()));
    }

    public static DatasetTable tableModelToTable(TableModel tableModel) {
        Map<String, Column> columnMap = new HashMap<>();
        List<Column> columns = new ArrayList<>();
        DatasetTable datasetTable = new DatasetTable().name(tableModel.getName());

        for (ColumnModel columnModel : tableModel.getColumns()) {
            Column column = columnModelToDatasetColumn(columnModel).table(datasetTable);
            columnMap.put(column.getName(), column);
            columns.add(column);
        }

        List<Column> primaryKeyColumns = Optional.ofNullable(tableModel.getPrimaryKey())
            .orElse(Collections.emptyList())
            .stream()
            .map(columnMap::get)
            .collect(Collectors.toList());
        datasetTable.primaryKey(primaryKeyColumns);

        BigQueryPartitionConfigV1 partitionConfig;
        switch (tableModel.getPartitionMode()) {
            case DATE:
                String column = tableModel.getDatePartitionOptions().getColumn();
                boolean useIngestDate = column.equals(PdaoConstant.PDAO_INGEST_DATE_COLUMN_ALIAS);
                partitionConfig = useIngestDate ?
                    BigQueryPartitionConfigV1.ingestDate() :
                    BigQueryPartitionConfigV1.date(column);
                break;
            case INT:
                IntPartitionOptionsModel options = tableModel.getIntPartitionOptions();
                partitionConfig = BigQueryPartitionConfigV1.intRange(
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
                dateOptions = new DatePartitionOptionsModel().column(PdaoConstant.PDAO_INGEST_DATE_COLUMN_ALIAS);
                break;
            case DATE:
                partitionMode = TableModel.PartitionModeEnum.DATE;
                dateOptions = new DatePartitionOptionsModel().column(config.getColumnName());
                break;
            case INT_RANGE:
                partitionMode = TableModel.PartitionModeEnum.INT;
                intOptions = new IntPartitionOptionsModel()
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
            .primaryKey(datasetTable.getPrimaryKey()
                .stream()
                .map(Column::getName)
                .collect(Collectors.toList()))
            .partitionMode(partitionMode)
            .datePartitionOptions(dateOptions)
            .intPartitionOptions(intOptions)
            .columns(datasetTable.getColumns().stream()
                .map(Column::toColumnModel)
                .collect(Collectors.toList()));
    }

    public static Column columnModelToDatasetColumn(ColumnModel columnModel) {
        return new Column()
                .name(columnModel.getName())
                .type(columnModel.getDatatype())
                .arrayOf(columnModel.isArrayOf());
    }

    public static Relationship relationshipModelToDatasetRelationship(
            RelationshipModel relationshipModel,
            Map<String, DatasetTable> tables) {
        Table fromTable = tables.get(relationshipModel.getFrom().getTable());
        Table toTable = tables.get(relationshipModel.getTo().getTable());
        return new Relationship()
                .name(relationshipModel.getName())
                .fromTable(fromTable)
                .fromColumn(fromTable.getColumnsMap().get(relationshipModel.getFrom().getColumn()))
                .toTable(toTable)
                .toColumn(toTable.getColumnsMap().get(relationshipModel.getTo().getColumn()));
    }

    public static RelationshipModel relationshipModelFromDatasetRelationship(Relationship datasetRel) {
        return new RelationshipModel()
                .name(datasetRel.getName())
                .from(relationshipTermModelFromColumn(
                        datasetRel.getFromTable(), datasetRel.getFromColumn()))
                .to(relationshipTermModelFromColumn(
                        datasetRel.getToTable(), datasetRel.getToColumn()));
    }

    protected static RelationshipTermModel relationshipTermModelFromColumn(
            Table table,
            Column col) {
        return new RelationshipTermModel()
                .table(table.getName())
                .column(col.getName());
    }

    public static AssetSpecification assetModelToAssetSpecification(
            AssetModel assetModel,
            Map<String, DatasetTable> tables,
            Map<String, Relationship> relationships) {
        AssetSpecification spec = new AssetSpecification()
                .name(assetModel.getName());
        spec.assetTables(processAssetTables(spec, assetModel, tables));
        spec.assetRelationships(processAssetRelationships(assetModel.getFollow(), relationships));
        return spec;
    }

    private static List<AssetTable> processAssetTables(
            AssetSpecification spec,
            AssetModel assetModel,
            Map<String, DatasetTable> tables) {
        List<AssetTable> newAssetTables = new ArrayList<>();
        assetModel.getTables().forEach(tblMod -> {
            boolean processingRootTable = false;
            String tableName = tblMod.getName();
            DatasetTable datasetTable = tables.get(tableName);
            //not sure if we need to set the id on the new table
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
            Map<String, AssetColumn> assetColumnsMap = colNamesToInclude
                    .stream()
                    .collect(Collectors.toMap(colName -> colName, colName ->
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
            List<String> assetRelationshipNames,
            Map<String, Relationship> relationships) {
        return Collections.unmodifiableList(relationships.entrySet()
                .stream()
                .filter(map -> assetRelationshipNames.contains(map.getKey()))
                .map(entry -> new AssetRelationship().datasetRelationship(entry.getValue()))
                .collect(Collectors.toList()));
    }

    public static AssetModel assetModelFromAssetSpecification(AssetSpecification spec) {
        return new AssetModel()
            .name(spec.getName())
            .rootTable(spec.getRootTable().getTable().getName())
            .rootColumn(spec.getRootColumn().getDatasetColumn().getName())
            .tables(spec.getAssetTables()
                    .stream()
                    .map(table ->
                            new AssetTableModel()
                                    .name(table.getTable().getName())
                                    .columns(table.getColumns()
                                            .stream()
                                            .map(column -> column.getDatasetColumn().getName())
                                            .collect(Collectors.toList())))
                    .collect(Collectors.toList()))
            .follow(spec.getAssetRelationships()
                    .stream()
                    .map(assetRelationship -> assetRelationship.getDatasetRelationship().getName())
                    .collect(Collectors.toList()));
    }

    private static List<String> uuidsToStrings(List<UUID> uuids) {
        if (uuids == null) {
            return null;
        }
        return uuids.stream().map(UUID::toString).collect(Collectors.toList());
    }

    private static List<UUID> stringsToUUIDs(List<String> strings) {
        if (strings == null) {
            return null;
        }
        return strings.stream().map(UUID::fromString).collect(Collectors.toList());
    }
}
