package bio.terra.service.dataset;

import bio.terra.common.Table;
import bio.terra.common.Column;
import bio.terra.model.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public final class DatasetJsonConversion {

    // only allow use of static methods
    private DatasetJsonConversion() {}

    public static Dataset datasetRequestToDataset(DatasetRequestModel datasetRequest) {
        Map<String, DatasetTable> tablesMap = new HashMap<>();
        Map<String, DatasetRelationship> relationshipsMap = new HashMap<>();
        List<AssetSpecification> assetSpecifications = new ArrayList<>();
        UUID defaultProfileId = UUID.fromString(datasetRequest.getDefaultProfileId());
        List<UUID> additionalProfileIds = stringsToUUIDs(datasetRequest.getAdditionalProfileIds());
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

        return new Dataset(new DatasetSummary()
                .name(datasetRequest.getName())
                .description(datasetRequest.getDescription())
                .defaultProfileId(defaultProfileId)
                .additionalProfileIds(additionalProfileIds))
                .tables(new ArrayList<>(tablesMap.values()))
                .relationships(new ArrayList<>(relationshipsMap.values()))
                .assetSpecifications(assetSpecifications);
    }

    public static DatasetSummaryModel datasetSummaryModelFromDatasetSummary(DatasetSummary dataset) {
        return new DatasetSummaryModel()
                .id(dataset.getId().toString())
                .name(dataset.getName())
                .description(dataset.getDescription())
                .createdDate(dataset.getCreatedDate().toString())
                .defaultProfileId(dataset.getDefaultProfileId().toString())
                .additionalProfileIds(uuidsToStrings(dataset.getAdditionalProfileIds()));
    }

    public static DatasetModel populateDatasetModelFromDataset(Dataset dataset) {
        return new DatasetModel()
                .id(dataset.getId().toString())
                .name(dataset.getName())
                .description(dataset.getDescription())
                .defaultProfileId(dataset.getDefaultProfileId().toString())
                .additionalProfileIds(uuidsToStrings(dataset.getAdditionalProfileIds()))
                .createdDate(dataset.getCreatedDate().toString())
                .schema(datasetSpecificationModelFromDatasetSchema(dataset));
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

        datasetTable.partitionStrategy(tableModel.getPartitionStrategy());
        Optional<Column> partitionColumn = Optional.ofNullable(tableModel.getPartitionColumn())
            .map(columnMap::get);
        datasetTable.partitionColumn(partitionColumn.orElse(null));
        datasetTable.intPartitionSettings(tableModel.getIntPartitionOptions());

        return datasetTable.columns(columns);
    }

    public static TableModel tableModelFromTable(DatasetTable datasetTable) {
        return new TableModel()
            .name(datasetTable.getName())
            .primaryKey(datasetTable.getPrimaryKey()
                .stream()
                .map(Column::getName)
                .collect(Collectors.toList()))
            .columns(datasetTable.getColumns().stream()
                .map(DatasetJsonConversion::columnModelFromDatasetColumn)
                .collect(Collectors.toList()));
    }

    public static Column columnModelToDatasetColumn(ColumnModel columnModel) {
        return new Column()
                .name(columnModel.getName())
                .type(columnModel.getDatatype())
                .arrayOf(columnModel.isArrayOf());
    }

    public static ColumnModel columnModelFromDatasetColumn(Column tableColumn) {
        return new ColumnModel()
                .name(tableColumn.getName())
                .datatype(tableColumn.getType())
                .arrayOf(tableColumn.isArrayOf());
    }

    public static DatasetRelationship relationshipModelToDatasetRelationship(
            RelationshipModel relationshipModel,
            Map<String, DatasetTable> tables) {
        Table fromTable = tables.get(relationshipModel.getFrom().getTable());
        Table toTable = tables.get(relationshipModel.getTo().getTable());
        return new DatasetRelationship()
                .name(relationshipModel.getName())
                .fromTable(fromTable)
                .fromColumn(fromTable.getColumnsMap().get(relationshipModel.getFrom().getColumn()))
                .toTable(toTable)
                .toColumn(toTable.getColumnsMap().get(relationshipModel.getTo().getColumn()));
    }

    public static RelationshipModel relationshipModelFromDatasetRelationship(DatasetRelationship datasetRel) {
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
            Map<String, DatasetRelationship> relationships) {
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
            Map<String, DatasetRelationship> relationships) {
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
