package bio.terra.model;

import bio.terra.metadata.AssetColumn;
import bio.terra.metadata.AssetRelationship;
import bio.terra.metadata.AssetSpecification;
import bio.terra.metadata.AssetTable;
import bio.terra.metadata.DrDataset;
import bio.terra.metadata.DrDatasetRelationship;
import bio.terra.metadata.DrDatasetSummary;
import bio.terra.metadata.Table;
import bio.terra.metadata.Column;
import bio.terra.model.RelationshipTermModel.CardinalityEnum;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class DrDatasetJsonConversion {

    // only allow use of static methods
    private DrDatasetJsonConversion() {}

    public static DrDataset datasetRequestToDataset(DrDatasetRequestModel datasetRequest) {
        Map<String, Table> tablesMap = new HashMap<>();
        Map<String, DrDatasetRelationship> relationshipsMap = new HashMap<>();
        List<AssetSpecification> assetSpecifications = new ArrayList<>();

        DrDatasetSpecificationModel datasetSpecification = datasetRequest.getSchema();
        datasetSpecification.getTables().forEach(tableModel ->
            tablesMap.put(tableModel.getName(), tableModelToTable(tableModel)));
        // Relationships are optional, so there might not be any here
        if (datasetSpecification.getRelationships() != null) {
            datasetSpecification.getRelationships().forEach(relationship ->
                relationshipsMap.put(
                    relationship.getName(),
                    relationshipModelToDatasetRelationship(relationship, tablesMap)));
        }
        datasetSpecification.getAssets().forEach(asset ->
            assetSpecifications.add(assetModelToAssetSpecification(asset, tablesMap, relationshipsMap)));

        return new DrDataset(new DrDatasetSummary()
            .name(datasetRequest.getName())
            .description(datasetRequest.getDescription()))
            .tables(new ArrayList<>(tablesMap.values()))
            .relationships(new ArrayList<>(relationshipsMap.values()))
            .assetSpecifications(assetSpecifications);
    }

    public static DrDatasetSummaryModel datasetSummaryModelFromDatasetSummary(DrDatasetSummary dataset) {
        return new DrDatasetSummaryModel()
                .id(dataset.getId().toString())
                .name(dataset.getName())
                .description(dataset.getDescription())
                .createdDate(dataset.getCreatedDate().toString());
    }

    public static DrDatasetModel datasetModelFromDataset(DrDataset dataset) {
        return new DrDatasetModel()
                .id(dataset.getId().toString())
                .name(dataset.getName())
                .description(dataset.getDescription())
                .createdDate(dataset.getCreatedDate().toString())
                .schema(datasetSpecificationModelFromDatasetSchema(dataset));
    }

    public static DrDatasetSpecificationModel datasetSpecificationModelFromDatasetSchema(DrDataset dataset) {
        return new DrDatasetSpecificationModel()
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

    public static Table tableModelToTable(TableModel tableModel) {
        Table datasetTable = new Table()
                .name(tableModel.getName());
        datasetTable
                .columns(tableModel.getColumns()
                        .stream()
                        .map(columnModel -> columnModelToDatasetColumn(columnModel).table(datasetTable))
                        .collect(Collectors.toList()));
        return datasetTable;
    }

    public static TableModel tableModelFromTable(Table datasetTable) {
        return new TableModel()
                .name(datasetTable.getName())
                .columns(datasetTable.getColumns()
                        .stream()
                        .map(column -> columnModelFromDatasetColumn(column))
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

    public static DrDatasetRelationship relationshipModelToDatasetRelationship(
            RelationshipModel relationshipModel,
            Map<String, Table> tables) {
        Table fromTable = tables.get(relationshipModel.getFrom().getTable());
        Table toTable = tables.get(relationshipModel.getTo().getTable());
        return new DrDatasetRelationship()
                .name(relationshipModel.getName())
                .fromTable(fromTable)
                .fromColumn(fromTable.getColumnsMap().get(relationshipModel.getFrom().getColumn()))
                .fromCardinality(relationshipModel.getFrom().getCardinality())
                .toTable(toTable)
                .toColumn(toTable.getColumnsMap().get(relationshipModel.getTo().getColumn()))
                .toCardinality(relationshipModel.getTo().getCardinality());
    }

    public static RelationshipModel relationshipModelFromDatasetRelationship(
            DrDatasetRelationship datasetRel) {
        return new RelationshipModel()
                .name(datasetRel.getName())
                .from(relationshipTermModelFromColumn(
                        datasetRel.getFromTable(), datasetRel.getFromColumn(), datasetRel.getFromCardinality()))
                .to(relationshipTermModelFromColumn(
                        datasetRel.getToTable(), datasetRel.getToColumn(), datasetRel.getToCardinality()));
    }

    protected static RelationshipTermModel relationshipTermModelFromColumn(
            Table table,
            Column col,
            CardinalityEnum cardinality) {
        return new RelationshipTermModel()
                .table(table.getName())
                .column(col.getName())
                .cardinality(cardinality);
    }

    public static AssetSpecification assetModelToAssetSpecification(AssetModel assetModel,
                                                                    Map<String, Table> tables,
                                                                    Map<String, DrDatasetRelationship> relationships) {
        AssetSpecification spec = new AssetSpecification()
                .name(assetModel.getName());
        spec.assetTables(processAssetTables(spec, assetModel, tables));
        spec.assetRelationships(processAssetRelationships(assetModel.getFollow(), relationships));
        return spec;
    }

    private static List<AssetTable> processAssetTables(
            AssetSpecification spec,
            AssetModel assetModel,
            Map<String, Table> tables) {
        List<AssetTable> newAssetTables = new ArrayList<>();
        assetModel.getTables().forEach(tblMod -> {
            boolean processingRootTable = false;
            String tableName = tblMod.getName();
            Table datasetTable = tables.get(tableName);
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

    private static List<AssetRelationship> processAssetRelationships(List<String> assetRelationshipNames,
                                                                     Map<String, DrDatasetRelationship> relationships) {
        return Collections.unmodifiableList(relationships.entrySet()
                .stream()
                .filter(map -> assetRelationshipNames.contains(map.getKey()))
                .map(entry -> new AssetRelationship().datasetRelationship(entry.getValue()))
                .collect(Collectors.toList()));
    }

    public static AssetModel assetModelFromAssetSpecification(AssetSpecification spec) {
        return new AssetModel()
                .name(spec.getName())
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
}
