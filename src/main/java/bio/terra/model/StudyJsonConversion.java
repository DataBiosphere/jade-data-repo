package bio.terra.model;

import bio.terra.metadata.AssetColumn;
import bio.terra.metadata.AssetRelationship;
import bio.terra.metadata.AssetSpecification;
import bio.terra.metadata.AssetTable;
import bio.terra.metadata.Study;
import bio.terra.metadata.StudyRelationship;
import bio.terra.metadata.StudySummary;
import bio.terra.metadata.StudyTable;
import bio.terra.metadata.StudyTableColumn;
import bio.terra.model.RelationshipTermModel.CardinalityEnum;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class StudyJsonConversion {

    // only allow use of static methods
    private StudyJsonConversion() {}

    public static Study studyRequestToStudy(StudyRequestModel studyRequest) {
        Map<String, StudyTable> tablesMap = new HashMap<>();
        Map<String, StudyRelationship> relationshipsMap = new HashMap<>();
        List<AssetSpecification> assetSpecifications = new ArrayList<>();

        StudySpecificationModel studySpecification = studyRequest.getSchema();
        studySpecification.getTables().forEach(tableModel ->
                tablesMap.put(tableModel.getName(), tableModelToStudyTable(tableModel)));
        // Relationships are optional, so there might not be any here
        if (studySpecification.getRelationships() != null) {
            studySpecification.getRelationships().forEach(relationship ->
                    relationshipsMap.put(
                            relationship.getName(),
                            relationshipModelToStudyRelationship(relationship, tablesMap)));
        }
        studySpecification.getAssets().forEach(asset ->
                assetSpecifications.add(assetModelToAssetSpecification(asset, tablesMap, relationshipsMap)));

        return new Study(new StudySummary()
                .setName(studyRequest.getName())
                .setDescription(studyRequest.getDescription()))
                .setTables(new ArrayList<>(tablesMap.values()))
                .setRelationships(new ArrayList<>(relationshipsMap.values()))
                .setAssetSpecifications(assetSpecifications);
    }

    public static StudySummaryModel studySummaryFromStudy(Study study) {
        return new StudySummaryModel()
                .id(study.getId().toString())
                .name(study.getName())
                .description(study.getDescription())
                .createdDate(study.getCreatedDate().toString());
    }

    public static StudyModel studyModelFromStudy(Study study) {
        return new StudyModel()
                .id(study.getId().toString())
                .name(study.getName())
                .description(study.getDescription())
                .createdDate(study.getCreatedDate().toString())
                .schema(studySpecificationModelFromStudySchema(study));
    }

    public static StudySpecificationModel studySpecificationModelFromStudySchema(Study study) {
        return new StudySpecificationModel()
                .tables(study.getTables()
                        .stream()
                        .map(table -> tableModelFromStudyTable(table))
                        .collect(Collectors.toList()))
                .relationships(study.getRelationships()
                        .stream()
                        .map(rel -> relationshipModelFromStudyRelationship(rel))
                        .collect(Collectors.toList()))
                .assets(study.getAssetSpecifications()
                        .stream()
                        .map(spec -> assetModelFromAssetSpecification(spec))
                        .collect(Collectors.toList()));
    }

    public static StudyTable tableModelToStudyTable(TableModel tableModel) {
        StudyTable studyTable = new StudyTable()
                .setName(tableModel.getName());
        studyTable
                .setColumns(tableModel.getColumns()
                        .stream()
                        .map(columnModel -> columnModelToStudyColumn(columnModel).setInTable(studyTable))
                        .collect(Collectors.toList()));
        return studyTable;
    }

    public static TableModel tableModelFromStudyTable(StudyTable studyTable) {
        return new TableModel()
                .name(studyTable.getName())
                .columns(studyTable.getColumns()
                        .stream()
                        .map(column -> columnModelFromStudyColumn(column))
                        .collect(Collectors.toList()));
    }

    public static StudyTableColumn columnModelToStudyColumn(ColumnModel columnModel) {
        return new StudyTableColumn()
                .setName(columnModel.getName())
                .setType(columnModel.getDatatype());
    }

    public static ColumnModel columnModelFromStudyColumn(StudyTableColumn tableColumn) {
        return new ColumnModel()
                .name(tableColumn.getName())
                .datatype(tableColumn.getType());
    }

    public static StudyRelationship relationshipModelToStudyRelationship(
            RelationshipModel relationshipModel,
            Map<String, StudyTable> tables) {
        return new StudyRelationship()
                .setName(relationshipModel.getName())
                .setFrom(getColumn(relationshipModel.getFrom(), tables))
                .setFromCardinality(relationshipModel.getFrom().getCardinality())
                .setTo(getColumn(relationshipModel.getTo(), tables))
                .setToCardinality(relationshipModel.getTo().getCardinality());
    }

    protected static StudyTableColumn getColumn(RelationshipTermModel relTerm, Map<String, StudyTable> tables) {
        return tables.get(relTerm.getTable()).getColumnsMap().get(relTerm.getColumn());
    }

    public static RelationshipModel relationshipModelFromStudyRelationship(
            StudyRelationship studyRel) {
        return new RelationshipModel()
                .name(studyRel.getName())
                .from(relationshipTermModelFromStudyTableColumn(studyRel.getFrom(), studyRel.getFromCardinality()))
                .to(relationshipTermModelFromStudyTableColumn(studyRel.getTo(), studyRel.getToCardinality()));
    }

    protected static RelationshipTermModel relationshipTermModelFromStudyTableColumn(
            StudyTableColumn rel,
            CardinalityEnum cardinality) {
        return new RelationshipTermModel()
                .table(rel.getInTable().getName())
                .column(rel.getName())
                .cardinality(cardinality);
    }

    public static AssetSpecification assetModelToAssetSpecification(AssetModel assetModel,
                                                                    Map<String, StudyTable> tables,
                                                                    Map<String, StudyRelationship> relationships) {
        AssetSpecification spec = new AssetSpecification()
                .setName(assetModel.getName());
        spec.setAssetTables(processAssetTables(spec, assetModel, tables));
        spec.setAssetRelationships(processAssetRelationships(assetModel.getFollow(), relationships));
        return spec;
    }

    private static List<AssetTable> processAssetTables(
            AssetSpecification spec,
            AssetModel assetModel,
            Map<String, StudyTable> tables) {
        List<AssetTable> newAssetTables = new ArrayList<>();
        assetModel.getTables().forEach(tblMod -> {
            boolean processingRootTable = false;
            String tableName = tblMod.getName();
            StudyTable studyTable = tables.get(tableName);
            //not sure if we need to set the id on the new table
            AssetTable newAssetTable = new AssetTable().setStudyTable(studyTable);
            if (assetModel.getRootTable().equals(tableName)) {
                spec.setRootTable(newAssetTable);
                processingRootTable = true;
            }
            Map<String, StudyTableColumn> allTableColumns = studyTable.getColumnsMap();
            List<String> colNamesToInclude = tblMod.getColumns();
            if (colNamesToInclude.isEmpty()) {
                colNamesToInclude = new ArrayList<>(allTableColumns.keySet());
            }
            Map<String, AssetColumn> assetColumnsMap = colNamesToInclude
                    .stream()
                    .collect(Collectors.toMap(colName -> colName, colName ->
                            new AssetColumn().setStudyColumn(allTableColumns.get(colName))));
            newAssetTable.setColumns(new ArrayList<>(assetColumnsMap.values()));
            if (processingRootTable) {
                spec.rootColumn(assetColumnsMap.get(assetModel.getRootColumn()));
            }
            newAssetTables.add(newAssetTable);
        });
        return newAssetTables;
    }

    private static List<AssetRelationship> processAssetRelationships(List<String> assetRelationshipNames,
                                                                     Map<String, StudyRelationship> relationships) {
        return Collections.unmodifiableList(relationships.entrySet()
                .stream()
                .filter(map -> assetRelationshipNames.contains(map.getKey()))
                .map(entry -> new AssetRelationship().setStudyRelationship(entry.getValue()))
                .collect(Collectors.toList()));
    }

    public static AssetModel assetModelFromAssetSpecification(AssetSpecification spec) {
        return new AssetModel()
                .name(spec.getName())
                .tables(spec.getAssetTables()
                        .stream()
                        .map(table ->
                                new AssetTableModel()
                                        .name(table.getStudyTable().getName())
                                        .columns(table.getColumns()
                                                .stream()
                                                .map(column -> column.getStudyColumn().getName())
                                                .collect(Collectors.toList())))
                        .collect(Collectors.toList()))
                .follow(spec.getAssetRelationships()
                        .stream()
                        .map(assetRelationship -> assetRelationship.getStudyRelationship().getName())
                        .collect(Collectors.toList()));
    }
}
