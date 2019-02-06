package bio.terra.model;

import bio.terra.metadata.AssetColumn;
import bio.terra.metadata.AssetRelationship;
import bio.terra.metadata.AssetSpecification;
import bio.terra.metadata.Study;
import bio.terra.metadata.StudyRelationship;
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
        studySpecification.getRelationships().forEach(relationship ->
                relationshipsMap.put(
                        relationship.getName(),
                        relationshipModelToStudyRelationship(relationship, tablesMap)));
        studySpecification.getAssets().forEach(asset ->
                assetSpecifications.add(assetModelToAssetSpecification(asset, tablesMap, relationshipsMap)));

        return new Study()
                .setName(studyRequest.getName())
                .setDescription(studyRequest.getDescription())
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
                .schema(studySpecificationModelFromStudy(study));
    }

    public static StudySpecificationModel studySpecificationModelFromStudy(Study study) {
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
        AssetSpecification spec = new AssetSpecification().setName(assetModel.getName());
        processAssetTables(spec, assetModel.getTables(), tables);
        spec.setAssetRelationships(processAssetRelationships(assetModel.getFollow(), relationships));
        return spec;
    }

    private static void processAssetTables(
            AssetSpecification spec,
            List<AssetTableModel> assetTables,
            Map<String, StudyTable> tables) {
        List<StudyTable> includedTables = new ArrayList<>();
        List<AssetColumn> assetColumns = new ArrayList<>();
        assetTables.forEach(tblMod -> {
            StudyTable studyTable = tables.get(tblMod.getName());
            // TODO fix this so it defaults to false
            if (tblMod.isIsRoot() != null && tblMod.isIsRoot()) { spec.setRootTable(studyTable); }
            includedTables.add(studyTable);
            assetColumns.addAll(Collections.unmodifiableList(studyTable.getColumnsMap().entrySet()
                    .stream()
                    .filter(entryToFilter -> tblMod.getColumns().contains(entryToFilter.getKey()))
                    .map(entry -> new AssetColumn().setStudyColumn(entry.getValue()))
                    .collect(Collectors.toList())));
        });
        spec.setAssetColumns(assetColumns);
        spec.setIncludedTables(includedTables);
    }

    private static List<AssetRelationship> processAssetRelationships(List<String> assetRelationshipNames,
                                                                     Map<String, StudyRelationship> relationships) {
        return Collections.unmodifiableList(relationships.entrySet()
                .stream()
                .filter(map -> assetRelationshipNames.contains(map.getKey()))
                .map(entry -> new AssetRelationship(entry.getValue()))
                .collect(Collectors.toList()));
    }

    public static AssetModel assetModelFromAssetSpecification(AssetSpecification spec) {
        return new AssetModel()
                .name(spec.getName())
                .tables(setAssetModelTablesAndColumns(spec))
                .follow(spec.getAssetRelationships()
                        .stream()
                        .map(assetRelationship -> assetRelationship.getStudyRelationship().getName())
                        .collect(Collectors.toList()));
    }

    public static List<AssetTableModel> setAssetModelTablesAndColumns(AssetSpecification spec) {
        Map<String, List<String>> tableToColumns = new HashMap<>();
        String rootTableName = spec.getRootTable().getName();

        // build structure
        spec.getAssetColumns()
                .forEach(column -> {
                    String tableName = column.getStudyColumn().getInTable().getName();
                    if (!tableToColumns.containsKey(tableName)) {
                        tableToColumns.put(tableName, new ArrayList<>());
                    }
                    tableToColumns.get(tableName).add(column.getStudyColumn().getName());
                });

        return tableToColumns.entrySet()
                .stream()
                .map(entry -> new AssetTableModel()
                        .name(entry.getKey())
                        .columns(entry.getValue())
                        .isRoot(entry.getKey().equals(rootTableName)))
                .collect(Collectors.toList());
    }

}
