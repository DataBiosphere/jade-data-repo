package bio.terra.model;

import bio.terra.metadata.AssetColumn;
import bio.terra.metadata.AssetRelationship;
import bio.terra.metadata.AssetSpecification;
import bio.terra.metadata.AssetTable;
import bio.terra.metadata.BillingProfile;
import bio.terra.metadata.Study;
import bio.terra.metadata.StudyRelationship;
import bio.terra.metadata.StudySummary;
import bio.terra.metadata.Table;
import bio.terra.metadata.Column;
import bio.terra.model.RelationshipTermModel.CardinalityEnum;
import bio.terra.service.ProfileService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public final class StudyJsonConversion {

    // only allow use of static methods
    private StudyJsonConversion() {}

    public static Study studyRequestToStudy(StudyRequestModel studyRequest) {
        Map<String, Table> tablesMap = new HashMap<>();
        Map<String, StudyRelationship> relationshipsMap = new HashMap<>();
        List<AssetSpecification> assetSpecifications = new ArrayList<>();
        UUID defaultProfileId = UUID.fromString(studyRequest.getDefaultProfile());
        List<UUID> additionalProfileIds = stringsToUUIDs(studyRequest.getAdditionalProfiles());
        StudySpecificationModel studySpecification = studyRequest.getSchema();
        studySpecification.getTables().forEach(tableModel ->
                tablesMap.put(tableModel.getName(), tableModelToTable(tableModel)));
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
                .name(studyRequest.getName())
                .description(studyRequest.getDescription())
                .defaultProfileId(defaultProfileId)
                .additionalProfileIds(additionalProfileIds))
                .tables(new ArrayList<>(tablesMap.values()))
                .relationships(new ArrayList<>(relationshipsMap.values()))
                .assetSpecifications(assetSpecifications);
    }

    public static StudySummaryModel studySummaryModelFromStudySummary(StudySummary study) {
        return new StudySummaryModel()
                .id(study.getId().toString())
                .name(study.getName())
                .description(study.getDescription())
                .createdDate(study.getCreatedDate().toString())
                .defaultProfileId(study.getDefaultProfileId().toString())
                .additionalProfileIds(uuidsToStrings(study.getAdditionalProfileIds()));
    }

    public static StudyModel studyModelFromStudy(Study study) {
        List<BillingProfile> additionalProfiles = study.getAdditionalProfiles();
        List<BillingProfileModel> additionalProfileModels = null;
        if (additionalProfiles != null) {
            additionalProfileModels = additionalProfiles
                .stream()
                .map(ProfileService::makeModelFromBillingProfile)
                .collect(Collectors.toList());
        }

        return new StudyModel()
                .id(study.getId().toString())
                .name(study.getName())
                .description(study.getDescription())
                .defaultProfile(ProfileService.makeModelFromBillingProfile(study.getDefaultProfile()))
                .additionalProfiles(additionalProfileModels)
                .createdDate(study.getCreatedDate().toString())
                .schema(studySpecificationModelFromStudySchema(study));
    }

    public static StudySpecificationModel studySpecificationModelFromStudySchema(Study study) {
        return new StudySpecificationModel()
                .tables(study.getTables()
                        .stream()
                        .map(table -> tableModelFromTable(table))
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

    public static Table tableModelToTable(TableModel tableModel) {
        Table studyTable = new Table()
                .name(tableModel.getName());
        studyTable
                .columns(tableModel.getColumns()
                        .stream()
                        .map(columnModel -> columnModelToStudyColumn(columnModel).table(studyTable))
                        .collect(Collectors.toList()));
        return studyTable;
    }

    public static TableModel tableModelFromTable(Table studyTable) {
        return new TableModel()
                .name(studyTable.getName())
                .columns(studyTable.getColumns()
                        .stream()
                        .map(column -> columnModelFromStudyColumn(column))
                        .collect(Collectors.toList()));
    }

    public static Column columnModelToStudyColumn(ColumnModel columnModel) {
        return new Column()
                .name(columnModel.getName())
                .type(columnModel.getDatatype())
                .arrayOf(columnModel.isArrayOf());
    }

    public static ColumnModel columnModelFromStudyColumn(Column tableColumn) {
        return new ColumnModel()
                .name(tableColumn.getName())
                .datatype(tableColumn.getType())
                .arrayOf(tableColumn.isArrayOf());
    }

    public static StudyRelationship relationshipModelToStudyRelationship(
            RelationshipModel relationshipModel,
            Map<String, Table> tables) {
        Table fromTable = tables.get(relationshipModel.getFrom().getTable());
        Table toTable = tables.get(relationshipModel.getTo().getTable());
        return new StudyRelationship()
                .name(relationshipModel.getName())
                .fromTable(fromTable)
                .fromColumn(fromTable.getColumnsMap().get(relationshipModel.getFrom().getColumn()))
                .fromCardinality(relationshipModel.getFrom().getCardinality())
                .toTable(toTable)
                .toColumn(toTable.getColumnsMap().get(relationshipModel.getTo().getColumn()))
                .toCardinality(relationshipModel.getTo().getCardinality());
    }

    public static RelationshipModel relationshipModelFromStudyRelationship(StudyRelationship studyRel) {
        return new RelationshipModel()
                .name(studyRel.getName())
                .from(relationshipTermModelFromColumn(
                        studyRel.getFromTable(), studyRel.getFromColumn(), studyRel.getFromCardinality()))
                .to(relationshipTermModelFromColumn(
                        studyRel.getToTable(), studyRel.getToColumn(), studyRel.getToCardinality()));
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

    public static AssetSpecification assetModelToAssetSpecification(
            AssetModel assetModel,
            Map<String, Table> tables,
            Map<String, StudyRelationship> relationships) {
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
            Table studyTable = tables.get(tableName);
            //not sure if we need to set the id on the new table
            AssetTable newAssetTable = new AssetTable().studyTable(studyTable);
            if (assetModel.getRootTable().equals(tableName)) {
                spec.rootTable(newAssetTable);
                processingRootTable = true;
            }
            Map<String, Column> allTableColumns = studyTable.getColumnsMap();
            List<String> colNamesToInclude = tblMod.getColumns();
            if (colNamesToInclude.isEmpty()) {
                colNamesToInclude = new ArrayList<>(allTableColumns.keySet());
            }
            Map<String, AssetColumn> assetColumnsMap = colNamesToInclude
                    .stream()
                    .collect(Collectors.toMap(colName -> colName, colName ->
                            new AssetColumn().studyColumn(allTableColumns.get(colName))));
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
            Map<String, StudyRelationship> relationships) {
        return Collections.unmodifiableList(relationships.entrySet()
                .stream()
                .filter(map -> assetRelationshipNames.contains(map.getKey()))
                .map(entry -> new AssetRelationship().studyRelationship(entry.getValue()))
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
                                                .map(column -> column.getStudyColumn().getName())
                                                .collect(Collectors.toList())))
                        .collect(Collectors.toList()))
                .follow(spec.getAssetRelationships()
                        .stream()
                        .map(assetRelationship -> assetRelationship.getStudyRelationship().getName())
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
