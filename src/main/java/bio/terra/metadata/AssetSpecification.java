package bio.terra.metadata;

import bio.terra.model.AssetModel;
import bio.terra.model.AssetTableModel;

import javax.annotation.concurrent.Immutable;
import java.util.*;
import java.util.stream.Collectors;

public class AssetSpecification {
    private UUID id;
    private String name;
    private StudyTable rootTable;
    private List<StudyTable> includedTables;
    private List<AssetColumn> assetColumns;
    private List<AssetRelationship> assetRelationships;

    public AssetSpecification(AssetModel assetModel, Map<String, StudyTable> tables, Map<String, StudyRelationship> relationships) {
        name = assetModel.getName();
        processAssetTables(assetModel.getTables(), tables);
        processAssetRelationships(assetModel.getFollow(), relationships);
    }

    private void processAssetTables(List<AssetTableModel> assetTables, Map<String, StudyTable> tables) {
        assetTables.forEach(tblMod -> {
            StudyTable studyTable = tables.get(tblMod.getName());
            if (tblMod.isIsRoot()) { rootTable = studyTable; }
            includedTables.add(studyTable);
            assetColumns = Collections.unmodifiableList(studyTable.getColumnsMap().entrySet()
                    .stream()
                    .filter(map -> tblMod.getColumns().contains(map.getKey()))
                    .map(entry -> new AssetColumn(entry.getValue()))
                    .collect(Collectors.toList()));
        });
    }

    private void processAssetRelationships(List<String> assetRelationshipNames, Map<String, StudyRelationship> relationships) {
        assetRelationships = Collections.unmodifiableList(relationships.entrySet()
                .stream()
                .filter(map -> assetRelationshipNames.contains(map.getKey()))
                .map(entry -> new AssetRelationship(entry.getValue()))
                .collect(Collectors.toList()));
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public StudyTable getRootTable() {
        return rootTable;
    }

    public List<StudyTable> getIncludedTables() {
        return Collections.unmodifiableList(includedTables);
    }

    public List<AssetColumn> getAssetColumns() {
        return Collections.unmodifiableList(assetColumns);
    }

    public List<AssetRelationship> getAssetRelationships() {
        return Collections.unmodifiableList(assetRelationships);
    }
}
