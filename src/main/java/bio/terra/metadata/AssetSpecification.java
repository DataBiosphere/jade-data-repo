package bio.terra.metadata;

import bio.terra.model.AssetModel;
import bio.terra.model.AssetTableModel;

import java.util.*;
import java.util.stream.Collectors;

public class AssetSpecification {
    private UUID id;
    private String name;
    private StudyTable rootTable;
    private List<StudyTable> includedTables = new ArrayList<>();
    private List<AssetColumn> assetColumns = new ArrayList<>();
    private List<AssetRelationship> assetRelationships;

    public AssetSpecification(AssetModel assetModel,
                              Map<String, StudyTable> tables,
                              Map<String, StudyRelationship> relationships) {
        name = assetModel.getName();
        processAssetTables(assetModel.getTables(), tables);
        processAssetRelationships(assetModel.getFollow(), relationships);
    }

    private void processAssetTables(List<AssetTableModel> assetTables, Map<String, StudyTable> tables) {
        assetTables.forEach(tblMod -> {
            StudyTable studyTable = tables.get(tblMod.getName());
            // TODO fix this so it defaults to false
            if (tblMod.isIsRoot() != null && tblMod.isIsRoot()) { rootTable = studyTable; }
            includedTables.add(studyTable);
            assetColumns.addAll(Collections.unmodifiableList(studyTable.getColumnsMap().entrySet()
                    .stream()
                    .filter(entryToFilter -> tblMod.getColumns().contains(entryToFilter.getKey()))
                    .map(entry -> new AssetColumn(entry.getValue()))
                    .collect(Collectors.toList())));
        });
    }

    private void processAssetRelationships(List<String> assetRelationshipNames,
                                           Map<String, StudyRelationship> relationships) {
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
