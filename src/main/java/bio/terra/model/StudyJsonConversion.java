package bio.terra.model;

import bio.terra.metadata.AssetSpecification;
import bio.terra.metadata.Study;
import bio.terra.metadata.StudyRelationship;
import bio.terra.metadata.StudyTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class StudyJsonConversion {

    // only allow use of static methods
    private StudyJsonConversion() {}

    public static Study studyRequestToStudy(StudyRequestModel studyRequest) {
        Map<String, StudyTable> tablesMap = new HashMap<>();
        Map<String, StudyRelationship> relationshipsMap = new HashMap<>();
        List<AssetSpecification> assetSpecifications = new ArrayList<>();

        StudySpecificationModel studySpecification = studyRequest.getSchema();
        studySpecification.getTables().forEach(tableModel ->
                tablesMap.put(tableModel.getName(), new StudyTable(tableModel)));
        studySpecification.getRelationships().forEach(relationship ->
                relationshipsMap.put(relationship.getName(), new StudyRelationship(relationship, tablesMap)));
        studySpecification.getAssets().forEach(asset ->
                assetSpecifications.add(new AssetSpecification(asset, tablesMap, relationshipsMap)));

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
                .description(study.getDescription());
    }

}
