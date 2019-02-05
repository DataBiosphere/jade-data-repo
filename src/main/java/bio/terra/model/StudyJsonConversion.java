package bio.terra.model;

import bio.terra.metadata.AssetSpecification;
import bio.terra.metadata.Study;
import bio.terra.metadata.StudyRelationship;
import bio.terra.metadata.StudyTable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class StudyJsonConversion {

    public static Study studyRequestToStudy(StudyRequestModel studyRequest) {
        Map<String, StudyTable> tables = new HashMap<>();
        Map<String, StudyRelationship> relationships = new HashMap<>();
        Map<String, AssetSpecification> assetSpecifications = new HashMap<>();

        StudySpecificationModel studySpecification = studyRequest.getSchema();
        studySpecification.getTables().forEach(tableModel ->
                tables.put(tableModel.getName(), new StudyTable(tableModel)));
        studySpecification.getRelationships().forEach(relationship ->
                relationships.put(relationship.getName(), new StudyRelationship(relationship, tables)));
        studySpecification.getAssets().forEach(asset ->
                assetSpecifications.put(asset.getName(), new AssetSpecification(asset, tables, relationships)));

        return new Study(studyRequest.getName(), studyRequest.getDescription(), tables, relationships, assetSpecifications);
    }


    public static StudySummaryModel studySummaryFromStudy(Study study) {
        return new StudySummaryModel()
                .id(study.getId().toString())
                .name(study.getName())
                .description(study.getDescription());
    }

}
