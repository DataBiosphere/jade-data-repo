package bio.terra.metadata;

import bio.terra.model.*;

import java.util.*;

public class Study {

    private UUID id;
    private String name;
    private String description;

    // should Map be concurrent?
    private Map<String, StudyTable> tables;
    private Map<String, Relationship> relationships;
    //private List<AssetSpecification> assetSpecifications;
    // TODO: remove setters, aim to be immutable

    public Study(StudyRequestModel studyRequest) {
        this(studyRequest.getName(), studyRequest.getDescription(), new HashMap<>());

        StudySpecificationModel studySpecification = studyRequest.getSchema();
        for (TableModel tableModel : studySpecification.getTables()) {
            tables.put(tableModel.getName(), new StudyTable(tableModel));
        }
        for (RelationshipModel relationship : studySpecification.getRelationships()) {
            relationships.put(relationship.getName(), new Relationship(relationship, tables));
        }
    }

    // Constructor for building studies in unit tests.
    public Study(String name, String description, Map<String, StudyTable> tables) {
        this.name = name;
        this.description = description;
        this.tables = tables;
    }

    public StudySummaryModel toSummary() {
        return new StudySummaryModel()
                .id(this.id.toString())
                .name(this.name)
                .description(this.description);
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) { this.id = id; }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public Map<String, StudyTable> getTables() {
        return Collections.unmodifiableMap(tables);
    }

    public Map<String, Relationship> getRelationships() {
        return Collections.unmodifiableMap(relationships);
    }

}
