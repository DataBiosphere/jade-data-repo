package bio.terra.metadata;

import bio.terra.model.*;

import java.util.*;

public class Study {

    private UUID id;
    private String name;
    private String description;

    private Map<String, StudyTable> tables = new HashMap<>();
    private Map<String, StudyRelationship> relationships = new HashMap<>();

    public Study(StudyRequestModel studyRequest) {
        this(studyRequest.getName(), studyRequest.getDescription());

        StudySpecificationModel studySpecification = studyRequest.getSchema();
        for (TableModel tableModel : studySpecification.getTables()) {
            tables.put(tableModel.getName(), new StudyTable(tableModel));
        }
        for (RelationshipModel relationship : studySpecification.getRelationships()) {
            relationships.put(relationship.getName(), new StudyRelationship(relationship, tables));
        }
    }

    public Study(String name, String description) {
        this.name = name;
        this.description = description;
    }

    // Constructor for building studies in unit tests.
    public Study(String name, String description, Map<String, StudyTable> tables) {
        this(name, description);
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

    public Collection<StudyTable> getTables() {
        return Collections.unmodifiableCollection(tables.values());
    }

    public Map<String, StudyRelationship> getRelationships() {
        return Collections.unmodifiableMap(relationships);
    }

}
