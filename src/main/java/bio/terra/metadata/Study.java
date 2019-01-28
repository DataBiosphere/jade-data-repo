package bio.terra.metadata;

import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySpecificationModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.model.TableModel;

import java.util.ArrayList;
import java.util.List;

public class Study {

    private String id;
    private String name;
    private String description;

    private List<StudyTable> tables;
    //private List<Relationship> relationships;
    //private List<AssetSpecification> assetSpecifications;
    // TODO: remove setters, aim to be immutable

    public Study(StudyRequestModel studyRequest) {
        this(studyRequest.getName(), studyRequest.getDescription(), new ArrayList<>());

        StudySpecificationModel studySpecification = studyRequest.getSchema();
        for (TableModel tableModel : studySpecification.getTables()) {
            tables.add(new StudyTable(tableModel));
        }
    }

    // Constructor for building studies in unit tests.
    public Study(String name, String description, List<StudyTable> tables) {
        this.name = name;
        this.description = description;
        this.tables = tables;
    }

    public StudySummaryModel toSummary() {
        return new StudySummaryModel()
                .id(this.id)
                .name(this.name)
                .description(this.description);
    }

    public String getId() {
        return id;
    }

    public String setId() {
        return this.id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<StudyTable> getTables() {
        return tables;
    }

    public void setTables(List<StudyTable> tables) {
        this.tables = tables;
    }

}
