package bio.terra.metadata;

import bio.terra.model.RelationshipModel;
import bio.terra.model.RelationshipTermModel;

import java.util.Map;
import java.util.UUID;

public class Relationship {

    private UUID id;
    private String name;
    private StudyTableColumn from;
    private RelationshipTermModel.CardinalityEnum fromCardinality;
    private StudyTableColumn to;
    private RelationshipTermModel.CardinalityEnum toCardinality;

    public Relationship(RelationshipModel relationshipModel, Map<String, StudyTable> tables) {
        name = relationshipModel.getName();
        from = getColumn(relationshipModel.getFrom(), tables);
        fromCardinality = relationshipModel.getFrom().getCardinality();
        to = getColumn(relationshipModel.getTo(), tables);
        toCardinality = relationshipModel.getTo().getCardinality();
    }

    protected StudyTableColumn getColumn(RelationshipTermModel relTerm, Map<String, StudyTable> tables) {
        return tables.get(relTerm.getTable()).getColumns().get(relTerm.getColumn());
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) { this.id = id; }

    public String getName() { return name; }

    public StudyTableColumn getFrom() { return from; }

    public StudyTableColumn getTo() { return to; }

    public RelationshipTermModel.CardinalityEnum getFromCardinality() { return fromCardinality; }

    public RelationshipTermModel.CardinalityEnum getToCardinality() { return toCardinality; }
}
