package bio.terra.tanagra.serialization;

import bio.terra.tanagra.underlay.RelationshipMapping;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.HashMap;
import java.util.Map;

/**
 * External representation of a relationship mapping.
 *
 * <p>This is a POJO class intended for serialization. This JSON format is user-facing.
 */
@JsonDeserialize(builder = UFRelationshipMapping.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UFRelationshipMapping {
  private final UFTablePointer idPairsTable;
  private final UFFieldPointer idPairsIdA;
  private final UFFieldPointer idPairsIdB;
  private final Map<String, UFRollupInformation> rollupInformationMapA;
  private final Map<String, UFRollupInformation> rollupInformationMapB;

  public UFRelationshipMapping(RelationshipMapping relationshipMapping) {
    this.idPairsTable = new UFTablePointer(relationshipMapping.getIdPairsTable());
    this.idPairsIdA = new UFFieldPointer(relationshipMapping.getIdPairsIdA());
    this.idPairsIdB = new UFFieldPointer(relationshipMapping.getIdPairsIdB());

    Map<String, UFRollupInformation> rollupInformationMapA = new HashMap<>();
    if (relationshipMapping.getRollupInformationMapA() != null) {
      relationshipMapping.getRollupInformationMapA().entrySet().stream()
          .forEach(
              entry ->
                  rollupInformationMapA.put(
                      entry.getKey(), new UFRollupInformation(entry.getValue())));
    }
    this.rollupInformationMapA = rollupInformationMapA;

    Map<String, UFRollupInformation> rollupInformationMapB = new HashMap<>();
    if (relationshipMapping.getRollupInformationMapB() != null) {
      relationshipMapping.getRollupInformationMapB().entrySet().stream()
          .forEach(
              entry ->
                  rollupInformationMapB.put(
                      entry.getKey(), new UFRollupInformation(entry.getValue())));
    }
    this.rollupInformationMapB = rollupInformationMapB;
  }

  private UFRelationshipMapping(Builder builder) {
    this.idPairsTable = builder.idPairsTable;
    this.idPairsIdA = builder.idPairsIdA;
    this.idPairsIdB = builder.idPairsIdB;
    this.rollupInformationMapA = builder.rollupInformationMapA;
    this.rollupInformationMapB = builder.rollupInformationMapB;
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder {
    private UFTablePointer idPairsTable;
    private UFFieldPointer idPairsIdA;
    private UFFieldPointer idPairsIdB;
    private Map<String, UFRollupInformation> rollupInformationMapA;
    private Map<String, UFRollupInformation> rollupInformationMapB;

    public Builder idPairsTable(UFTablePointer idPairsTable) {
      this.idPairsTable = idPairsTable;
      return this;
    }

    public Builder idPairsIdA(UFFieldPointer idPairsIdA) {
      this.idPairsIdA = idPairsIdA;
      return this;
    }

    public Builder idPairsIdB(UFFieldPointer idPairsIdB) {
      this.idPairsIdB = idPairsIdB;
      return this;
    }

    public Builder rollupInformationMapA(Map<String, UFRollupInformation> rollupInformationMapA) {
      this.rollupInformationMapA = rollupInformationMapA;
      return this;
    }

    public Builder rollupInformationMapB(Map<String, UFRollupInformation> rollupInformationMapB) {
      this.rollupInformationMapB = rollupInformationMapB;
      return this;
    }

    public UFRelationshipMapping build() {
      return new UFRelationshipMapping(this);
    }
  }

  public UFTablePointer getIdPairsTable() {
    return idPairsTable;
  }

  public UFFieldPointer getIdPairsIdA() {
    return idPairsIdA;
  }

  public UFFieldPointer getIdPairsIdB() {
    return idPairsIdB;
  }

  public Map<String, UFRollupInformation> getRollupInformationMapA() {
    return rollupInformationMapA;
  }

  public Map<String, UFRollupInformation> getRollupInformationMapB() {
    return rollupInformationMapB;
  }
}
