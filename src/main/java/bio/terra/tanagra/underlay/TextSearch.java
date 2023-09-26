package bio.terra.tanagra.underlay;

public class TextSearch {
  private final boolean isEnabled;
  private final TextSearchMapping sourceMapping;
  private final TextSearchMapping indexMapping;
  private Entity entity;

  public TextSearch() {
    this.isEnabled = false;
    sourceMapping = null;
    indexMapping = null;
  }

  public TextSearch(TextSearchMapping sourceMapping, TextSearchMapping indexMapping) {
    this.isEnabled = true;
    this.sourceMapping = sourceMapping;
    this.indexMapping = indexMapping;
  }

  public void initialize(Entity entity) {
    this.entity = entity;
    if (sourceMapping != null) {
      sourceMapping.initialize(this);
    }
    if (indexMapping != null) {
      indexMapping.initialize(this);
    }
  }

  public TextSearchMapping getMapping(Underlay.MappingType mappingType) {
    return Underlay.MappingType.SOURCE.equals(mappingType) ? sourceMapping : indexMapping;
  }

  public boolean isEnabled() {
    return isEnabled;
  }

  public Entity getEntity() {
    return entity;
  }
}
