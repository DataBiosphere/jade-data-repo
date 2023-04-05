package bio.terra.tanagra.underlay;

import java.util.List;

public class AuxiliaryData {
  private final String name;
  private final List<String> fields;

  private AuxiliaryDataMapping sourceMapping;
  private AuxiliaryDataMapping indexMapping;

  public AuxiliaryData(String name, List<String> fields) {
    this.name = name;
    this.fields = fields;
  }

  public void initialize(AuxiliaryDataMapping sourceMapping, AuxiliaryDataMapping indexMapping) {
    this.sourceMapping = sourceMapping;
    this.indexMapping = indexMapping;
  }

  public String getName() {
    return name;
  }

  public List<String> getFields() {
    return fields;
  }

  public AuxiliaryDataMapping getMapping(Underlay.MappingType mappingType) {
    return Underlay.MappingType.SOURCE == mappingType ? sourceMapping : indexMapping;
  }

  public AuxiliaryData cloneWithoutMappings() {
    return new AuxiliaryData(name, List.copyOf(fields));
  }
}
