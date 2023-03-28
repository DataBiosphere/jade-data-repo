package bio.terra.tanagra.underlay;

import bio.terra.tanagra.underlay.hierarchyfield.IsMember;
import bio.terra.tanagra.underlay.hierarchyfield.IsRoot;
import bio.terra.tanagra.underlay.hierarchyfield.NumChildren;
import bio.terra.tanagra.underlay.hierarchyfield.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Hierarchy {
  private final String name;
  private final HierarchyMapping sourceMapping;
  private final HierarchyMapping indexMapping;
  private final Map<HierarchyField.Type, HierarchyField> fields =
      Map.ofEntries(
          Map.entry(HierarchyField.Type.IS_MEMBER, new IsMember()),
          Map.entry(HierarchyField.Type.IS_ROOT, new IsRoot()),
          Map.entry(HierarchyField.Type.PATH, new Path()),
          Map.entry(HierarchyField.Type.NUM_CHILDREN, new NumChildren()));
  private Entity entity;

  public Hierarchy(String name, HierarchyMapping sourceMapping, HierarchyMapping indexMapping) {
    this.name = name;
    this.sourceMapping = sourceMapping;
    this.indexMapping = indexMapping;
  }

  public void initialize(Entity entity) {
    this.entity = entity;
    sourceMapping.initialize(this);
    indexMapping.initialize(this);
    fields.values().stream().forEach(field -> field.initialize(this));
  }

  public String getName() {
    return name;
  }

  public HierarchyMapping getMapping(Underlay.MappingType mappingType) {
    return Underlay.MappingType.SOURCE.equals(mappingType) ? sourceMapping : indexMapping;
  }

  public Entity getEntity() {
    return entity;
  }

  public List<HierarchyField> getFields() {
    return Collections.unmodifiableList(fields.values().stream().collect(Collectors.toList()));
  }

  public HierarchyField getField(HierarchyField.Type type) {
    return fields.get(type);
  }
}
