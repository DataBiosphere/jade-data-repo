package bio.terra.tanagra.serialization;

import bio.terra.tanagra.underlay.HierarchyMapping;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

/**
 * External representation of a mapping between a hierarchy and the underlying data.
 *
 * <p>This is a POJO class intended for serialization. This JSON format is user-facing.
 */
@JsonDeserialize(builder = UFHierarchyMapping.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UFHierarchyMapping {
  private final UFAuxiliaryDataMapping childParent;
  private final UFAuxiliaryDataMapping rootNodesFilter;
  private final UFAuxiliaryDataMapping ancestorDescendant;
  private final UFAuxiliaryDataMapping pathNumChildren;
  private final int maxHierarchyDepth;

  public UFHierarchyMapping(HierarchyMapping hierarchyMapping) {
    this.childParent = new UFAuxiliaryDataMapping(hierarchyMapping.getChildParent());
    this.rootNodesFilter =
        hierarchyMapping.hasRootNodesFilter()
            ? new UFAuxiliaryDataMapping(hierarchyMapping.getRootNodesFilter())
            : null;
    this.ancestorDescendant =
        hierarchyMapping.hasAncestorDescendant()
            ? new UFAuxiliaryDataMapping(hierarchyMapping.getAncestorDescendant())
            : null;
    this.pathNumChildren =
        hierarchyMapping.hasPathNumChildren()
            ? new UFAuxiliaryDataMapping(hierarchyMapping.getPathNumChildren())
            : null;
    this.maxHierarchyDepth = hierarchyMapping.getMaxHierarchyDepth();
  }

  private UFHierarchyMapping(Builder builder) {
    this.childParent = builder.childParent;
    this.rootNodesFilter = builder.rootNodesFilter;
    this.ancestorDescendant = builder.ancestorDescendant;
    this.pathNumChildren = builder.pathNumChildren;
    this.maxHierarchyDepth = builder.maxHierarchyDepth;
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder {
    private UFAuxiliaryDataMapping childParent;
    private UFAuxiliaryDataMapping rootNodesFilter;
    private UFAuxiliaryDataMapping ancestorDescendant;
    private UFAuxiliaryDataMapping pathNumChildren;
    private int maxHierarchyDepth;

    public Builder childParent(UFAuxiliaryDataMapping childParent) {
      this.childParent = childParent;
      return this;
    }

    public Builder rootNodesFilter(UFAuxiliaryDataMapping rootNodesFilter) {
      this.rootNodesFilter = rootNodesFilter;
      return this;
    }

    public Builder ancestorDescendant(UFAuxiliaryDataMapping ancestorDescendant) {
      this.ancestorDescendant = ancestorDescendant;
      return this;
    }

    public Builder pathNumChildren(UFAuxiliaryDataMapping pathNumChildren) {
      this.pathNumChildren = pathNumChildren;
      return this;
    }

    public Builder maxHierarchyDepth(int maxHierarchyDepth) {
      this.maxHierarchyDepth = maxHierarchyDepth;
      return this;
    }

    /** Call the private constructor. */
    public UFHierarchyMapping build() {
      return new UFHierarchyMapping(this);
    }
  }

  public UFAuxiliaryDataMapping getChildParent() {
    return childParent;
  }

  public UFAuxiliaryDataMapping getRootNodesFilter() {
    return rootNodesFilter;
  }

  public UFAuxiliaryDataMapping getAncestorDescendant() {
    return ancestorDescendant;
  }

  public UFAuxiliaryDataMapping getPathNumChildren() {
    return pathNumChildren;
  }

  public int getMaxHierarchyDepth() {
    return maxHierarchyDepth;
  }
}
