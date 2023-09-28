package bio.terra.tanagra.underlay;

import bio.terra.tanagra.underlay.displayhint.EnumVal;
import java.util.List;

public abstract class DisplayHint {
  private final Type type;

  protected DisplayHint(Type type) {
    this.type = type;
  }

  public enum Type {
    ENUM,
    RANGE
  }

  public Type getType() {
    return type;
  }

  public List<EnumVal> getEnumValsList() {
    return List.of();
  }
}
