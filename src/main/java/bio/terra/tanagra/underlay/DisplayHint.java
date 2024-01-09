package bio.terra.tanagra.underlay;

import bio.terra.tanagra.serialization.UFDisplayHint;

public abstract class DisplayHint {
  public enum Type {
    ENUM,
    RANGE
  }

  public abstract Type getType();

  public abstract UFDisplayHint serialize();
}
