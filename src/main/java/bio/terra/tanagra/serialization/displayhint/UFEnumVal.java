package bio.terra.tanagra.serialization.displayhint;

import bio.terra.tanagra.underlay.displayhint.EnumVal;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

/**
 * External representation of an enum value/display + count pair.
 *
 * <p>This is a POJO class intended for serialization. This JSON format is user-facing.
 */
@JsonDeserialize(builder = UFEnumVal.Builder.class)
public class UFEnumVal {
  private final UFValueDisplay enumVal;
  private final long count;

  public UFEnumVal(EnumVal enumVal) {
    this.enumVal = new UFValueDisplay(enumVal.getValueDisplay());
    this.count = enumVal.getCount();
  }

  private UFEnumVal(Builder builder) {
    this.enumVal = builder.enumVal;
    this.count = builder.count;
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder {
    private UFValueDisplay enumVal;
    private long count;

    public Builder enumVal(UFValueDisplay enumVal) {
      this.enumVal = enumVal;
      return this;
    }

    public Builder count(long count) {
      this.count = count;
      return this;
    }

    public UFEnumVal build() {
      return new UFEnumVal(this);
    }
  }

  public UFValueDisplay getEnumVal() {
    return enumVal;
  }

  public long getCount() {
    return count;
  }
}
