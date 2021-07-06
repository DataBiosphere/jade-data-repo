package bio.terra.service.load;

import java.util.UUID;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class Load {
  private UUID id;
  private String loadTag;
  private boolean locked;
  private String lockingFlightId;

  public UUID getId() {
    return id;
  }

  public Load id(UUID id) {
    this.id = id;
    return this;
  }

  public String getLoadTag() {
    return loadTag;
  }

  public Load loadTag(String loadTag) {
    this.loadTag = loadTag;
    return this;
  }

  public boolean isLocked() {
    return locked;
  }

  public Load locked(boolean locked) {
    this.locked = locked;
    return this;
  }

  public String getLockingFlightId() {
    return lockingFlightId;
  }

  public Load lockingFlightId(String lockingFlightId) {
    this.lockingFlightId = lockingFlightId;
    return this;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
        .append("id", id)
        .append("loadTag", loadTag)
        .append("locked", locked)
        .append("lockingFlightId", lockingFlightId)
        .toString();
  }
}
