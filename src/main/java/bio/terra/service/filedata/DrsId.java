package bio.terra.service.filedata;

import bio.terra.service.filedata.exception.InvalidDrsIdException;
import java.util.Objects;
import org.apache.commons.lang3.builder.ToStringBuilder;

/*
 * WARNING: if making any changes to this class make sure to notify the #dsp-batch channel. Describe the change and
 * any consequences downstream to DRS clients.
 */
public class DrsId {
  private final String dnsname;
  private final String version;
  private final String snapshotId;
  private final String fsObjectId;
  private final boolean compactId;

  DrsId(String dnsname, String version, String snapshotId, String fsObjectId, boolean compactId) {
    this.dnsname = dnsname;
    this.version = version;
    this.snapshotId = snapshotId;
    this.fsObjectId = fsObjectId;
    this.compactId = compactId;
  }

  public String getDnsname() {
    return dnsname;
  }

  public String getVersion() {
    return version;
  }

  public String getSnapshotId() {
    return snapshotId;
  }

  public String getFsObjectId() {
    return fsObjectId;
  }

  // Pass in the correct name of this repo's dnsname
  public static Builder builder() {
    return new DrsId.Builder();
  }

  public String toDrsObjectId() {
    if (version == null || version.equals("v1")) {
      return "v1_" + snapshotId + "_" + fsObjectId;
    } else if (version.equals("v2")) {
      return "v2_" + fsObjectId;
    } else {
      throw new InvalidDrsIdException("Unrecognized DRS Object ID version: %s".formatted(version));
    }
  }

  public String toDrsUri() {
    if (compactId) {
      return "drs://" + dnsname + ":" + toDrsObjectId();
    } else {
      return "drs://" + dnsname + "/" + toDrsObjectId();
    }
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("dnsname", dnsname)
        .append("version", version)
        .append("snapshotId", snapshotId)
        .append("fsObjectId", fsObjectId)
        .append("compactId", compactId)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DrsId snapshotRequestAssetModel = (DrsId) o;
    return Objects.equals(this.dnsname, snapshotRequestAssetModel.dnsname)
        && Objects.equals(this.version, snapshotRequestAssetModel.version)
        && Objects.equals(this.snapshotId, snapshotRequestAssetModel.snapshotId)
        && Objects.equals(this.fsObjectId, snapshotRequestAssetModel.fsObjectId)
        && Objects.equals(this.compactId, snapshotRequestAssetModel.compactId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dnsname, version, snapshotId, fsObjectId, compactId);
  }

  public static class Builder {
    private String dnsname;
    private String version;
    private String snapshotId;
    private String fsObjectId;
    private boolean compactId;

    public Builder dnsname(String dnsname) {
      this.dnsname = dnsname;
      return this;
    }

    public Builder version(String version) {
      this.version = version;
      return this;
    }

    public Builder snapshotId(String snapshotId) {
      this.snapshotId = snapshotId;
      return this;
    }

    public Builder fsObjectId(String fsObjectId) {
      this.fsObjectId = fsObjectId;
      return this;
    }

    public Builder compactId(boolean compactId) {
      this.compactId = compactId;
      return this;
    }

    public DrsId build() {
      return new DrsId(dnsname, version, snapshotId, fsObjectId, compactId);
    }
  }
}
