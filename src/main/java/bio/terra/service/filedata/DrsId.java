package bio.terra.service.filedata;

import bio.terra.service.filedata.exception.InvalidDrsIdException;
import java.net.URI;
import java.net.URISyntaxException;
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

  public DrsId(String dnsname, String version, String snapshotId, String fsObjectId) {
    this.dnsname = dnsname;
    this.version = version;
    this.snapshotId = snapshotId;
    this.fsObjectId = fsObjectId;
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
    String path = "/" + toDrsObjectId();
    try {
      URI uri = new URI("drs", dnsname, path, null, null);
      return uri.toString();
    } catch (URISyntaxException ex) {
      throw new InvalidDrsIdException("Invalid DRS syntax", ex);
    }
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("dnsname", dnsname)
        .append("version", version)
        .append("snapshotId", snapshotId)
        .append("fsObjectId", fsObjectId)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DrsId drsId = (DrsId) o;

    if (dnsname != null ? !dnsname.equals(drsId.dnsname) : drsId.dnsname != null) return false;
    if (version != null ? !version.equals(drsId.version) : drsId.version != null) return false;
    if (snapshotId != null ? !snapshotId.equals(drsId.snapshotId) : drsId.snapshotId != null)
      return false;
    return fsObjectId != null ? fsObjectId.equals(drsId.fsObjectId) : drsId.fsObjectId == null;
  }

  @Override
  public int hashCode() {
    int result = dnsname != null ? dnsname.hashCode() : 0;
    result = 31 * result + (version != null ? version.hashCode() : 0);
    result = 31 * result + (snapshotId != null ? snapshotId.hashCode() : 0);
    result = 31 * result + (fsObjectId != null ? fsObjectId.hashCode() : 0);
    return result;
  }

  public static class Builder {
    private String dnsname;
    private String version;
    private String snapshotId;
    private String fsObjectId;

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

    public DrsId build() {
      return new DrsId(dnsname, version, snapshotId, fsObjectId);
    }
  }
}
