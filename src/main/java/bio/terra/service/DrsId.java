package bio.terra.service;

import bio.terra.service.exception.InvalidDrsIdException;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.net.URI;
import java.net.URISyntaxException;

public class DrsId {
    private final String dnsname;
    private final String version;
    private final String studyId;
    private final String dataSnapshotId;
    private final String fsObjectId;

    public DrsId(String dnsname, String version, String studyId, String dataSnapshotId, String fsObjectId) {
        this.dnsname = dnsname;
        this.version = version;
        this.studyId = studyId;
        this.dataSnapshotId = dataSnapshotId;
        this.fsObjectId = fsObjectId;
    }

    public String getDnsname() {
        return dnsname;
    }

    public String getVersion() {
        return version;
    }

    public String getStudyId() {
        return studyId;
    }

    public String getDataSnapshotId() {
        return dataSnapshotId;
    }

    public String getFsObjectId() {
        return fsObjectId;
    }

    // Pass in the correct name of this repo's dnsname
    public static Builder builder() {
        return new DrsId.Builder();
    }

    public String toDrsObjectId() {
        String vv = version == null ? "v1" : version;
        return vv + "_" + studyId + "_" + dataSnapshotId + "_" + fsObjectId;
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
            .append("studyId", studyId)
            .append("dataSnapshotId", dataSnapshotId)
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
        if (studyId != null ? !studyId.equals(drsId.studyId) : drsId.studyId != null) return false;
        if (dataSnapshotId != null ? !dataSnapshotId.equals(drsId.dataSnapshotId) : drsId.dataSnapshotId != null)
            return false;
        return fsObjectId != null ? fsObjectId.equals(drsId.fsObjectId) : drsId.fsObjectId == null;
    }

    @Override
    public int hashCode() {
        int result = dnsname != null ? dnsname.hashCode() : 0;
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (studyId != null ? studyId.hashCode() : 0);
        result = 31 * result + (dataSnapshotId != null ? dataSnapshotId.hashCode() : 0);
        result = 31 * result + (fsObjectId != null ? fsObjectId.hashCode() : 0);
        return result;
    }

    public static class Builder {
        private String dnsname;
        private String version;
        private String studyId;
        private String dataSnapshotId;
        private String fsObjectId;

        public Builder dnsname(String dnsname) {
            this.dnsname = dnsname;
            return this;
        }

        public Builder version(String version) {
            this.version = version;
            return this;
        }

        public Builder studyId(String studyId) {
            this.studyId = studyId;
            return this;
        }

        public Builder dataSnapshotId(String dataSnapshotId) {
            this.dataSnapshotId = dataSnapshotId;
            return this;
        }

        public Builder fsObjectId(String fsObjectId) {
            this.fsObjectId = fsObjectId;
            return this;
        }

        public DrsId build() {
            return new DrsId(dnsname, version, studyId, dataSnapshotId, fsObjectId);
        }
    }

}
