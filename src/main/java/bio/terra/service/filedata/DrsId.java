package bio.terra.service.filedata;

import bio.terra.service.filedata.exception.InvalidDrsIdException;
import com.google.auto.value.AutoValue;

import java.net.URI;
import java.net.URISyntaxException;

@AutoValue
public abstract class DrsId {

    public abstract String getDnsname();

    public abstract String getVersion();

    public abstract String getSnapshotId();

    public abstract String getFsObjectId();

    // Pass in the correct name of this repo's dnsname
    public static Builder builder() {
        return new AutoValue_DrsId.Builder();
    }

    public String toDrsObjectId() {
        String vv = getVersion() == null ? "v1" : getVersion();
        return vv + "_" + getSnapshotId() + "_" + getFsObjectId();
    }

    public String toDrsUri() {
        String path = "/" + toDrsObjectId();
        try {
            URI uri = new URI("drs", getDnsname(), path, null, null);
            return uri.toString();
        } catch (URISyntaxException ex) {
            throw new InvalidDrsIdException("Invalid DRS syntax", ex);
        }
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder dnsname(String dnsname);

        public abstract Builder version(String version);

        public abstract Builder snapshotId(String snapshotId);

        public abstract Builder fsObjectId(String fsObjectId);

        public abstract DrsId build();
    }
}
