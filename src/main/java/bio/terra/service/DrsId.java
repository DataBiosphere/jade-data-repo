package bio.terra.service;

import bio.terra.service.exception.InvalidDrsIdException;

import java.net.URI;
import java.net.URISyntaxException;

public class DrsId {
    private final String dnsname;
    private final String version;
    private final String studyId;
    private final String datasetId;
    private final String fsObjectId;

    public DrsId(String dnsname, String version, String studyId, String datasetId, String fsObjectId) {
        this.dnsname = dnsname;
        this.version = version;
        this.studyId = studyId;
        this.datasetId = datasetId;
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

    public String getDatasetId() {
        return datasetId;
    }

    public String getFsObjectId() {
        return fsObjectId;
    }

    // Pass in the correct name of this repo's dnsname
    public static Builder builder() {
        return new DrsId.Builder();
    }

    @Override
    public String toString() {
        String objectId = version + "_" + studyId + "_" + datasetId + "_" + fsObjectId;
        try {
            URI uri = new URI("uri", dnsname, objectId, null, null);
            return uri.toString();
        } catch (URISyntaxException ex) {
            throw new InvalidDrsIdException("Invalid DRS syntax", ex);
        }
    }

    public static class Builder {
        private String dnsname;
        private String version;
        private String studyId;
        private String datasetId;
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

        public Builder datasetId(String datasetId) {
            this.datasetId = datasetId;
            return this;
        }

        public Builder fsObjectId(String fsObjectId) {
            this.fsObjectId = fsObjectId;
            return this;
        }

        public DrsId build() {
            return new DrsId(dnsname, version, studyId, datasetId, fsObjectId);
        }
    }

}
