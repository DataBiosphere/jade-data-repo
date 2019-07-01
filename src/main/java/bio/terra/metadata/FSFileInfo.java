package bio.terra.metadata;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

// POJO of the collected information of a file returned from the bucket to be stored in the filesystem.
public class FSFileInfo {
    private String objectId;
    private String datasetId;
    private String createdDate;
    private String gspath;
    private String checksumCrc32c;
    private String checksumMd5;
    private Long size;
    private String flightId;

    public String getObjectId() {
        return objectId;
    }

    public FSFileInfo objectId(String objectId) {
        this.objectId = objectId;
        return this;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public FSFileInfo datasetId(String datasetId) {
        this.datasetId = datasetId;
        return this;
    }

    public String getCreatedDate() {
        return createdDate;
    }

    public FSFileInfo createdDate(String createdDate) {
        this.createdDate = createdDate;
        return this;
    }

    public String getGspath() {
        return gspath;
    }

    public FSFileInfo gspath(String gspath) {
        this.gspath = gspath;
        return this;
    }

    public String getChecksumCrc32c() {
        return checksumCrc32c;
    }

    public FSFileInfo checksumCrc32c(String checksumCrc32c) {
        this.checksumCrc32c = checksumCrc32c;
        return this;
    }

    public String getChecksumMd5() {
        return checksumMd5;
    }

    public FSFileInfo checksumMd5(String checksumMd5) {
        this.checksumMd5 = checksumMd5;
        return this;
    }

    public Long getSize() {
        return size;
    }

    public FSFileInfo size(Long size) {
        this.size = size;
        return this;
    }

    public String getFlightId() {
        return flightId;
    }

    public FSFileInfo flightId(String flightId) {
        this.flightId = flightId;
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
            .append("objectId", objectId)
            .append("datasetId", datasetId)
            .append("gspath", gspath)
            .append("checksumCrc32c", checksumCrc32c)
            .append("checksumMd5", checksumMd5)
            .append("size", size)
            .append("flightId", flightId)
            .toString();
    }
}
