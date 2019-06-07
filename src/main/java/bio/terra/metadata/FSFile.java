package bio.terra.metadata;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * This provides the base class for all file system objects. There are three specializations:
 * <ul>
 *     <li>FSFile - describes a file</li>
 *     <li>FSDir - describes a directory</li>
 *     <li>FSEnumDir - describes a directory and its contents</li>
 * </ul>
 *
 */
public class FSFile extends FSObjectBase {
    private String gspath;
    private String checksumCrc32c;
    private String checksumMd5;
    private String mimeType;
    private String flightId;

    public String getGspath() {
        return gspath;
    }

    public FSFile gspath(String gspath) {
        this.gspath = gspath;
        return this;
    }

    public String getChecksumCrc32c() {
        return checksumCrc32c;
    }

    public FSFile checksumCrc32c(String checksumCrc32c) {
        this.checksumCrc32c = checksumCrc32c;
        return this;
    }

    public String getChecksumMd5() {
        return checksumMd5;
    }

    public FSFile checksumMd5(String checksumMd5) {
        this.checksumMd5 = checksumMd5;
        return this;
    }

    public String getMimeType() {
        return mimeType;
    }

    public FSFile mimeType(String mimeType) {
        this.mimeType = mimeType;
        return this;
    }

    public String getFlightId() {
        return flightId;
    }

    public FSFile flightId(String flightId) {
        this.flightId = flightId;
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
            .append("gspath", gspath)
            .append("checksumCrc32c", checksumCrc32c)
            .append("checksumMd5", checksumMd5)
            .append("mimeType", mimeType)
            .append("flightId", flightId)
            .toString();
    }
}
