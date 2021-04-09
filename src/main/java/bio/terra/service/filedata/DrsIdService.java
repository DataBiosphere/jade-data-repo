package bio.terra.service.filedata;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.service.filedata.exception.InvalidDrsIdException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.URI;

/*
 * WARNING: if making any changes to this class make sure to notify the #dsp-batch channel. Describe the change and
 * any consequences downstream to DRS clients.
 */
/**
 * This class provides DRS id parsing and building methods
 */
@Component
public class DrsIdService {

    private final String datarepoDnsName;

    @Autowired
    public DrsIdService(ApplicationConfiguration applicationConfiguration) {
        this.datarepoDnsName = applicationConfiguration.getDnsName();
    }

    public String toDrsUri(String snapshotId, String fsObjectId) {
        return fromParts(snapshotId, fsObjectId).toDrsUri();
    }

    public String toDrsObjectId(String snapshotId, String fsObjectId) {
        return fromParts(snapshotId, fsObjectId).toDrsObjectId();
    }

    public DrsId makeDrsId(FSItem fsObject, String snapshotId) {
        return fromParts(
            snapshotId,
            fsObject.getFileId().toString());
    }

    private DrsId fromParts(String snapshotId, String fsObjectId) {
        return DrsId.builder()
            .dnsname(datarepoDnsName)
            .version("v1")
            .snapshotId(snapshotId)
            .fsObjectId(fsObjectId)
            .build();
    }

    public DrsId fromUri(String drsuri) {
        URI uri = URI.create(drsuri);
        if (!StringUtils.equals(uri.getScheme(), "drs") ||
            uri.getAuthority() == null ||
            !StringUtils.startsWith(uri.getPath(), "/")) {
            throw new InvalidDrsIdException("Invalid DRS URI '" + drsuri + "'");
        }

        String objectId = StringUtils.remove(uri.getPath(), '/');
        return parseObjectId(objectId).dnsname(uri.getAuthority()).build();
    }

    public DrsId fromObjectId(String drsObjectId) {
        return parseObjectId(drsObjectId).build();
    }

    private DrsId.Builder parseObjectId(String objectId) {
        // The format is v1_<snapshotid>_<fsobjectid>
        String[] idParts = StringUtils.split(objectId, '_');
        if (idParts.length != 3 || !StringUtils.equals(idParts[0], "v1")) {
            throw new InvalidDrsIdException("Invalid DRS object id '" + objectId + "'");
        }

        return DrsId.builder()
            .dnsname(datarepoDnsName)
            .version(idParts[0])
            .snapshotId(idParts[1])
            .fsObjectId(idParts[2]);
    }
}
