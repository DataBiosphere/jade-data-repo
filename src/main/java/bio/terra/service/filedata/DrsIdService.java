package bio.terra.service.filedata;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.service.filedata.exception.InvalidDrsIdException;
import java.net.URI;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/*
 * WARNING: if making any changes to this class make sure to notify the #dsp-batch channel. Describe the change and
 * any consequences downstream to DRS clients.
 */
/** This class provides DRS id parsing and building methods */
@Component
public class DrsIdService {

  private final String datarepoDnsName;

  @Autowired
  public DrsIdService(ApplicationConfiguration applicationConfiguration) {
    this.datarepoDnsName = applicationConfiguration.getDnsName();
  }

  public DrsId makeDrsId(FSItem fsObject, String snapshotId) {
    return fromParts(snapshotId, fsObject.getFileId().toString());
  }

  public DrsId makeDrsId(FSItem fsObject) {
    return makeDrsId(fsObject.getFileId().toString());
  }

  public DrsId makeDrsId(String fileId) {
    return fromParts(null, fileId);
  }

  private DrsId fromParts(String snapshotId, String fsObjectId) {
    if (snapshotId != null) {
      return DrsId.builder()
          .dnsname(datarepoDnsName)
          .version("v1")
          .snapshotId(snapshotId)
          .fsObjectId(fsObjectId)
          .build();
    } else {
      return DrsId.builder().dnsname(datarepoDnsName).version("v2").fsObjectId(fsObjectId).build();
    }
  }

  public static DrsId fromUri(String drsUri) {
    URI uri = URI.create(drsUri);
    if (!StringUtils.equals(uri.getScheme(), "drs")
        || uri.getAuthority() == null
        // This is invalid for standard DRS URIs
        || (!StringUtils.startsWith(uri.getPath(), "/") && !StringUtils.isEmpty(uri.getPath()))
        // This is invalid for compact IDs
        || (StringUtils.isEmpty(uri.getPath()) && !uri.getAuthority().contains(":"))) {
      throw new InvalidDrsIdException("Invalid DRS URI '" + drsUri + "'");
    }

    // According to the DRS spec, port numbers can't be specified so just the presence of `:` in the
    // authority is an indication of a compact id being used
    boolean compactId = uri.getAuthority().contains(":");
    String datarepoDnsName;
    String objectId;
    if (compactId) {
      String[] parts = uri.getAuthority().split(":");
      datarepoDnsName = parts[0];
      objectId = parts[1];
    } else {
      datarepoDnsName = uri.getAuthority();
      objectId = StringUtils.remove(uri.getPath(), '/');
    }

    return parseObjectId(datarepoDnsName, objectId).compactId(compactId).build();
  }

  public DrsId fromObjectId(String drsObjectId) {
    return parseObjectId(drsObjectId).build();
  }

  public boolean isValidObjectId(String drsObjectId) {
    try {
      parseObjectId(drsObjectId);
      return true;
    } catch (InvalidDrsIdException e) {
      return false;
    }
  }

  private DrsId.Builder parseObjectId(String objectId) {
    return parseObjectId(datarepoDnsName, objectId);
  }

  private static DrsId.Builder parseObjectId(String datarepoDnsName, String objectId) {
    // The format is v1_<snapshotid>_<fsobjectid> or v2_<fsobjectid>
    String[] idParts = StringUtils.split(objectId, '_');
    if (idParts.length == 3 && StringUtils.equals(idParts[0], "v1")) {
      return DrsId.builder()
          .dnsname(datarepoDnsName)
          .version(idParts[0])
          .snapshotId(idParts[1])
          .fsObjectId(idParts[2]);
    } else if (idParts.length == 2 && StringUtils.equals(idParts[0], "v2")) {
      return DrsId.builder().dnsname(datarepoDnsName).version(idParts[0]).fsObjectId(idParts[1]);
    } else {
      throw new InvalidDrsIdException("Invalid DRS object id '" + objectId + "'");
    }
  }
}
