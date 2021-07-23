package bio.terra.service.filedata.google.gcs;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import java.io.BufferedReader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;

/** Given a gs path and a storage object, return a buffered reader for the blob */
public class GcsBufferedReader extends BufferedReader {

  public GcsBufferedReader(Storage storage, String projectId, BlobInfo blobInfo) {
    super(
        Channels.newReader(
            storage
                .get(blobInfo.getBlobId(), Storage.BlobGetOption.userProject(projectId))
                .reader(Blob.BlobSourceOption.userProject(projectId)),
            StandardCharsets.UTF_8.name()));
  }

  public GcsBufferedReader(Storage storage, String projectId, String gsPath) {
    this(storage, projectId, GcsPdao.getBlobFromGsPath(storage, gsPath, projectId));
  }
}
