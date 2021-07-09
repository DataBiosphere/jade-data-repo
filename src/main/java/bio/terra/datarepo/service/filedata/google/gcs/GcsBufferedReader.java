package bio.terra.datarepo.service.filedata.google.gcs;

import com.google.cloud.storage.Storage;
import java.io.BufferedReader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;

/** Given a gs path and a storage object, return a buffered reader for the blob */
public class GcsBufferedReader extends BufferedReader {
  public GcsBufferedReader(Storage storage, String projectId, String gspath) {
    super(
        Channels.newReader(
            GcsPdao.getBlobFromGsPath(storage, gspath, projectId).reader(),
            StandardCharsets.UTF_8.name()));
  }
}
