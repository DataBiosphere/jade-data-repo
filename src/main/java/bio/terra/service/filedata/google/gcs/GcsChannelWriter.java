package bio.terra.service.filedata.google.gcs;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** Given blob ingredients and a storage object, make a writer */
public class GcsChannelWriter implements Closeable {
  private final WriteChannel writer;

  public GcsChannelWriter(Storage storage, String bucket, String targetPath) {
    BlobInfo targetBlobInfo = BlobInfo.newBuilder(BlobId.of(bucket, targetPath)).build();

    this.writer = storage.writer(targetBlobInfo);
  }

  public int writeLine(String line) throws IOException {
    line = line + "\n";
    return writer.write(ByteBuffer.wrap(line.getBytes(StandardCharsets.UTF_8)));
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }
}
