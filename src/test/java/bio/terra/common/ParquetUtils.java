package bio.terra.common;

import bio.terra.service.filedata.google.gcs.GcsPdao;
import com.azure.storage.blob.BlobUrlParts;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.LogicalTypeAnnotation;

/** Helper test methods for working directly with parquet files */
public class ParquetUtils {

  public static List<Map<String, Object>> readGcsParquetRecords(
      GcsPdao gcsPdao, final String url, String googleProjectId) {
    Configuration config = new Configuration();
    byte[] contents = gcsPdao.getBlobBytes(url, googleProjectId);
    try {
      java.nio.file.Path tempParquetFile = Files.createTempFile("localGcs", ".parquet");
      Files.write(tempParquetFile, contents);
      URI uri = tempParquetFile.toUri();
      return readParquetRecordsFromUri(config, uri);
    } catch (IOException e) {
      throw new RuntimeException("Could not write " + url + " to local temp file", e);
    }
  }
  /**
   * Given a signed Azure URL, this method will read through the parquet data specified as list of
   * maps that are keyed on column name and whose values are string representations of the parquet
   * field values
   *
   * @param url The Azure URL to read from with a sas token included
   * @return A java representation of the parquet record data
   */
  public static List<Map<String, String>> readParquetRecords(final String url) {
    BlobUrlParts blobUrlParts = BlobUrlParts.parse(url);
    Configuration config = new Configuration();
    config.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem");
    config.set(
        "fs.azure.sas."
            + blobUrlParts.getBlobContainerName()
            + "."
            + blobUrlParts.getAccountName()
            + ".blob.core.windows.net",
        blobUrlParts.getCommonSasQueryParameters().encode());

    URI uri;
    try {
      uri =
          new URI(
              "wasbs://"
                  + blobUrlParts.getBlobContainerName()
                  + "@"
                  + blobUrlParts.getAccountName()
                  + ".blob.core.windows.net/"
                  + blobUrlParts.getBlobName());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    return readParquetRecordsFromUri(config, uri).stream()
        .map(
            m ->
                m.entrySet().stream()
                    .map(e -> Map.entry(e.getKey(), e.getValue().toString()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
        .collect(Collectors.toList());
  }

  private static List<Map<String, Object>> readParquetRecordsFromUri(
      Configuration config, URI uri) {
    List<Map<String, Object>> results = new ArrayList<>();
    try (ParquetReader<Group> pReader =
        ParquetReader.builder(new GroupReadSupport(), new Path(uri)).withConf(config).build()) {
      Group record;
      while ((record = pReader.read()) != null) {
        final Group r = record;
        Map<String, Object> resultRecord = new HashMap<>();
        // Unfortunately, we can't use collectors with null values
        IntStream.range(0, r.getType().getFields().size())
            .forEach(
                i -> {
                  if (r.getType()
                      .getType(i)
                      .getLogicalTypeAnnotation()
                      .equals(LogicalTypeAnnotation.uuidType())) {
                    resultRecord.put(
                        r.getType().getFieldName(i),
                        getUUIDFromByteArray(r.getBinary(i, 0).getBytes()));
                  } else if (r.getType()
                      .getType(i)
                      .getLogicalTypeAnnotation()
                      .equals(LogicalTypeAnnotation.listType())) {
                    resultRecord.put(r.getType().getFieldName(i), readListValue(r, i));
                  } else {
                    resultRecord.put(r.getType().getFieldName(i), readFieldValue(r, i));
                  }
                });

        results.add(resultRecord);
      }
    } catch (IOException ex) {
      throw new RuntimeException("Error reading parquet data data", ex);
    }
    return results;
  }

  private static String readFieldValue(final Group record, final int index) {
    try {
      return record.getValueToString(index, 0);
    } catch (RuntimeException e) {
      // No value at position index or empty array
      return null;
    }
  }

  private static List<String> readListValue(final Group record, final int index) {
    Group group = record.getGroup(index, 0);
    int count = group.getFieldRepetitionCount(0);
    List<String> elements = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      elements.add(group.getGroup(0, i).getValueToString(0, 0));
    }
    return elements;
  }

  public static String getUUIDFromByteArray(byte[] bytes) {
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    long high = bb.getLong();
    long low = bb.getLong();
    UUID id = new UUID(high, low);
    return id.toString();
  }
}
