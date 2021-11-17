package bio.terra.common;

import com.azure.storage.blob.BlobUrlParts;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

/** Helper test methods for working directly with parquet files */
public class ParquetUtils {

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

    List<Map<String, String>> results = new ArrayList<>();

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
    try (ParquetReader<Group> pReader =
        ParquetReader.builder(new GroupReadSupport(), new Path(uri)).withConf(config).build()) {
      Group record;
      while ((record = pReader.read()) != null) {
        final Group r = record;
        results.add(
            IntStream.range(0, r.getType().getFields().size())
                .mapToObj(i -> Pair.of(r.getType().getFieldName(i), r.getValueToString(i, 0)))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
      }
    } catch (IOException ex) {
      throw new RuntimeException("Error reading parquet data data", ex);
    }
    return results;
  }
}
