package bio.terra.service.parquet;

import bio.terra.model.TableDataType;
import com.azure.storage.blob.BlobUrlParts;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;

public class ParquetReaderWriterWithAvro {

  private ParquetReaderWriterWithAvro() {}

  public static List<GenericData.Record> readFromParquet(InputFile inputFile) throws IOException {
    List<GenericData.Record> records = new ArrayList<>();
    try (ParquetReader<GenericData.Record> reader =
        AvroParquetReader.<GenericData.Record>builder(inputFile).build()) {
      GenericData.Record record;

      while ((record = reader.read()) != null) {
        records.add(record);
      }
      return records;
    }
  }

  public static List<GenericData.Record> readFromParquet(Path filePath) throws IOException {
    var config = new Configuration();
    config.set("parquet.avro.readInt96AsFixed", "true");
    InputFile inputFile = HadoopInputFile.fromPath(filePath, config);
    return readFromParquet(inputFile);
  }

  public static void readWriteToParquet(
      InputFile inputFile,
      List<String> columnNames,
      Map<String, TableDataType> columnDataTypeMap,
      OutputFile fileToWrite,
      Schema schema,
      Configuration config)
      throws IOException {
    try (ParquetWriter<GenericData.Record> writer =
        AvroParquetWriter.<GenericData.Record>builder(fileToWrite)
            .withSchema(schema)
            .withConf(config)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .build()) {
      try (ParquetReader<GenericData.Record> reader =
          AvroParquetReader.<GenericData.Record>builder(inputFile).build()) {
        GenericData.Record record;

        while ((record = reader.read()) != null) {

          var newRecord = new GenericData.Record(schema);
          for (var column : columnNames) {
            switch (columnDataTypeMap.get(column)) {
              case DATETIME, TIMESTAMP:
                // Convert from fixed length binary to a long representing microseconds since epoch
                GenericData.Fixed dtFixed = (GenericData.Fixed) record.get(column);
                if (dtFixed == null) {
                  newRecord.put(column, null);
                } else {
                  var bytes = dtFixed.bytes();
                  ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
                  long timeOfDayNanos = bb.getLong();
                  var julianDay = bb.getInt();
                  // Given timeOfDayNanos and julianDay, convert to microseconds since epoch
                  Long microSeconds =
                      (long) (julianDay - 2440588) * 86400 * 1000000 + timeOfDayNanos / 1000;
                  newRecord.put(column, microSeconds);
                }
                break;
              default:
                newRecord.put(column, record.get(column));
            }
          }
          writer.write(newRecord);
        }
      }
    }
  }

  public static void simpleWriteToParquet(
      List<GenericData.Record> recordsToWrite,
      OutputFile fileToWrite,
      Schema schema,
      Configuration config)
      throws IOException {
    try (ParquetWriter<GenericData.Record> writer =
        AvroParquetWriter.<GenericData.Record>builder(fileToWrite)
            .withSchema(schema)
            .withConf(config)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .build()) {

      for (GenericData.Record record : recordsToWrite) {
        writer.write(record);
      }
    }
  }

  // Pulled this code from ParquetUtils.java
  // In a real implementation, we would probably want to do something a little more direct in the
  // flight rather than producing the signed url and then breaking it back down
  public static Configuration getConfigFromSignedUri(String signedUrl) {
    BlobUrlParts blobUrlParts = BlobUrlParts.parse(signedUrl);
    Configuration config = new Configuration();
    config.set("parquet.avro.readInt96AsFixed", "true");
    config.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem");
    config.set(
        "fs.azure.sas."
            + blobUrlParts.getBlobContainerName()
            + "."
            + blobUrlParts.getAccountName()
            + ".blob.core.windows.net",
        blobUrlParts.getCommonSasQueryParameters().encode());
    return config;
  }

  public static URI getURIFromSignedUrl(String signedUrl) {
    BlobUrlParts blobUrlParts = BlobUrlParts.parse(signedUrl);
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
    return uri;
  }
}
