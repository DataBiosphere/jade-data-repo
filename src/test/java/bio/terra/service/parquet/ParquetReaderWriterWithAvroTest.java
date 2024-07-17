package bio.terra.service.parquet;

import static bio.terra.service.parquet.ParquetReaderWriterWithAvro.*;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import bio.terra.common.category.Unit;
import bio.terra.model.TableDataType;
import bio.terra.stairway.ShortUUID;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class ParquetReaderWriterWithAvroTest {
  private static final String FOLDER_PATH = "./src/main/java/bio/terra/service/parquet/";
  private static final String OUTPUT_PATH = FOLDER_PATH + "output/";

  @Test
  void readWriteToLocalFile() throws IOException {
    var schema = sampleDataSchema();
    var recordsToWrite = sampleRecords(schema);

    Configuration config = new Configuration();
    config.set("parquet.avro.readInt96AsFixed", "true");
    Path path = randomizedFilePath("localFileWriteTest");
    OutputFile outputFile = HadoopOutputFile.fromPath(path, new Configuration());
    simpleWriteToParquet(recordsToWrite, outputFile, schema, config);

    // Confirm that we can read the same records out of the file
    List<GenericData.Record> records = readFromParquet(path);
    assertSampleData(records);
  }

  @Test
  void readWriteToAzureStorageBlob() throws IOException {
    var schema = sampleDataSchema();
    var recordsToWrite = sampleRecords(schema);

    // Write to azure storage blob
    var signedUrl = buildSignedUrl(ShortUUID.get() + "_datetime.parquet");
    var writeToUri = ParquetReaderWriterWithAvro.getURIFromSignedUrl(signedUrl);
    var config = ParquetReaderWriterWithAvro.getConfigFromSignedUri(signedUrl);
    var path = new Path(writeToUri);
    OutputFile outputFile = HadoopOutputFile.fromPath(path, config);
    simpleWriteToParquet(recordsToWrite, outputFile, schema, config);

    // Read from azure storage blob
    InputFile inputFile = HadoopInputFile.fromPath(path, config);
    List<GenericData.Record> records = readFromParquet(inputFile);
    assertSampleData(records);
  }

  @Test
  void formatDateTimeField() throws IOException {
    Path azureSourceFile = new Path(FOLDER_PATH + "azure_original.parquet");
    var localConfig = new Configuration();
    localConfig.set("parquet.avro.readInt96AsFixed", "true");
    InputFile azureInputFile = HadoopInputFile.fromPath(azureSourceFile, localConfig);

    Schema nullType = Schema.create(Schema.Type.NULL);
    Schema timestampMicroType =
        LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    Schema newSchema =
        SchemaBuilder.record("newFormatSchema")
            .fields()
            .name("variant_id")
            .type()
            .nullable()
            .stringType()
            .noDefault()
            .name("date_time_column")
            .type(Schema.createUnion(nullType, timestampMicroType))
            .noDefault()
            .endRecord();
    var columnNames = List.of("variant_id", "date_time_column");
    var columnDataTypeMap =
        Map.of("variant_id", TableDataType.STRING, "date_time_column", TableDataType.DATETIME);

    // === write to local file ===
    var output = randomizedFilePath("datetime");
    var config = new Configuration();
    OutputFile outputFile = HadoopOutputFile.fromPath(output, config);
    readWriteToParquet(
        azureInputFile, columnNames, columnDataTypeMap, outputFile, newSchema, config);

    List<GenericData.Record> formattedRecords = ParquetReaderWriterWithAvro.readFromParquet(output);
    assertThat(
        "datetime should now be long value representing microseconds since epoch",
        formattedRecords.get(0).get("date_time_column"),
        equalTo(1657022401000000L));
    assertThat(
        "record has correct schema",
        formattedRecords.get(0).getSchema().getField("date_time_column").schema().toString(),
        containsString("timestamp-micros"));
  }

  @Test
  void e2eTestFormatDatetime() throws IOException {
    // == local read ==
    Path azureSourceFile = new Path(FOLDER_PATH + "azure_original.parquet");
    var localConfig = new Configuration();
    localConfig.set("parquet.avro.readInt96AsFixed", "true");
    InputFile azureInputFile = HadoopInputFile.fromPath(azureSourceFile, localConfig);
    //  === generate signed url from snapshotExport ===
    //    var signedUrlFromSnapshotExport = null;
    //    InputFile azureInputFile = buildInputFileFromSignedUrl(signedUrlFromSnapshotExport);

    Schema nullType = Schema.create(Schema.Type.NULL);
    Schema timestampMicroType =
        LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    Schema newSchema =
        SchemaBuilder.record("newFormatSchema")
            .fields()
            .name("variant_id")
            .type()
            .nullable()
            .stringType()
            .noDefault()
            .name("date_time_column")
            .type(Schema.createUnion(nullType, timestampMicroType))
            .noDefault()
            .endRecord();
    var columnNames = List.of("variant_id", "date_time_column");
    var columnDataTypeMap =
        Map.of("variant_id", TableDataType.STRING, "date_time_column", TableDataType.DATETIME);

    // Write to azure storage blob

    var signedUrl = buildSignedUrl(ShortUUID.get() + "_e2e.parquet");
    var writeToUri = ParquetReaderWriterWithAvro.getURIFromSignedUrl(signedUrl);
    var config = ParquetReaderWriterWithAvro.getConfigFromSignedUri(signedUrl);
    var path = new Path(writeToUri);
    OutputFile outputFile = HadoopOutputFile.fromPath(path, config);
    readWriteToParquet(
        azureInputFile, columnNames, columnDataTypeMap, outputFile, newSchema, config);

    // Read from azure storage blob
    InputFile inputFile = HadoopInputFile.fromPath(path, config);
    List<GenericData.Record> formattedRecords =
        ParquetReaderWriterWithAvro.readFromParquet(inputFile);
    assertThat(
        "datetime should now be long value representing microseconds since epoch",
        formattedRecords.get(0).get("date_time_column"),
        equalTo(1657022401000000L));
    assertThat(
        "record has correct schema",
        formattedRecords.get(0).getSchema().getField("date_time_column").schema().toString(),
        containsString("timestamp-micros"));

    // read from GCP output file and confirm the same results
    Path gcpSourceFile = new Path(FOLDER_PATH + "bq_original.parquet");
    List<GenericData.Record> gcpRecords =
        ParquetReaderWriterWithAvro.readFromParquet(gcpSourceFile);
    assertThat(
        "record has correct schema",
        gcpRecords.get(0).getSchema().getField("date_time_column").schema().toString(),
        containsString("timestamp-micros"));
  }

  private Path randomizedFilePath(String baseFileName) {
    return new Path(OUTPUT_PATH + baseFileName + "_" + ShortUUID.get().toString() + ".parquet");
  }

  private Schema sampleDataSchema() {
    var jsonSchema =
        """
        {
          "type" : "record",
          "name" : "int_record",
          "namespace" : "test",
          "fields" : [ {
            "name" : "c1",
            "type" : [ "null", "long" ],
            "default" : null
          } ]
        }
        """;
    return new Schema.Parser().parse(jsonSchema);
  }

  private List<GenericData.Record> sampleRecords(Schema schema) {
    GenericData.Record record = new GenericData.Record(schema);
    record.put("c1", 1);
    GenericData.Record record2 = new GenericData.Record(schema);
    record2.put("c1", 2);
    return List.of(record, record2);
  }

  private void assertSampleData(List<GenericData.Record> records) {
    assertThat("contains the right number of values", records, hasSize(2));
    assertThat("retrieves the right value", records.get(0).get("c1"), equalTo(1L));
    assertThat("retrieves the right value", records.get(1).get("c1"), equalTo(2L));
  }

  // We don't really need to build the signed URL, we could instead just pass these few bits of
  // information to the method that builds the write configuration
  private String buildSignedUrl(String fileName) {
    var storageAccount = "shelbytestaccount";
    var filePath = "testfilesystem/oysters/";
    // Url signed from "testfilesystem" container
    // TODO - set your own SAS token here
    String sasToken = null;
    if (sasToken == null) {
      throw new IllegalArgumentException("SAS token must be set");
    }
    return "https://%s.blob.core.windows.net/%s%s?%s"
        .formatted(storageAccount, filePath, fileName, sasToken);
  }

  private InputFile buildInputFileFromSignedUrl(String signedUrl) throws IOException {
    var writeToUri = ParquetReaderWriterWithAvro.getURIFromSignedUrl(signedUrl);
    var config = ParquetReaderWriterWithAvro.getConfigFromSignedUri(signedUrl);
    var path = new Path(writeToUri);
    return HadoopInputFile.fromPath(path, config);
  }
}
