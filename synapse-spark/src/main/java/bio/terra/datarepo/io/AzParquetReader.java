package bio.terra.datarepo.io;


import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class AzParquetReader {
  // To use, build the jar using:
  // ./gradlew :synapse-spark:jar
  // Then upload the jar to
  // https://tdrsnpsintsaeastus.blob.core.windows.net/exec
  // Spark will read the jar from there

  public static void main(String[] args) {
    // Create your Spark session
    SparkSession spark = SparkSession.builder().appName("AZ Parquet Reader").getOrCreate();

    // How to log
    spark.log().info("Hello, World!");

    // Parse the arguments.  This is gross and can totally be cleaned up!
    String sourceUrl = null;
    // Note on the Sas token: it has to be a container or storage account SAS token
    // (with Read + List and eventually create + write), not a blob Sas token
    String sourceSasToken = null;
    for (String arg: args) {
      if (arg.startsWith("--tdr.source.path")) {
        sourceUrl = arg.split(" ")[1];
      } else if (arg.startsWith("--tdr.source.sas")) {
        sourceSasToken = arg.split(" ")[1];
      }
      spark.log().info(arg);
    }
    spark.log().info("Source URL: " + sourceUrl);
    spark.log().info("Source SAS: " + sourceSasToken);

    // Maybe we can use BlobUrlParts.parse(sourceUrl);
    String storageAccount = sourceUrl.split(".dfs.core.windows.net")[0].split("@")[1];
    spark.conf().set("fs.azure.account.auth.type", "SAS");
    spark.log().info("Storage account: " + storageAccount);

    // Set configuration to read from the storage account using a Sas token
    spark.sparkContext().hadoopConfiguration()
        .set(String.format("fs.azure.account.auth.type.%s.dfs.core.windows.net", storageAccount), "SAS");
    spark.sparkContext().hadoopConfiguration().set("fs.azure.sas.token.provider.type", "com.microsoft.azure.synapse.tokenlibrary.ConfBasedSASProvider");
    spark.conf()
        .set(String.format("spark.storage.synapse.%s.dfs.core.windows.net.sas", storageAccount),
            sourceSasToken);

    // Read the parquet file
    // Note that we need to specify a schema or it fails trying to read the UUID column
    Dataset<Row> parquet = spark.read()
        .schema(new StructType()
                .add(new StructField("datarepo_row_id", StringType, false, Metadata.empty()))
                .add(new StructField("id", IntegerType, false, Metadata.empty()))
                .add(new StructField("data", StringType, false, Metadata.empty())))
        .parquet(sourceUrl);

    // For now just log the record counts
    spark.log().info("File has " + parquet.count() + " records");
  }
}
