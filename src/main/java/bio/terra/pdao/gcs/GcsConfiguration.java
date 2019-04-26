package bio.terra.pdao.gcs;


import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * The theory of operation here is that each implementation of a pdao will specify a configuration.
 * That way, implementation specifics can be separated from the interface. We'll see if it works out that way.
 */
@Configuration
@Profile("google")
@ConfigurationProperties(prefix = "datarepo.gcs")
public class GcsConfiguration {

    private String bucket;
    private String region;
    private int connectTimeoutSeconds;
    private int readTimeoutSeconds;

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public int getConnectTimeoutSeconds() {
        return connectTimeoutSeconds;
    }

    public void setConnectTimeoutSeconds(int connectTimeoutSeconds) {
        this.connectTimeoutSeconds = connectTimeoutSeconds;
    }

    public int getReadTimeoutSeconds() {
        return readTimeoutSeconds;
    }

    public void setReadTimeoutSeconds(int readTimeoutSeconds) {
        this.readTimeoutSeconds = readTimeoutSeconds;
    }

    @Bean
    public Storage storage() {
        // NOTE: it is also possible to set retry options here with RetrySettings.Builder to make
        // settings and the .setRetryOptions() setter below. For now, just extend the timeouts
        HttpTransportOptions transportOptions = StorageOptions.getDefaultHttpTransportOptions();
        transportOptions = transportOptions.toBuilder()
            .setConnectTimeout(connectTimeoutSeconds * 1000)
            .setReadTimeout(readTimeoutSeconds * 1000)
            .build();
        StorageOptions storageOptions = StorageOptions.newBuilder()
            .setTransportOptions(transportOptions)
            .build();
        return storageOptions.getService();
    }

}
