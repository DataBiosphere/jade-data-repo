package bio.terra.pdao.gcs;


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

    @Bean
    public Storage storage() {
        return StorageOptions.getDefaultInstance().getService();
    }

}
