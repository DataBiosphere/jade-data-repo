package bio.terra.service.cohortbuilder.tanagra.configuration;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "tanagra.underlay")
public class UnderlayConfiguration {
  // The list of underlay config files in the resources/config directory. (e.g.
  // 'broad/aou_synthetic/expanded/aou_synthetic.json')
  private List<String> files = new ArrayList<>();

  public List<String> getFiles() {
    return files;
  }

  public void setFiles(List<String> files) {
    this.files = files;
  }
}
