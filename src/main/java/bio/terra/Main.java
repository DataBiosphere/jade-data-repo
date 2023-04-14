package bio.terra;

import bio.terra.tanagra.indexing.IndexerMain;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class Main implements CommandLineRunner {

  @Autowired private IndexerMain indexer;

  @Override
  public void run(String... arg0) throws Exception {
    if (arg0.length > 0 && arg0[0].equals("exitcode")) {
      throw new ExitException();
    }
    if (arg0.length > 1) {
      indexer.run(arg0);
      System.exit(0);
    }
  }

  public static void main(String[] args) throws Exception {
    SpringApplication theApp = new SpringApplication(Main.class);
    // Initially, Jade runs only with Google cloud parts right now, so we set the profile here.
    // ITFOT, we can parameterize the profile to include the appropriate pdao implementations.
    theApp.setAdditionalProfiles("google");
    theApp.run(args);
  }

  static class ExitException extends RuntimeException implements ExitCodeGenerator {
    private static final long serialVersionUID = 1L;

    @Override
    public int getExitCode() {
      return 10;
    }
  }
}
