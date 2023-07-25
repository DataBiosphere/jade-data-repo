package bio.terra;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class Main implements CommandLineRunner {

  @Override
  public void run(String... arg0) throws Exception {
    if (arg0.length > 0 && arg0[0].equals("exitcode")) {
      throw new ExitException();
    }
  }

  public static void main(String[] args) throws Exception {
    SpringApplication theApp = new SpringApplication(Main.class);
    // Initially, Jade runs only with Google cloud parts right now, so we set the profile here.
    // ITFOT, we can parameterize the profile to include the appropriate pdao implementations.
    theApp.setAdditionalProfiles("google");

    // Enable sending %2F (e.g. url encoded forward slashes) in the path of a URL which is helpful
    // if there is a path parameter that contains a value with a slash in it.
    System.setProperty("org.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH", "true");

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
