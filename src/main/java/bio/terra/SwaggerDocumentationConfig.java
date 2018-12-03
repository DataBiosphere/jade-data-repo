package bio.terra;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;

@javax.annotation.Generated(value = "io.swagger.codegen.languages.SpringCodegen",
                            date = "2018-11-21T14:51:14.798-05:00")

@Configuration
public class SwaggerDocumentationConfig {

    ApiInfo apiInfo() {
        return new ApiInfoBuilder()
            .title("Data Repository API")
            .description("This document defines the REST API for Data Repository.\n\n" +
                "**Status: design in progress**\n\n" +
                         "There are three top-level endpoints (besides some used by swagger):\n" +
                         " * /       - generated by swagger: swagger API page that provides this" +
                         "documentation and a live UI for submitting REST requests\n" +
                         " * /status - provides the operational status of the service\n" +
                         " * /api    - is the authenticated and authorized Data Repository API\n\n" +
                         "The overall API (/api) supports several different interfaces:\n" +
                         " * Access - accessing data from the repository\n" +
                         " * Search - finding data in the repository\n" +
                         " * Model - managing data models used in studies\n" +
                         " * Custody - managing studies, datasets, and access permissions\n" +
                         " * Ingest - loading data into the repository\n\n" +
                         "The API endpoints are organized by interface. Each interface is separately versioned.\n" +
                         "## Notes on Naming\n" +
                         "All of the reference items are prefixed with \"Model\"." +
                         "Those names are used as the class names in the generated Java code." +
                         "It is helpful to distinguish these model classes from other related classes," +
                         "like the DAO classes and the operation classes.\n\n" +
                         "## Editing and debugging\n" +
                         "I have found it best to edit this file directly to make changes and then use" +
                         "the swagger-editor to validate." +
                         "The errors out of swagger-codegen are not that helpful. In the swagger-editor," +
                         "it gives you nice errors and links to the place in the YAML where the errors are." +
                         "But... the swagger-editor has been a bit of a pain for me to run." +
                         "I tried the online website and was not able to load my YAML." +
                         "Instead, I run it locally in a docker container, like this:\n" +
                         " ```\n" +
                         " docker pull swaggerapi/swagger-editor\n" +
                         " docker run -p 9090:8080 swaggerapi/swagger-editor\n" +
                         " ```\n" +
                         " Then navigate to localhost:9090 in your browser.\n" +
                         " I have not been able to get the file upload to work. It is a bit of a PITA," +
                         "but I copy-paste the source code, replacing what is in the editor." +
                         "Then make any fixes. Then copy-paste the resulting, valid file back into our source code." +
                         "Not elegant, but easier than playing detective with the swagger-codegen errors." +
                         "This might be something about my browser or environment," +
                         "so give it a try yourself and see how it goes.")
            .license("Apache 2.0")
            .licenseUrl("https://www.apache.org/licenses/LICENSE-2.0.html")
            .termsOfServiceUrl("")
            .version("0.1.0")
            .contact(new Contact("", "", ""))
            .build();
    }

    @Bean
    public Docket customImplementation() {
        return new Docket(DocumentationType.SWAGGER_2)
                .select()
                    .apis(RequestHandlerSelectors.basePackage("bio.terra.controller"))
                    .build()
                .directModelSubstitute(java.time.LocalDate.class, java.sql.Date.class)
                .directModelSubstitute(java.time.OffsetDateTime.class, java.util.Date.class)
                .apiInfo(apiInfo());
    }

}
