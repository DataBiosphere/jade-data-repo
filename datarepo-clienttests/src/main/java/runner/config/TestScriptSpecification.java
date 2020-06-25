package runner.config;

import runner.TestScript;

import java.time.Duration;
import java.time.format.DateTimeParseException;

public class TestScriptSpecification implements SpecificationInterface {
    public String name;
    public long numberToRun;
    public String timeout;

    public Class<? extends TestScript> scriptClass;
    public Duration timeoutObj;

    public static final String scriptsPackage = "testscripts";

    TestScriptSpecification() { }

    /**
     * Validate the server specification read in from the JSON file.
     * The timeout string is parsed into a Duration; the name is converted into a Java class reference.
     */
    public void validate() {
        try {
            timeoutObj = Duration.parse(timeout);
        } catch (DateTimeParseException dtpEx) {
            throw new IllegalArgumentException("Timeout format not supported: " + timeout, dtpEx);
        }

        try {
            Class<?> scriptClassGeneric = Class.forName(scriptsPackage + "." + name);
            scriptClass = (Class<? extends TestScript>)scriptClassGeneric;
        } catch (ClassNotFoundException|ClassCastException classEx) {
            throw new IllegalArgumentException("Test script class not found: " + name, classEx);
        }
    }

    public void display() {
        System.out.println("Test Script: " + name);
        System.out.println("  numberToRun: " + numberToRun);
        System.out.println("  timeout: " + timeoutObj);
    }
}
