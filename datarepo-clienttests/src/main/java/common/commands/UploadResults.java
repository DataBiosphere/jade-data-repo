package common.commands;

import uploader.ResultUploader;

public class UploadResults {
  public static void main(String[] args) throws Exception {
    if (args.length == 2) { // upload results
      String uploadListFileName = args[0];
      String outputDirName = args[1];
      ResultUploader.uploadResults(uploadListFileName, outputDirName);
    } else { // if no args specified or invalid number of args specified, print help
      PrintHelp.printHelp();
    }
  }
}
