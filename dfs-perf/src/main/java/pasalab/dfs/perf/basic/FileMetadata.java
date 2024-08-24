package pasalab.dfs.perf.basic;

public class FileMetadata {
  public String fileName;
  public String filePath;
  public int fileSize;
  public RedundancyStatus status;

  public FileMetadata(String fileName, String filePath, int fileSize, RedundancyStatus redundancyStatus) {
    this.fileName = fileName;
    this.filePath = filePath;
    this.fileSize = fileSize;
    // always starts as either replicated or hybrid
    this.status = redundancyStatus;
  }

  public String getFullPath() {
    return filePath + "/" + fileName;
  }

}