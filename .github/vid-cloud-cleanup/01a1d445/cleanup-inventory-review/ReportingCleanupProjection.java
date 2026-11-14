import java.nio.file.*;
public final class ReportingCleanupProjection {
  public static void main(String[] args) {
    boolean ok = args.length == 0 && Files.isRegularFile(Path.of("/GCloudInternalReportingServer_deploy.jar"))
      && Files.isRegularFile(Path.of("/etc/halo-cmms/config-files/.vid-cloud-20261113-cleanup-inventory.jar"));
    System.out.println(ok ? "PROJECTION_AND_RUNTIME_PRESENT" : "PROJECTION_OR_RUNTIME_ABSENT");
    System.exit(ok ? 0 : 1);
  }
}
