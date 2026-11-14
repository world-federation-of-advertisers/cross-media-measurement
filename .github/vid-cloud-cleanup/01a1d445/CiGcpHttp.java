import com.google.auth.oauth2.GoogleCredentials;
import java.net.URI;import java.net.http.*;import java.nio.file.*;import java.time.Duration;import java.util.Set;
/** Existing WIF ADC read-only dev API bridge; tokens never enter result files. */
public final class CiGcpHttp {
 public static void main(String[] a)throws Exception {
  if(a.length!=4)throw new IllegalArgumentException("method URL body-path content-type");var u=URI.create(a[1]);String host=u.getHost(),path=u.getPath(),method=a[0];boolean allowed=false;
  if(method.equals("GET"))allowed=Set.of("run.googleapis.com","compute.googleapis.com","cloudscheduler.googleapis.com","container.googleapis.com","spanner.googleapis.com").contains(host)&&path.contains("/projects/halo-cmm-dev/")||host.equals("storage.googleapis.com")&&(path.contains("/b/secure-computation-storage-dev-bucket")||path.contains("/b/edpa-configs-storage-dev-bucket"));
  if(method.equals("POST"))allowed=host.equals("compute.googleapis.com")&&path.startsWith("/compute/v1/projects/halo-cmm-dev/")&&path.endsWith("/listManagedInstances");
  if(!u.getScheme().equals("https")||!allowed)throw new IllegalArgumentException("Forbidden dev read endpoint/method");
  var credentials=GoogleCredentials.getApplicationDefault().createScoped("https://www.googleapis.com/auth/cloud-platform");credentials.refreshIfExpired();var body=a[2].equals("-")?HttpRequest.BodyPublishers.noBody():HttpRequest.BodyPublishers.ofFile(Path.of(a[2]));var req=HttpRequest.newBuilder(u).timeout(Duration.ofSeconds(90)).header("Authorization","Bearer "+credentials.getAccessToken().getTokenValue()).header("Content-Type",a[3]).method(method,body).build();var response=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build().send(req,HttpResponse.BodyHandlers.ofString());if(response.statusCode()>=400)throw new IllegalStateException("API HTTP "+response.statusCode()+": "+response.body());System.out.print(response.body());
 }
}
