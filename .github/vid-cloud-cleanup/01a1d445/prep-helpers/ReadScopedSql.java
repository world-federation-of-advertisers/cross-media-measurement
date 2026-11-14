import com.google.auth.oauth2.GoogleCredentials;
import com.google.gson.*;
import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.Set;
/** DO_NOT_SUBMIT: One-process existing-ADC strong read-only Spanner query; no data mutations. */
public final class ReadScopedSql {
 public static void main(String[] args)throws Exception {
  if(args.length!=2||!Set.of("edp-aggregator","secure-computation","kingdom","reporting").contains(args[0]))throw new IllegalArgumentException("Fixed dev DB and SQL file required");
  var credentials=GoogleCredentials.getApplicationDefault().createScoped("https://www.googleapis.com/auth/cloud-platform");credentials.refreshIfExpired();String token=credentials.getAccessToken().getTokenValue();
  var client=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build();String base="https://spanner.googleapis.com/v1/projects/halo-cmm-dev/instances/dev-instance/databases/"+args[0];
  String session=null;
  try {
   var created=call(client,token,"POST",base+"/sessions","{}");session=JsonParser.parseString(created).getAsJsonObject().get("name").getAsString();
   if(!session.startsWith(base.substring("https://spanner.googleapis.com/v1/".length())+"/sessions/"))throw new IllegalStateException("Unexpected session scope");
   var body=new JsonObject();body.addProperty("sql",Files.readString(Path.of(args[1])));var txn=new JsonObject();var single=new JsonObject();single.add("readOnly",new JsonObject());txn.add("singleUse",single);body.add("transaction",txn);
   String result=call(client,token,"POST","https://spanner.googleapis.com/v1/"+session+":executeSql",body.toString());System.out.println(result);
  } finally {if(session!=null)call(client,token,"DELETE","https://spanner.googleapis.com/v1/"+session,null);}
 }
 static String call(HttpClient client,String token,String method,String url,String body)throws Exception {
  var req=HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(60)).header("Authorization","Bearer "+token).header("Content-Type","application/json").method(method,body==null?HttpRequest.BodyPublishers.noBody():HttpRequest.BodyPublishers.ofString(body)).build();
  var result=client.send(req,HttpResponse.BodyHandlers.ofString());if(result.statusCode()>=400)throw new IllegalStateException("Spanner HTTP "+result.statusCode()+" "+result.body());return result.body();
 }
}
