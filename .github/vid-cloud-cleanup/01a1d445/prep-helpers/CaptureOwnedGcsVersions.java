import com.google.cloud.storage.*;
import com.google.gson.*;
import java.nio.file.*;
import java.util.*;
/** Read-only fixed private scope inventory, readable and retained soft-deleted separately. */
public final class CaptureOwnedGcsVersions {
 static final String BUCKET="secure-computation-storage-dev-bucket";
 static final List<String> PREFIXES=List.of("raw-impressions/edp/edp7/vid-cloud-20261113/","external-impressions/edp/edp7/vid-cloud-20261113/","vid-labeled-impressions/edp/edp7/vid-cloud-20261113/","test-operator-artifacts/vid-cloud-20261113/");
 static Map<String,Object> row(Blob b){var r=new LinkedHashMap<String,Object>();r.put("bucket",b.getBucket());r.put("name",b.getName());r.put("generation",b.getGeneration().toString());r.put("metageneration",b.getMetageneration().toString());r.put("size",b.getSize().toString());r.put("crc32c",b.getCrc32c());r.put("md5Hash",b.getMd5());r.put("metadata",b.getMetadata()==null?Map.of():b.getMetadata());return r;}
 public static void main(String[] a)throws Exception {
  if(a.length!=1||!a[0].startsWith("/tmp/vid-cloud-data-01a1d445/cleanup-"))throw new IllegalArgumentException("fixed session output");
  var s=StorageOptions.newBuilder().setProjectId("halo-cmm-dev").build().getService();var readable=new ArrayList<Map<String,Object>>();var soft=new ArrayList<Map<String,Object>>();
  for(String p:PREFIXES){for(Blob b:s.list(BUCKET,Storage.BlobListOption.prefix(p),Storage.BlobListOption.versions(true),Storage.BlobListOption.pageSize(1000)).iterateAll())readable.add(row(b));for(Blob b:s.list(BUCKET,Storage.BlobListOption.prefix(p),Storage.BlobListOption.softDeleted(true),Storage.BlobListOption.pageSize(1000)).iterateAll())soft.add(row(b));System.err.println(p+" readableTotal="+readable.size()+" softTotal="+soft.size());}
  readable.sort(Comparator.comparing(r->r.get("name")+"#"+r.get("generation")));soft.sort(Comparator.comparing(r->r.get("name")+"#"+r.get("generation")));
  var out=new LinkedHashMap<String,Object>();out.put("project","halo-cmm-dev");out.put("session","01a1d445");out.put("capturedAt",java.time.Instant.now().toString());out.put("prefixes",PREFIXES);out.put("readable",readable);out.put("retainedSoftDeleted",soft);Files.writeString(Path.of(a[0]),new GsonBuilder().setPrettyPrinting().create().toJson(out)+"\n");System.out.println("READABLE "+readable.size()+" RETAINED_SOFT "+soft.size());
 }
}
