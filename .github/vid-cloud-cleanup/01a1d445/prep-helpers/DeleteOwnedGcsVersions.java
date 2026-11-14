import com.google.cloud.BatchResult;
import com.google.cloud.storage.*;
import com.google.gson.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/** DO_NOT_SUBMIT: Deletes only reviewed exact versions under this run's four private prefixes. */
public final class DeleteOwnedGcsVersions {
  static final String BUCKET="secure-computation-storage-dev-bucket";
  static final List<String> PREFIXES=List.of(
      "raw-impressions/edp/edp7/vid-cloud-20261113/",
      "external-impressions/edp/edp7/vid-cloud-20261113/",
      "vid-labeled-impressions/edp/edp7/vid-cloud-20261113/",
      "test-operator-artifacts/vid-cloud-20261113/");
  static void require(boolean b,String m) { if(!b)throw new IllegalArgumentException(m); }
  static void atomic(Path path,Object value)throws Exception {
    Files.createDirectories(path.toAbsolutePath().getParent());
    Path tmp=path.resolveSibling(path.getFileName()+".tmp");
    byte[] bytes=(new GsonBuilder().setPrettyPrinting().create().toJson(value)+"\n").getBytes(java.nio.charset.StandardCharsets.UTF_8);
    try(FileChannel file=FileChannel.open(tmp,StandardOpenOption.CREATE,StandardOpenOption.WRITE,StandardOpenOption.TRUNCATE_EXISTING)) {
      ByteBuffer buffer=ByteBuffer.wrap(bytes);while(buffer.hasRemaining())file.write(buffer);file.force(true);
    }
    Files.move(tmp,path,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);
    try(FileChannel directory=FileChannel.open(path.toAbsolutePath().getParent(),StandardOpenOption.READ)){directory.force(true);}
  }
  public static void main(String[] a)throws Exception {
    require(a.length==4,"snapshot-json expected-sha256 --dry-run|--execute journal-json");
    require(a[2].equals("--dry-run")||a[2].equals("--execute"),"Explicit mode required");
    byte[] bytes=Files.readAllBytes(Path.of(a[0]));
    String sha=HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    require(sha.equals(a[1]),"Snapshot hash mismatch");
    JsonObject manifest=JsonParser.parseString(new String(bytes,java.nio.charset.StandardCharsets.UTF_8)).getAsJsonObject();
    require(manifest.get("project").getAsString().equals("halo-cmm-dev") && manifest.get("session").getAsString().equals("01a1d445"),"Wrong project/session");
    String kind=manifest.get("kind").getAsString();
    require(kind.equals("RAW_DONE_FIRST")||kind.equals("REMAINING"),"Wrong cleanup pass");
    JsonArray rows=manifest.getAsJsonArray("objects");Set<String> ids=new HashSet<>();
    for(JsonElement element:rows) {
      JsonObject row=element.getAsJsonObject();String bucket=row.get("bucket").getAsString(),name=row.get("name").getAsString();
      long generation=Long.parseLong(row.get("generation").getAsString()),meta=Long.parseLong(row.get("metageneration").getAsString());
      require(bucket.equals(BUCKET)&&PREFIXES.stream().anyMatch(name::startsWith)&&!name.contains("..")&&generation>0&&meta>0,"Outside reviewed private version scope");
      require(ids.add(bucket+"/"+name+"#"+generation),"Duplicate version");
      boolean rawDone=name.startsWith(PREFIXES.get(0))&&name.endsWith("/done");
      require(kind.equals("RAW_DONE_FIRST")?rawDone:!rawDone,"Wrong RAW done ordering pass");
    }
    Map<String,Object> journal=new LinkedHashMap<>();journal.put("snapshotSha256",sha);journal.put("project","halo-cmm-dev");journal.put("session","01a1d445");journal.put("kind",kind);journal.put("mode",a[2]);journal.put("expectedVersions",rows.size());
    Map<String,String> results=new ConcurrentHashMap<>();journal.put("results",results);
    Path output=Path.of(a[3]);atomic(output,journal);
    if(a[2].equals("--dry-run")) {System.out.println("Validated "+rows.size()+" exact private versions; no cloud mutation.");return;}
    Storage storage=StorageOptions.newBuilder().setProjectId("halo-cmm-dev").build().getService();
    for(int start=0;start<rows.size();start+=100) {
      StorageBatch batch=storage.batch();List<String> batchKeys=new ArrayList<>();
      for(int i=start;i<Math.min(start+100,rows.size());i++) {
        JsonObject row=rows.get(i).getAsJsonObject();String name=row.get("name").getAsString();
        long generation=Long.parseLong(row.get("generation").getAsString()),meta=Long.parseLong(row.get("metageneration").getAsString());String key=name+"#"+generation;batchKeys.add(key);
        batch.delete(BlobId.of(BUCKET,name,generation),Storage.BlobSourceOption.generationMatch(generation),Storage.BlobSourceOption.metagenerationMatch(meta)).notify(new BatchResult.Callback<Boolean,StorageException>() {
          public void success(Boolean deleted) { results.put(key,Boolean.TRUE.equals(deleted)?"DELETED":"ALREADY_ABSENT"); }
          public void error(StorageException error) {results.put(key,error.getCode()==404?"ALREADY_ABSENT":"ERROR:"+error.getCode());}
        });
      }
      journal.put("currentBatchStart",start);journal.put("currentBatchKeys",batchKeys);journal.put("state","BATCH_PREPARED");atomic(output,journal);
      try {batch.submit();}
      catch(Exception error) {journal.put("state","FAILED_STOPPED");journal.put("errorType",error.getClass().getName());throw error;}
      finally {atomic(output,journal);}
      if(results.values().stream().anyMatch(x->x.startsWith("ERROR:"))||results.size()!=Math.min(start+100,rows.size())) {
        journal.put("state","FAILED_STOPPED");atomic(output,journal);throw new IllegalStateException("API/precondition/incomplete results; stopped future batches; reconcile exact journal versions");
      }
      journal.put("state","BATCH_COMPLETE");atomic(output,journal);
    }
    journal.put("state","COMPLETE");atomic(output,journal);System.out.println("Completed exact owned generation-fenced pass "+kind+" versions="+rows.size());
  }
}
