import java.lang.reflect.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;

/** Read-only inventory using the already deployed server's R2DBC Cloud SQL connector. */
public final class ReportingCleanupInventory {
  private static final String MC = "VCTqwV_vFXw";
  private static final String PREFIX = "vid-cloud-20261113-";
  private static Class<?> cls(String name) throws Exception { return Class.forName(name); }
  private static Object invoke(Object obj, String name, Class<?>[] types, Object... args)
      throws Exception {
    return obj.getClass().getMethod(name, types).invoke(obj, args);
  }
  private static List<Object> await(Object publisher) throws Exception {
    List<Object> rows = new ArrayList<>();
    CountDownLatch done = new CountDownLatch(1);
    Throwable[] failure = new Throwable[1];
    Class<?> subscriber = cls("org.reactivestreams.Subscriber");
    Object sink = Proxy.newProxyInstance(subscriber.getClassLoader(), new Class<?>[]{subscriber},
        (p, m, a) -> {
          switch (m.getName()) {
            case "onSubscribe": cls("org.reactivestreams.Subscription")
                .getMethod("request", long.class).invoke(a[0], Long.MAX_VALUE); break;
            case "onNext": rows.add(a[0]); break;
            case "onError": failure[0] = (Throwable)a[0]; done.countDown(); break;
            case "onComplete": done.countDown(); break;
            case "toString": return "ReadOnlyInventorySubscriber";
          }
          return null;
        });
    cls("org.reactivestreams.Publisher").getMethod("subscribe", subscriber).invoke(publisher, sink);
    if (!done.await(70, TimeUnit.SECONDS)) throw new TimeoutException("R2DBC operation timed out");
    if (failure[0] != null) throw new Exception("R2DBC operation failed", failure[0]);
    return rows;
  }
  private static List<Map<String,Object>> query(Object connection, String sql) throws Exception {
    Object statement = cls("io.r2dbc.spi.Connection").getMethod("createStatement", String.class)
        .invoke(connection, sql);
    Object resultPublisher = cls("io.r2dbc.spi.Statement").getMethod("execute").invoke(statement);
    List<Map<String,Object>> rows = new ArrayList<>();
    BiFunction<Object,Object,Object> mapper = (row, metadata) -> {
      try {
        Map<String,Object> item = new LinkedHashMap<>();
        Iterable<?> columns = (Iterable<?>)cls("io.r2dbc.spi.RowMetadata")
            .getMethod("getColumnMetadatas").invoke(metadata);
        for (Object column : columns) {
          String name = (String)cls("io.r2dbc.spi.ColumnMetadata").getMethod("getName").invoke(column);
          Object value = cls("io.r2dbc.spi.Readable").getMethod("get", String.class).invoke(row,name);
          item.put(name, name.toLowerCase(Locale.ROOT).endsWith("id") && value instanceof Number ? value.toString() : value);
        }
        return item;
      } catch (Exception e) { throw new RuntimeException(e); }
    };
    for (Object result : await(resultPublisher)) {
      Object mapped = cls("io.r2dbc.spi.Result").getMethod("map", BiFunction.class).invoke(result,mapper);
      for (Object row : await(mapped)) rows.add((Map<String,Object>)row);
    }
    return rows;
  }
  private static String json(Object x) {
    if (x == null) return "null";
    if (x instanceof Boolean || x instanceof Number) return x.toString();
    if (x instanceof Map<?,?> m) {
      List<String> fields = new ArrayList<>();
      for (var e : m.entrySet()) fields.add(json(e.getKey().toString())+":"+json(e.getValue()));
      return "{"+String.join(",",fields)+"}";
    }
    if (x instanceof Iterable<?> items) {
      List<String> vals = new ArrayList<>(); for(Object v:items) vals.add(json(v));
      return "["+String.join(",",vals)+"]";
    }
    StringBuilder out = new StringBuilder("\"");
    for (char c : x.toString().toCharArray()) {
      switch(c) {
        case '\\': out.append("\\\\"); break;
        case '"': out.append("\\\""); break;
        case '\n': out.append("\\n"); break;
        case '\r': out.append("\\r"); break;
        case '\t': out.append("\\t"); break;
        default: if(c<32) out.append(String.format("\\u%04x",(int)c)); else out.append(c);
      }
    }
    return out.append('"').toString();
  }
  public static void main(String[] args) {
    try {
      inventory(args);
      System.exit(0);
    } catch (Throwable failure) {
      failure.printStackTrace(System.err);
      System.exit(1);
    }
  }
  private static void inventory(String[] args) throws Exception {
    if(args.length != 0) throw new IllegalArgumentException("This helper only inventories fixed owned scope");
    Class<?> options = cls("io.r2dbc.spi.ConnectionFactoryOptions");
    Class<?> option = cls("io.r2dbc.spi.Option");
    Object builder = options.getMethod("builder").invoke(null);
    Class<?> builderClass = cls("io.r2dbc.spi.ConnectionFactoryOptions$Builder");
    String[][] settings = {{"DRIVER","gcp"},{"PROTOCOL","postgresql"},
        {"USER","reporting-v2-internal@halo-cmm-dev.iam"},{"PASSWORD","UNUSED"},
        {"DATABASE","reporting-v2"},{"HOST","halo-cmm-dev:us-central1:dev-postgres"}};
    for(String[] setting:settings) builderClass.getMethod("option",option,Object.class)
        .invoke(builder,options.getField(setting[0]).get(null),setting[1]);
    builderClass.getMethod("option",option,Object.class).invoke(builder,
        cls("com.google.cloud.sql.core.GcpConnectionFactoryProvider")
            .getField("ENABLE_IAM_AUTH").get(null),true);
    builderClass.getMethod("option",option,Object.class).invoke(builder,
        options.getField("STATEMENT_TIMEOUT").get(null),Duration.ofSeconds(60));
    Object factory = cls("io.r2dbc.spi.ConnectionFactories").getMethod("get",options)
        .invoke(null,builderClass.getMethod("build").invoke(builder));
    Object connection = await(cls("io.r2dbc.spi.ConnectionFactory").getMethod("create").invoke(factory)).get(0);
    try {
      query(connection,"BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
      List<Map<String,Object>> who = query(connection,"SELECT current_user AS sql_user,current_database() AS database");
      if(who.size()!=1 || !"reporting-v2-internal@halo-cmm-dev.iam".equals(who.get(0).get("sql_user"))
          || !"reporting-v2".equals(who.get(0).get("database"))) throw new IllegalStateException("Unexpected SQL identity/database");
      String scope = "MeasurementConsumerId=(SELECT MeasurementConsumerId FROM MeasurementConsumers WHERE CmmsMeasurementConsumerId='"+MC+"')";
      String ownReports = "SELECT ReportId FROM Reports WHERE "+scope+" AND STARTS_WITH(ExternalReportId,'"+PREFIX+"')";
      String linkedMetrics = "SELECT DISTINCT MetricId FROM MetricCalculationSpecReportingMetrics WHERE "+scope+" AND ReportId IN ("+ownReports+")";
      String ownMetrics = "SELECT MetricId FROM Metrics WHERE "+scope+" AND (MetricId IN ("+linkedMetrics+") OR STARTS_WITH(ExternalMetricId,'"+PREFIX+"'))";
      String ownSets = "SELECT ReportingSetId FROM ReportingSets WHERE "+scope+" AND STARTS_WITH(ExternalReportingSetId,'"+PREFIX+"')";
      String ownSpecs = "SELECT MetricCalculationSpecId FROM MetricCalculationSpecs WHERE "+scope+" AND STARTS_WITH(ExternalMetricCalculationSpecId,'"+PREFIX+"')";
      LinkedHashMap<String,String> queries = new LinkedHashMap<>();
      queries.put("identity","SELECT current_user AS sql_user,current_database() AS database");
      queries.put("foreign_keys","SELECT conrelid::regclass::text AS child_table,confrelid::regclass::text AS parent_table,pg_get_constraintdef(oid) AS definition FROM pg_constraint WHERE contype='f' ORDER BY 1,2,3");
      queries.put("columns","SELECT table_name,column_name,data_type FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name,ordinal_position");
      queries.put("reports","SELECT MeasurementConsumerId,ReportId,ExternalReportId,CreateReportRequestId FROM Reports WHERE "+scope+" AND STARTS_WITH(ExternalReportId,'"+PREFIX+"')");
      queries.put("reporting_sets","SELECT MeasurementConsumerId,ReportingSetId,ExternalReportingSetId FROM ReportingSets WHERE "+scope+" AND STARTS_WITH(ExternalReportingSetId,'"+PREFIX+"')");
      queries.put("metric_specs","SELECT MeasurementConsumerId,MetricCalculationSpecId,ExternalMetricCalculationSpecId FROM MetricCalculationSpecs WHERE "+scope+" AND STARTS_WITH(ExternalMetricCalculationSpecId,'"+PREFIX+"')");
      queries.put("report_metric_links","SELECT MeasurementConsumerId,ReportId,ReportingSetId,MetricCalculationSpecId,CreateMetricRequestId,MetricId FROM MetricCalculationSpecReportingMetrics WHERE "+scope+" AND ReportId IN ("+ownReports+")");
      queries.put("metrics","SELECT MeasurementConsumerId,MetricId,ReportingSetId,ExternalMetricId,State FROM Metrics WHERE "+scope+" AND (MetricId IN ("+ownMetrics+") OR STARTS_WITH(ExternalMetricId,'"+PREFIX+"'))");
      queries.put("metric_measurements","SELECT MeasurementConsumerId,MetricId,MeasurementId FROM MetricMeasurements WHERE "+scope+" AND MetricId IN ("+ownMetrics+")");
      queries.put("measurements","SELECT MeasurementConsumerId,MeasurementId,CmmsCreateMeasurementRequestId,CmmsMeasurementId,State FROM Measurements WHERE "+scope+" AND MeasurementId IN (SELECT MeasurementId FROM MetricMeasurements WHERE "+scope+" AND MetricId IN ("+ownMetrics+"))");
      queries.put("shared_metric_refs","SELECT MeasurementConsumerId,ReportId,MetricId FROM MetricCalculationSpecReportingMetrics WHERE "+scope+" AND MetricId IN ("+ownMetrics+") AND ReportId NOT IN ("+ownReports+")");
      queries.put("shared_spec_refs","SELECT MeasurementConsumerId,ReportId,MetricCalculationSpecId FROM MetricCalculationSpecReportingMetrics WHERE "+scope+" AND MetricCalculationSpecId IN ("+ownSpecs+") AND ReportId NOT IN ("+ownReports+")");
      queries.put("unowned_metrics_using_own_sets","SELECT MeasurementConsumerId,MetricId,ReportingSetId,ExternalMetricId FROM Metrics WHERE "+scope+" AND ReportingSetId IN ("+ownSets+") AND MetricId NOT IN ("+ownMetrics+")");
      queries.put("shared_measurement_refs","SELECT MeasurementConsumerId,MetricId,MeasurementId FROM MetricMeasurements WHERE "+scope+" AND MeasurementId IN (SELECT MeasurementId FROM MetricMeasurements WHERE "+scope+" AND MetricId IN ("+ownMetrics+")) AND MetricId NOT IN ("+ownMetrics+")");
      queries.put("event_groups","SELECT MeasurementConsumerId,EventGroupId,CmmsDataProviderId,CmmsEventGroupId FROM EventGroups WHERE "+scope+" AND CmmsDataProviderId='T5RryPMNong' AND CmmsEventGroupId IN ('CHUjhtVinyk','HrNdNNVinys','WJbBiNVinyw')");
      // Closure queries use column names from the actual dev PostgreSQL catalog captured above.
      String ownMeasurements = "SELECT MeasurementId FROM MetricMeasurements WHERE "+scope+" AND MetricId IN ("+ownMetrics+")";
      String ownBases = "SELECT PrimitiveReportingSetBasisId FROM PrimitiveReportingSetBases WHERE "+scope+" AND PrimitiveReportingSetId IN ("+ownSets+")";
      String ownEventGroups = "SELECT EventGroupId FROM EventGroups WHERE "+scope+" AND CmmsDataProviderId='T5RryPMNong' AND CmmsEventGroupId IN ('CHUjhtVinyk','HrNdNNVinys','WJbBiNVinyw')";
      queries.put("constraints","SELECT conrelid::regclass::text AS table_name,conname AS constraint_name,contype AS constraint_type,convalidated,condeferrable,condeferred,pg_get_constraintdef(oid) AS definition FROM pg_constraint WHERE connamespace='public'::regnamespace ORDER BY 1,2");
      queries.put("reporting_set_details","SELECT MeasurementConsumerId,ReportingSetId,ExternalReportingSetId,SetExpressionId,CampaignGroupId FROM ReportingSets WHERE "+scope+" AND ReportingSetId IN ("+ownSets+")");
      queries.put("metric_spec_details","SELECT MeasurementConsumerId,MetricCalculationSpecId,ExternalMetricCalculationSpecId,CampaignGroupId FROM MetricCalculationSpecs WHERE "+scope+" AND MetricCalculationSpecId IN ("+ownSpecs+")");
      queries.put("set_expressions","SELECT * FROM SetExpressions WHERE "+scope+" AND ReportingSetId IN ("+ownSets+")");
      queries.put("primitive_bases","SELECT * FROM PrimitiveReportingSetBases WHERE "+scope+" AND PrimitiveReportingSetId IN ("+ownSets+")");
      queries.put("basis_filters","SELECT * FROM PrimitiveReportingSetBasisFilters WHERE "+scope+" AND PrimitiveReportingSetBasisId IN ("+ownBases+")");
      queries.put("weighted_subset_unions","SELECT * FROM WeightedSubsetUnions WHERE "+scope+" AND ReportingSetId IN ("+ownSets+")");
      queries.put("weighted_union_basis_links","SELECT * FROM WeightedSubsetUnionPrimitiveReportingSetBases WHERE "+scope+" AND (ReportingSetId IN ("+ownSets+") OR PrimitiveReportingSetBasisId IN ("+ownBases+"))");
      queries.put("measurement_basis_links","SELECT * FROM MeasurementPrimitiveReportingSetBases WHERE "+scope+" AND (MeasurementId IN ("+ownMeasurements+") OR PrimitiveReportingSetBasisId IN ("+ownBases+"))");
      queries.put("set_event_group_links","SELECT * FROM ReportingSetEventGroups WHERE "+scope+" AND (ReportingSetId IN ("+ownSets+") OR EventGroupId IN ("+ownEventGroups+"))");
      queries.put("shared_set_report_metric_refs","SELECT MeasurementConsumerId,ReportId,ReportingSetId,MetricCalculationSpecId,MetricId FROM MetricCalculationSpecReportingMetrics WHERE "+scope+" AND ReportingSetId IN ("+ownSets+") AND ReportId NOT IN ("+ownReports+")");
      queries.put("shared_set_spec_campaign_refs","SELECT MeasurementConsumerId,MetricCalculationSpecId,ExternalMetricCalculationSpecId,CampaignGroupId FROM MetricCalculationSpecs WHERE "+scope+" AND CampaignGroupId IN ("+ownSets+") AND MetricCalculationSpecId NOT IN ("+ownSpecs+")");
      queries.put("shared_set_campaign_refs","SELECT MeasurementConsumerId,ReportingSetId,ExternalReportingSetId,CampaignGroupId FROM ReportingSets WHERE "+scope+" AND CampaignGroupId IN ("+ownSets+") AND ReportingSetId NOT IN ("+ownSets+")");
      queries.put("shared_set_expression_refs","SELECT * FROM SetExpressions WHERE "+scope+" AND ReportingSetId NOT IN ("+ownSets+") AND (LeftHandReportingSetId IN ("+ownSets+") OR RightHandReportingSetId IN ("+ownSets+"))");
      queries.put("shared_basis_weighted_refs","SELECT * FROM WeightedSubsetUnionPrimitiveReportingSetBases WHERE "+scope+" AND PrimitiveReportingSetBasisId IN ("+ownBases+") AND ReportingSetId NOT IN ("+ownSets+")");
      queries.put("shared_basis_measurement_refs","SELECT * FROM MeasurementPrimitiveReportingSetBases WHERE "+scope+" AND PrimitiveReportingSetBasisId IN ("+ownBases+") AND MeasurementId NOT IN ("+ownMeasurements+")");
      queries.put("shared_cached_event_group_set_refs","SELECT * FROM ReportingSetEventGroups WHERE "+scope+" AND EventGroupId IN ("+ownEventGroups+") AND ReportingSetId NOT IN ("+ownSets+")");
      queries.put("report_schedule_links","SELECT * FROM ReportsReportSchedules WHERE "+scope+" AND ReportId IN ("+ownReports+")");
      queries.put("owned_schedules","SELECT MeasurementConsumerId,ReportScheduleId,ExternalReportScheduleId FROM ReportSchedules WHERE "+scope+" AND STARTS_WITH(ExternalReportScheduleId,'"+PREFIX+"')");
      Map<String,Object> output = new LinkedHashMap<>();
      output.put("scope",MC+"/"+PREFIX);output.put("read_only",true);
      for(var e:queries.entrySet()) output.put(e.getKey(),query(connection,e.getValue()));
      query(connection,"ROLLBACK");
      System.out.println(json(output));
    } finally { await(cls("io.r2dbc.spi.Connection").getMethod("close").invoke(connection)); }
  }
}
