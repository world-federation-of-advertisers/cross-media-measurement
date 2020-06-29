package org.wfanet.measurement.db.gcp.testing

import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.Instance
import com.google.cloud.spanner.InstanceConfigId
import com.google.cloud.spanner.InstanceId
import com.google.cloud.spanner.InstanceInfo
import com.google.cloud.spanner.Spanner
import com.google.cloud.spanner.SpannerOptions
import org.junit.rules.TestRule
import org.wfanet.measurement.common.testing.CloseableResource

/** [TestRule] to start a Cloud Spanner Emulator with a temporary Spanner [Instance]. */
class SpannerEmulatorRule : EmulatorRule by EmulatorRuleImpl()

private interface EmulatorRule : TestRule {
  val instance: Instance
  fun getDatabaseClient(databaseId: DatabaseId): DatabaseClient
}

private class EmulatorRuleImpl : EmulatorRule,
  CloseableResource<TemporaryEmulatorInstance>({ TemporaryEmulatorInstance() }) {

  override val instance
    get() = resource.instance

  override fun before() {
    resource.init()
  }

  override fun getDatabaseClient(databaseId: DatabaseId): DatabaseClient =
    resource.spanner.getDatabaseClient(databaseId)
}

/**
 * [AutoCloseable] resource wrapping a temporary [SpannerEmulator] with a single [Instance].
 */
private class TemporaryEmulatorInstance : AutoCloseable {
  companion object {
    private const val PROJECT_ID = "test-project"
    private const val INSTANCE_NAME = "test-instance"
    private const val INSTANCE_DISPLAY_NAME = "Test Instance"
    private const val INSTANCE_CONFIG = "emulator-config"
  }

  private lateinit var spannerEmulator: SpannerEmulator
  lateinit var spanner: Spanner
    private set
  lateinit var instance: Instance
    private set

  fun init() {
    check(!this::spannerEmulator.isInitialized)

    spannerEmulator = SpannerEmulator()
    spannerEmulator.start()
    val emulatorHost = spannerEmulator.blockUntilReady()

    val spannerOptions =
      SpannerOptions.newBuilder().setProjectId(PROJECT_ID).setEmulatorHost(emulatorHost).build()
    spanner = spannerOptions.service
    instance = spanner.instanceAdminClient.createInstance(
      InstanceInfo
        .newBuilder(InstanceId.of(PROJECT_ID, INSTANCE_NAME))
        .setDisplayName(INSTANCE_DISPLAY_NAME)
        .setInstanceConfigId(InstanceConfigId.of(PROJECT_ID, INSTANCE_CONFIG))
        .setNodeCount(1)
        .build()
    ).get()
  }

  override fun close() {
    if (this::instance.isInitialized) {
      instance.delete()
    }
    if (this::spanner.isInitialized) {
      spanner.close()
    }
    if (this::spannerEmulator.isInitialized) {
      spannerEmulator.close()
    }
  }
}
