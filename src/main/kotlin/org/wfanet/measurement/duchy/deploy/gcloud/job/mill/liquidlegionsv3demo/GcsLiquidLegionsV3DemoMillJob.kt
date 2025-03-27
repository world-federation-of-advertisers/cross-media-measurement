package org.wfanet.measurement.duchy.deploy.gcloud.job.mill.liquidlegionsv3demo

import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.duchy.mill.liquidlegionsv3demo.LiquidLegionsV3DemoMill
import org.wfanet.measurement.gcloud.gcs.GcsFromFlags
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import picocli.CommandLine

@CommandLine.Command(
  name = "GcsLiquidLegionsV3DemoMillJob",
  description = ["Liquid Legions V3 TEE demo mill job."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
class GcsLiquidLegionsV3DemoMillJob : Runnable {
  @CommandLine.Mixin private lateinit var gcsFlags: GcsFromFlags.Flags

  override fun run() {
    val gcsStorageClient = GcsStorageClient.fromFlags(GcsFromFlags(gcsFlags))
    LiquidLegionsV3DemoMill(gcsStorageClient).run()
  }
}

fun main(args: Array<String>) = commandLineMain(GcsLiquidLegionsV3DemoMillJob(), args)
