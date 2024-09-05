package org.wfanet.panelmatch.client.exchangetasks.remote.aws

/**
 * Emr Serverless client interface to manage exchange task applications
 * on the AWS EMR Serverless service.
 */
interface EmrServerlessClient {
  suspend fun createApplication(applicationName: String): String

  suspend fun startApplication(applicationId: String): Boolean

  suspend fun stopApplication(applicationId: String): Boolean

  suspend fun startAndWaitJobRunCompletion(
    jobRunName: String,
    applicationId: String,
    arguments: List<String>,
  ): Boolean
}
