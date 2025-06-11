package org.wfanet.measurement.edpaggregator

import java.io.File

/** Configuration for storage clients. */
data class StorageConfig(val rootDirectory: File? = null, val projectId: String? = null)
