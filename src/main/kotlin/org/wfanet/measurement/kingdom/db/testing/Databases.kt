package org.wfanet.measurement.kingdom.db.testing

import org.wfanet.measurement.kingdom.db.LegacySchedulingDatabase
import org.wfanet.measurement.kingdom.db.ReportDatabase
import org.wfanet.measurement.kingdom.db.RequisitionDatabase

data class Databases(
  val legacySchedulingDatabase: LegacySchedulingDatabase,
  val reportDatabase: ReportDatabase,
  val requisitionDatabase: RequisitionDatabase,
  val databaseTestHelper: DatabaseTestHelper
)
