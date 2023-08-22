package org.wfanet.measurement.duchy.deploy.aws.server

import com.google.gson.Gson
import io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider
import io.r2dbc.postgresql.client.SSLMode
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions
import org.wfanet.measurement.common.db.postgres.PostgresFlags
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse

data class PostgresCredential(
  val username: String,
  val password: String,
)

object PostgresConnectionFactories {
  @JvmStatic
  fun buildConnectionFactory(flags: PostgresFlags): ConnectionFactory {
//          .region(Region.of(flags.region))
    val secretManagerCli = SecretsManagerClient.builder()
      .region(Region.of("us-west-2"))
      .credentialsProvider(DefaultCredentialsProvider.create())
      .build()

    val valueRequest = GetSecretValueRequest.builder()
      .secretId("rds!db-df84f8e7-ae03-4650-9ca6-04b0c465d621")
      .build()

    val valueResponse: GetSecretValueResponse = secretManagerCli.getSecretValue(valueRequest)
    val secret = valueResponse.secretString()
    println("secrets: $secret")

//    val secret = secretManagerCli.getSecretValue{ GetSecretValueRequest.builder().secretId(flags.credentialSecretName)}.secretString()
//    val secret = secretManagerCli.getSecretValue{ GetSecretValueRequest.builder().secretId("rds!db-df84f8e7-ae03-4650-9ca6-04b0c465d621").build() }.secretString()
    val postgresCredential = Gson().fromJson(secret, PostgresCredential::class.java)

    return ConnectionFactories.get(
      ConnectionFactoryOptions.builder()
        .option(
          ConnectionFactoryOptions.DRIVER,
          PostgresqlConnectionFactoryProvider.POSTGRESQL_DRIVER
        )
        .option(ConnectionFactoryOptions.PROTOCOL, "postgresql")
        .option(ConnectionFactoryOptions.USER, postgresCredential.username)
        .option(ConnectionFactoryOptions.PASSWORD, postgresCredential.password)
        .option(ConnectionFactoryOptions.DATABASE, flags.database)
        .option(ConnectionFactoryOptions.HOST, flags.host)
        .option(PostgresqlConnectionFactoryProvider.SSL_MODE, SSLMode.REQUIRE)
        .build()
    )
  }
}
