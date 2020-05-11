import com.google.protobuf.gradle.*

group = "org.wfanet"
version = "1.0-SNAPSHOT"

repositories {
    mavenLocal()
    google()
    jcenter()
    mavenCentral()
}

buildscript {
    repositories {
        google()
        jcenter()
        mavenCentral()
        mavenLocal()
    }
    dependencies {
        classpath("com.google.protobuf:protobuf-gradle-plugin:0.8.12")
        classpath("org.jetbrains.kotlin:kotlin-gradle-plugin:1.3.72")
    }
}

plugins {
    kotlin("jvm") version "1.3.72"
    id("com.google.protobuf") version "0.8.12"
    idea
    application
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.3.3")
    implementation("io.grpc:grpc-kotlin-stub:0.1.1")
    implementation("com.google.protobuf:protobuf-java:3.11.1")
    implementation("com.google.protobuf:protobuf-java-util:3.11.1")
    implementation("io.grpc:grpc-netty-shaded:1.28.1")
    implementation("io.grpc:grpc-protobuf:1.28.1")
    implementation("io.grpc:grpc-stub:1.28.1")
    compileOnly("javax.annotation:javax.annotation-api:1.2")
}

protobuf {
    protoc { artifact = "com.google.protobuf:protoc:3.6.1" }
    plugins {
        id("grpc") { artifact = "io.grpc:protoc-gen-grpc-java:1.15.1" }
        id("grpckt") { artifact = "io.grpc:protoc-gen-grpc-kotlin:0.1.1" }
    }
    generateProtoTasks {
        ofSourceSet("main").forEach {
            it.plugins {
                id("grpc")
                id("grpckt")
            }
        }
    }
}

tasks {
    compileKotlin {
        kotlinOptions.jvmTarget = "1.8"
    }
    compileTestKotlin {
        kotlinOptions.jvmTarget = "1.8"
    }
}