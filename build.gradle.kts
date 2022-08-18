import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.7.10"
    application
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
    // Other Dependencies
    implementation("com.google.inject:guice:5.1.0")
    implementation("org.apache.logging.log4j:log4j-core:2.17.2")
    implementation("org.apache.logging.log4j:log4j-api:2.17.2")
    implementation("org.opensearch.client:opensearch-rest-high-level-client:2.1.0")
    implementation("io.github.acm19:aws-request-signing-apache-interceptor:2.1.1")
    implementation("org.opensearch.client:opensearch-rest-high-level-client:2.1.0")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

application {
    mainClass.set("com.gvnavin.test.Main")
}