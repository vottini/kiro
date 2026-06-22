
plugins {
    alias(libs.plugins.kotlin.jvm)
}

group = "systems.untangle"
version = "0.0.1"

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.kotlinx.coroutines.core)
    testImplementation(kotlin("test"))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.kotlinx.coroutines.test)
}

kotlin {
    jvmToolchain(17)
}

tasks.test {
    useJUnitPlatform()
    // ZGC keeps GC pause times below 1 ms, preventing route-table eviction in large coroutine simulations.
    jvmArgs("-Xmx1g", "-XX:+UseZGC")
}
