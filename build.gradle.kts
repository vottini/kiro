plugins {
    kotlin("jvm") version "2.0.0"
}

group = "kiro"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.8.1")
}

kotlin {
    jvmToolchain(17)
}

tasks.test {
    useJUnitPlatform()
    // ZGC keeps GC pause times below 1 ms, preventing route-table eviction in large coroutine simulations.
    jvmArgs("-Xmx1g", "-XX:+UseZGC")
}
