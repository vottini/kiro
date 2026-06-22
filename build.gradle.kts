
plugins {
    alias(libs.plugins.kotlin.multiplatform)
    alias(libs.plugins.android.library)
}

group = "systems.untangle"
version = "0.0.1"

kotlin {
    jvmToolchain(17)
    jvm()
    androidTarget()

    sourceSets {
        // Intermediate source set: both JVM and Android inherit from here.
        // Java APIs (ConcurrentHashMap, AtomicLong, java.time.*) are available
        // on both targets so all library code lives here.
        val jvmAndAndroidMain by creating {
            dependsOn(commonMain.get())
            dependencies {
                implementation(libs.kotlinx.coroutines.core)
            }
        }
        jvmMain   { dependsOn(jvmAndAndroidMain) }
        androidMain { dependsOn(jvmAndAndroidMain) }

        jvmTest {
            dependencies {
                implementation(kotlin("test-junit5"))
                implementation(libs.junit.jupiter)
                implementation(libs.kotlinx.coroutines.test)
            }
        }
    }
}

android {
    namespace = "systems.untangle.kiro"
    compileSdk = 35
    defaultConfig {
        minSdk = 26  // required for java.time.* APIs
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
}

tasks.named<Test>("jvmTest") {
    useJUnitPlatform()
    jvmArgs("-Xmx1g", "-XX:+UseZGC")
}
