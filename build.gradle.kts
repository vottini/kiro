
plugins {
    alias(libs.plugins.kotlin.multiplatform)
    alias(libs.plugins.android.library)
    alias(libs.plugins.maven.publish)
}

group = "systems.untangle"
version = "0.1.1"

kotlin {
    jvmToolchain(17)
    jvm()
    android {
        namespace = "systems.untangle.kiro"
        compileSdk = 35
        minSdk = 26  // required for java.time.* APIs
    }

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

        jvmMain     { dependsOn(jvmAndAndroidMain) }
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

mavenPublishing {
    publishToMavenCentral()
    signAllPublications()

    coordinates(
        group.toString(),
        "kiro",
        version.toString()
    )

    pom {
        name = "Kiro"
        description = "BATMAN routing algorithm implemented in Kotlin at application layer"
        inceptionYear = "2026"
        url = "https://github.com/vottini/kiro"

        licenses {
            license {
                name = "MIT"
                url = "https://opensource.org/licenses/MIT"
            }
        }

        developers {
            developer {
                name = "Gustavo Venturini"
                email = "gustavo.c.venturini@gmail.com"
            }
        }

        scm {
            url = "https://github.com/vottini/kiro"
            connection = "scm:git:git://github.com/vottini/kiro.git"
            developerConnection = "scm:git:ssh://git@github.com/vottini/kiro.git"
        }
    }
}

tasks.named<Test>("jvmTest") {
    useJUnitPlatform()
    jvmArgs("-Xmx1g", "-XX:+UseZGC")
}
