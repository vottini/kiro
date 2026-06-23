plugins {
    id("org.jetbrains.kotlin.jvm")
    alias(libs.plugins.shadow)
    application
}

dependencies {
    implementation(project(":"))
    implementation(libs.kotlinx.coroutines.core)
}

application {
    mainClass = "systems.untangle.kiro.demo.MainKt"
}

tasks.shadowJar {
    archiveBaseName = "kiro-demo"
    archiveClassifier = ""
    archiveVersion = ""
    mergeServiceFiles()
    exclude { it.name == "module-info.class" }
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
}
