import java.nio.file.Paths

buildscript {
    val opensearchVersion = project.properties["opensearchVersion"] ?: "2.18.0"
    repositories {
        mavenLocal()
        mavenCentral()
    }
    dependencies {
        classpath("org.opensearch.gradle:build-tools:$opensearchVersion")
    }
}

plugins {
    java
    idea
    id("org.ajoberstar.grgit") version "4.1.0"
    id("com.netflix.nebula.ospackage") version "11.9.0"
}

apply {
    plugin("opensearch.opensearchplugin")
    plugin("opensearch.testclusters")
}

group = "dev.evo.opensearch"

val lastTag = grgit.describe(mapOf("match" to listOf("v*"), "tags" to true)) ?: "v0.0.0"
val pluginVersion = lastTag.trimStart('v')
val versions = org.opensearch.gradle.VersionProperties.getVersions() as Map<String, String>
version = "$pluginVersion-opensearch${versions["opensearch"]}"

val distDir = Paths.get(project.layout.buildDirectory.toString(), "distributions")

repositories {
    mavenCentral()
}

dependencies {
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

val pluginName = "collapse-extension"

configure<org.opensearch.gradle.plugin.PluginPropertiesExtension> {
    name = pluginName
    description = "Adds rescorer for mixing up search hits inside their groups."
    classname = "dev.evo.opensearch.collapse.CollapseRescorePlugin"
    version = project.version.toString()
    licenseFile = project.file("LICENSE.txt")
    noticeFile = project.file("NOTICE.txt")
}

configure<NamedDomainObjectContainer<org.opensearch.gradle.testclusters.OpenSearchCluster>> {
    val integTestCluster = create("integTest") {
        setTestDistribution(org.opensearch.gradle.testclusters.TestDistribution.INTEG_TEST)
        // setTestDistribution(org.opensearch.gradle.testclusters.TestDistribution.ARCHIVE)
        numberOfNodes = 2
        // module("")
        plugin(tasks.named<Zip>("bundlePlugin").get().archiveFile)
    }

    val integTestTask = tasks.register<org.opensearch.gradle.test.RestIntegTestTask>("integTest") {
        dependsOn("bundlePlugin")
    }

    tasks.named("check") {
        dependsOn(integTestTask)
    }
}

tasks.register("listRepos") {
    doLast {
        println("Repositories:")
        project.repositories.forEach {
            print("- ")
            if (it is MavenArtifactRepository) {
                println("Name: ${it.name}; url: ${it.url}")
            } else if (it is IvyArtifactRepository) {
                println("Name: ${it.name}; url: ${it.url}")
            } else {
                println("Unknown repository type: $it")
            }
        }
    }
}

tasks.register("deb", com.netflix.gradle.plugins.deb.Deb::class) {
    dependsOn("bundlePlugin")

    packageName = project.name
    requires("opensearch", versions["opensearch"])

    from(zipTree(tasks["bundlePlugin"].outputs.files.singleFile))

    val esHome = project.properties["opensearchHome"] ?: "/usr/share/opensearch"
    into("$esHome/plugins/${pluginName}")

    doLast {
        if (properties.containsKey("assembledInfo")) {
            distDir.resolve("assembled-deb.filename").toFile()
                .writeText(assembleArchiveName())
        }
    }
}
tasks.named("assemble") {
    dependsOn("deb")
}
