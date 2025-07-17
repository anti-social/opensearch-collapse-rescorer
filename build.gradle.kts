plugins {
    java
    idea
    id("opensearch.opensearchplugin")
    id("opensearch.java-agent")
    id("com.netflix.nebula.ospackage")
}

configure<JavaPluginExtension> {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

configureOpensearchPlugin(
    name = "collapse-extension",
    description = "Adds rescorer for mixing up search hits inside their groups.",
    classname = "dev.evo.opensearch.collapse.CollapseRescorePlugin",
    numberOfTestClusterNodes = 2,
)

version = Versions.project
