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
    // Compatibility between MINOR update is not working at the moment:
    // https://github.com/opensearch-project/OpenSearch/issues/18787
    opensearchCompatibility = OpensearchCompatibility.REVISION,
)

version = Versions.project
