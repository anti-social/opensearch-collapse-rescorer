plugins {
    java
    idea
    id("opensearch.opensearchplugin")
    id("com.netflix.nebula.ospackage")
}

configureOpensearchPlugin(
    name = "collapse-extension",
    description = "Adds rescorer for mixing up search hits inside their groups.",
    classname = "dev.evo.opensearch.collapse.CollapseRescorePlugin",
)

version = Versions.project
