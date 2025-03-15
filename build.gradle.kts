plugins {
    id("java")
    application
}

group = "org.ds.kmeans"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // Apache Hadoop dependencies.
    // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
    implementation("org.apache.hadoop:hadoop-common:3.4.1")
    // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-client-core
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.4.1")

    // JUnit testing dependencies - not necessary for this project.
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(8))
    }
}

tasks.test {
    useJUnitPlatform()
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "org.ds.kmeans.Driver"
    }
}

application {
    mainClass.set("org.ds.kmeans.Driver")
}