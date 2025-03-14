plugins {
    id("java")
}

group = "org.ds"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
    implementation("org.apache.hadoop:hadoop-common:3.4.1")
    // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-client-core
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.4.1")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(8))
    }
}

tasks.withType<JavaCompile> {
    sourceCompatibility = "1.8"
    targetCompatibility = "1.8"
}

tasks.test {
    useJUnitPlatform()
}