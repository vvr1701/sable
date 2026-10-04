import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
    `java-library`
}


tasks.withType<JavaCompile> {
    options.release.set(17)
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.11.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
    maxHeapSize = "1g"
    systemProperty("sable.crash.iterations", findProperty("crashIterations") ?: "15")
    testLogging {
        events("failed")
        exceptionFormat = TestExceptionFormat.FULL
    }
}

tasks.register<JavaExec>("bench") {
    classpath = sourceSets.test.get().runtimeClasspath
    mainClass.set("io.github.vvr1701.sable.Bench")
    args((findProperty("args") as String? ?: "").split(" ").filter { it.isNotEmpty() })
    jvmArgs("-Xmx2g")
}
