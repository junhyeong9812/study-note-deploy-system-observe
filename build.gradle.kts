plugins {
	kotlin("jvm") version "2.3.21"
	kotlin("plugin.spring") version "2.3.21"
	id("org.springframework.boot") version "4.1.1"
	id("io.spring.dependency-management") version "1.1.7"
}

group = "xyz.junproject"
version = "0.0.1-SNAPSHOT"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(21)
	}
}

repositories {
	mavenCentral()
}

dependencies {
	implementation("org.springframework.boot:spring-boot-starter-webmvc")
	implementation("org.springframework.boot:spring-boot-starter-data-redis")   // 로그 스트림 소비
	implementation("org.springframework:spring-jdbc")                            // JdbcTemplate 만 — 연결 풀은 용도별로 직접 만든다
	implementation("com.zaxxer:HikariCP")
	runtimeOnly("org.postgresql:postgresql")
	implementation("org.jetbrains.kotlin:kotlin-reflect")
	implementation("tools.jackson.module:jackson-module-kotlin")
	testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
	testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
	compilerOptions {
		freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
	}
}

// 단위 테스트만 — Docker 빌드의 test 단계가 이것을 돈다(빌드 안에서는 DB 를 띄울 수 없다).
// withType 이 아니라 test 하나에만 건다 — withType 이면 engineTest 에도 excludeTags 가 붙어 engine 을 넣고 빼는 셈이 돼 아무것도 돌지 않는다.
tasks.named<Test>("test") {
	useJUnitPlatform { excludeTags("engine") }
}

// 호출자 이름(X-Caller-Service)은 코드의 상수 — 설정에서 읽으면 테스트가 알아채게 단위 · 엔진 테스트 모두에 다른 값을 둔다
tasks.withType<Test>().configureEach {
	environment("SERVER_NAME", "front")
}

// 실제 PostgreSQL·Redis 가 있어야 도는 테스트 — scripts/engine-test.sh 가 컨테이너를 띄우고 부른다
tasks.register<Test>("engineTest") {
	description = "실제 PostgreSQL·Redis 에 대고 도는 테스트"
	group = "verification"
	testClassesDirs = sourceSets["test"].output.classesDirs
	classpath = sourceSets["test"].runtimeClasspath
	useJUnitPlatform { includeTags("engine") }
	outputs.upToDateWhen { false }
}
