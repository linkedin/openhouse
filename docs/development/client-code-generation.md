## Workflow
# Produce OpenApi Spec for the server
To enable a server to produce OpenApi spec, just include the following code block in its build.gradle:
```
plugin {
  id 'com.github.johnrengelman.processes' version '0.5.0'
  id 'org.springdoc.openapi-gradle-plugin' version '1.3.3'
  id 'openhouse.service-specgen-convention'
}
```
This will create the spec file in `<Root>/build/<Service>/specs/`.

# Generate client for the server
Each generated client should have its corresponding module. These modules are placed in `:client`, for example
`:client:hts`.  Just add a build.gradle with following code block:
```
plugins {
  id 'java'
  id 'openhouse.client-codegen-convention'
  id 'openhouse.springboot-conventions'
}

ext {
  codeGenForService = ":services:housetables"
}

apply from: "${project(':client:common').file("codegen.build.gradle")}"
```
Note: the service should be enabled to produce spec file.

## FAQ
### The service controller was changed on my local build, but the generated client doesn't reflect it. WTD?
OpenApi gradle plugin doesn't reproduce the spec file when code change happens. Because of this, the spec doesn't contain
the latest Api details. We can re-trigger the spec generation by doing a `:clean` on the server module.

Client codegen will be automatically triggered if it detects a spec file change.

Steps involved for `:hts` would be:
```
./gradlew :service:housetables:clean :client:hts:build 
```
