#!/bin/sh

export JAVA_HOME=/opt/jdk17.0.2/Contents/Home

$JAVA_HOME/bin/java --version

./gradlew --version

./gradlew clean

./gradlew :fineract-provider:spotlessApply

./gradlew --stacktrace --no-daemon -x rat -x compileTestJava -x test clean bootJar


