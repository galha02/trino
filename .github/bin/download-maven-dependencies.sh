#!/usr/bin/env bash

set -exuo pipefail

RETRY=".github/bin/retry"
MAVEN_ONLINE="${MAVEN//--offline/}"

# We do "quick build" now just to see

./mvnw clean deploy -Dtest=void -Dit.test=void -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false -DfailIfNoTests=false -Dmaven.install.skip=true -Dmaven.deploy.skip=true -Dtakari.skip=true -P gib -Dgib.disable -P errorprone-compiler -P ci -b smart -T2C -ntp -Dmaven.javadoc.offline=true

# Run download tools without any profiles to use active-by-default profiles
#$RETRY $MAVEN_ONLINE -B dependency:go-offline -Dsilent
#$RETRY $MAVEN_ONLINE -B de.qaware.maven:go-offline-maven-plugin:resolve-dependencies

# Downloading dependencies is used to populate the maven cache shared between PRs, so PR-specific GIB state needs to be ignored

# Enable common profiles to make sure their plugin dependencies are downloaded as well
# GIB should be disabled even though it's profile is active, to make sure it doesn't skip any submodules
#$RETRY $MAVEN_ONLINE -B -P ci,errorprone-compiler ${MAVEN_GIB} -Dgib.disable dependency:go-offline -Dsilent
#$RETRY $MAVEN_ONLINE -B -P ci,errorprone-compiler ${MAVEN_GIB} -Dgib.disable de.qaware.maven:go-offline-maven-plugin:resolve-dependencies

# TODO: Remove next step once https://github.com/qaware/go-offline-maven-plugin/issues/28 is fixed
# trino-pinot overrides some common dependency versions, focus on it to make sure those overrides are downloaded as well
#$RETRY $MAVEN_ONLINE -B -P ci,errorprone-compiler ${MAVEN_GIB} -Dgib.disable de.qaware.maven:go-offline-maven-plugin:resolve-dependencies -pl ':trino-pinot'

# Add more dynamic dependencies in the configuration section of the go-offline-maven-plugin in the root pom.xml
