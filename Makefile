build:
	mvn spotless:check test-compile -DskipTests=true -B -V

test:
	mvn test -B jacoco:report
	# treat s3 integration tests as unit tests as these tests starts a mock s3 docker image,
	# not dissimilar to starting local dynamo in most existing unit tests, and runs as quick
	mvn verify -Ps3-integration-tests -Dskip.surefire.tests -B jacoco:report


integration:
	mvn verify -Paws-integration-tests -Dskip.surefire.tests

validate:
	mvn checkstyle:check@checkstyle-execution dependency:analyze@analyze

codecov:
    mvn cobertura:cobertura

check-versions:
	mvn versions:display-dependency-updates