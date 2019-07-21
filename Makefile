build:
	mvn spotless:check test-compile -DskipTests=true -B -V

test:
	mvn test -B jacoco:report
	# dont treat this as an integration test,
	# as this starts mock s3 similar to local dynamo, and runs as quick
	mvn verify -Ps3-integration-tests -Dskip.surefire.tests


integration:
	mvn verify -Paws-integration-tests -Dskip.surefire.tests

validate:
	mvn checkstyle:check@checkstyle-execution dependency:analyze@analyze

coveralls:
	mvn coveralls:report

check-versions:
	mvn versions:display-dependency-updates