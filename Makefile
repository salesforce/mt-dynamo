build:
	mvn spotless:check test-compile -DskipTests=true -B -V

test:
	mvn test -B jacoco:report

integration:
	mvn verify -Pintegration-tests -Dskip.surefire.tests

validate:
	mvn checkstyle:check@checkstyle-execution dependency:analyze@analyze

coveralls:
	mvn coveralls:report

check-versions:
	mvn versions:display-dependency-updates