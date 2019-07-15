build:
	mvn spotless:check test-compile -DskipTests=true -B -V

test:
	mvn test -B jacoco:report

integration:
	mvn -Dskip.surefire.tests -DskipITs=false failsafe:integration-test@failsafe-execution

validate:
	mvn checkstyle:check@checkstyle-execution

coveralls:
	mvn coveralls:report

check-versions:
	mvn versions:display-dependency-updates