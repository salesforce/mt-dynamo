build:
	mvn test-compile -DskipTests=true -B -V

test:
	mvn test -B jacoco:report

validate:
	mvn javadoc:javadoc@javadoc-execution
	mvn checkstyle:check@checkstyle-execution

coveralls:
	mvn coveralls:report

check-versions:
	mvn versions:display-dependency-updates