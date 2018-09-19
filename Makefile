build:
	mvn test-compile -DskipTests=true -B -V

test:
	mvn test -B jacoco:report

javadoc:
	mvn javadoc:javadoc@javadoc-execution

checkstyle:
	mvn checkstyle:check@checkstyle-execution

coveralls:
	mvn coveralls:report

check-versions:
	mvn versions:display-dependency-updates