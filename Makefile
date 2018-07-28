build:
	mvn test-compile -DskipTests=true -Dmaven.javadoc.skip=true -B -V

test:
	mvn test -B jacoco:report

coveralls:
	mvn coveralls:report

check-versions:
	mvn versions:display-dependency-updates