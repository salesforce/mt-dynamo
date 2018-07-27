build:
	mvn test-compile -DskipTests=true -Dmaven.javadoc.skip=true -B -V

test:
	mvn test -B