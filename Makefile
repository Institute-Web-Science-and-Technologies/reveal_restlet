build:
	mvn package
run:
	mvn exec:exec -Dexec.executable="java" "-Dexec.args=-classpath %classpath uniko.west.reveal_restlet.RevealRouter"
