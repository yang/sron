# vim:noet:sw=4:ts=4

TOP_DIR = .
SRC_DIR = .
CLASS_DIR = $(TOP_DIR)/classes
#JAVAC_FLAGS = -g -deprecation
JAVAC_FLAGS = -O
JAVAC = javac
JAVA = java

all: compile

compile: $(CLASS_DIR)/edu/cmu/neuron2/RonTest.class

# create target class dir if not present.
$(CLASS_DIR):
	mkdir -p $(CLASS_DIR)

JAR_DEPS = ext/mina-core-1.1.2.jar:ext/slf4j-api-1.4.3.jar:ext/slf4j-simple-1.4.3.jar

$(CLASS_DIR)/edu/cmu/neuron2/RonTest.class: $(CLASS_DIR) $(SRC_DIR)/edu/cmu/neuron2/*.java
	$(JAVAC) -d $(CLASS_DIR) -classpath $(JAR_DEPS) $(JAVAC_FLAGS) $(SRC_DIR)/edu/cmu/neuron2/*.java

clean: $(CLASS_DIR)
	rm -rf $(CLASS_DIR)

test: $(CLASS_DIR)/edu/cmu/neuron2/RonTest.class
	./run.bash

scaleron.jar: $(CLASS_DIR)/edu/cmu/neuron2/RonTest.class
	jar cf scaleron.jar -C $(CLASS_DIR) .

jar: scaleron.jar
