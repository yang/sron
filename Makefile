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

$(CLASS_DIR)/edu/cmu/neuron2/RonTest.class: $(CLASS_DIR) $(SRC_DIR)/edu/cmu/neuron2/*.java
	$(JAVAC) -Xlint:unchecked -d $(CLASS_DIR) $(JAVAC_FLAGS) $(SRC_DIR)/edu/cmu/neuron2/*.java

clean: $(CLASS_DIR)
	rm -rf $(CLASS_DIR)

test: $(CLASS_DIR)/edu/cmu/neuron2/RonTest.class
	./run.bash

scaleron.jar: $(CLASS_DIR)/edu/cmu/neuron2/RonTest.class $(CLASS_DIR)/edu/cmu/neuron2/FailureDataGen.class
	jar cf scaleron.jar -C $(CLASS_DIR) edu

$(CLASS_DIR)/genio/GenIo.class: gen.scala $(CLASS_DIR)
	fsc -d $(CLASS_DIR) gen.scala

jar: scaleron.jar

genio: $(CLASS_DIR)/genio/GenIo.class

gen: $(CLASS_DIR)/genio/GenIo.class
	scala -classpath $(CLASS_DIR) genio.GenIo
