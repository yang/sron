# vim:noet:sw=4:ts=4

TOP_DIR = .
SRC_DIR = .
CLASS_DIR = $(TOP_DIR)/classes
JAVAC_FLAGS = -O
JAVAC = javac
JAVA = java

all: compile
compile: $(CLASS_DIR)/sron/RonTest.class
jar: scaleron.jar

# create target class dir if not present.
$(CLASS_DIR):
	mkdir -p $(CLASS_DIR)

$(CLASS_DIR)/sron/RonTest.class: $(CLASS_DIR) $(SRC_DIR)/sron/*.java
	$(JAVAC) -Xlint:unchecked -d $(CLASS_DIR) $(JAVAC_FLAGS) $(SRC_DIR)/sron/RonTest.java

clean: $(CLASS_DIR)
	rm -rf $(CLASS_DIR)

test: $(CLASS_DIR)/sron/RonTest.class
	./run.bash

scaleron.jar: $(CLASS_DIR)/sron/RonTest.class $(CLASS_DIR)/sron/FailureDataGen.class
	jar cf scaleron.jar -C $(CLASS_DIR) sron
