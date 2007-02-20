TOP_DIR		=.

# set here the target dir for all classes
CLASS_DIR       =$(TOP_DIR)/classes

# compiler field
#JDEBUGFLAGS	= -g -deprecation
#JDEBUGFLAGS	= -O -depend -nowarn
JCC		= javac
JAVA		= java

all: clean compile

LOCAL_CLASS_DIR       = $(CLASS_DIR)

# create target class dir if not present.
$(LOCAL_CLASS_DIR):
	mkdir -p $(LOCAL_CLASS_DIR)

# new rule for java
.SUFFIXES: .java .class


# magical command that tells make to find class files in another dir
vpath %.class $(LOCAL_CLASS_DIR)


compile:
	CLASSPATH=$(CLASS_DIR):$(TOP_DIR) $(JCC) -nowarn -d $(CLASS_DIR) $(JDEBUGFLAGS) $(TOP_DIR)/edu/cmu/nuron/RonTest.java

clean:$(LOCAL_CLASS_DIR)
	@@ echo 'rm -f $(LOCAL_CLASS_DIR)/*class'
	@@rm -f $(LOCAL_CLASS_DIR)/*class

test:
	CLASSPATH=$(CLASS_DIR) $(JAVA) edu.cmu.nuron.RonTest 4 localhost 8100 0
