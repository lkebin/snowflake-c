PACKAGE = gid
BUILD_DIR = ./build

CC = cc

all: dir
	$(CC) -Wall -o $(BUILD_DIR)/$(PACKAGE) -lgearman *.c

dir:
	mkdir -p $(BUILD_DIR)

.PHONY: all dir
