CXX = clang++
CXXFLAGS = -g -Wall -Wextra -pedantic -std=c++14 $(PKGFLAGS) -Wno-unused-parameter
PKGFLAGS = `pkg-config fuse --cflags --libs`

all: cleanup fatfs

fatfs: fatfs.cc

cleanup:
	-fusermount -u mnt

test: cleanup fatfs
	# Unmount, ignore errors
	
	./fatfs filestore mnt -s -d
	# ls -l
