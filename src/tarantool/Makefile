all: linkbench.so

CFLAGS ?= -O3 -march=native

linkbench.so: linkbench.c
	gcc -shared $< -o $@ \
		-I/usr/include/tarantool \
		-lmsgpuck \
		$(CFLAGS) \
		-fPIC

clean:
	rm -f linkbench.so
	rm -rf 51?/ *.vylog *.snap *.xlog *.log

.PHONY: clean
