all: cfunc.so

cfunc.so: cfunc.c
	gcc -shared $< -o $@ -I/usr/include/tarantool -lmsgpuck -fPIC

clean:
	rm -f cfunc.so

.PHONY: clean
