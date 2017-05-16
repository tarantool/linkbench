all: deploy

src/.git:
	git clone --recursive https://github.com/tarantool/tarantool.git -b 1.7 $(dir $@)
fetch: src/.git

.deps: src/.git
	sudo apt update
	sudo apt -y install gcc g++ cmake libbz2-dev zlib1g-dev libreadline6-dev \
		libncurses5-dev libssl-dev
	touch $@
deps: .deps

src/src/tarantool: src/.git .deps
	cd src && cmake . \
		-DCMAKE_INSTALL_PREFIX=../root \
		-DCMAKE_BUILD_TYPE=RelWithDebInfo \
		-DCMAKE_C_FLAGS="-march=native" \
		-DCMAKE_CXX_FLAGS="-march=native"
	${MAKE} -C src ${MAKEOPTS}
build: src/src/tarantool

cfunc.so: cfunc.c
	gcc -shared -O3 -march=native $< -o $@ \
		-Isrc/src -I./src/src/trivia  \
		-I./src/src/lib/msgpuck \
		-L./src/src/lib/msgpuck \
		-lmsgpuck \
		-Isrc/third_party/luajit/src \
		-fPIC

deploy: src/src/tarantool cfunc.so
	./src/src/tarantool app.lua

clean:
	rm -f cfunc.so
	rm -rf 51?/ *.xlog *.snap *.vylog *.log

bench:
	../bin/linkbench -c LinkConfig.properties -l
	../bin/linkbench -c LinkConfig.properties -r
