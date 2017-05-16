# make fetch   - download myrocks
# make deps    - apt-get install dependencies
# make build   - build MyRocks
# make install - install MyRocks into a temproary directory
# make deploy  - configure database and enter to console

all: deploy

#
# See http://myrocks.io/docs/getting-started/
#

src/.git:
	git clone --recursive https://github.com/facebook/myrocks-5.6.git $(dir $@)
fetch: src/.git

.deps: src/.git
	sudo apt update
	sudo apt -y install g++ cmake libbz2-dev libaio-dev bison \
		zlib1g-dev libsnappy-dev libgflags-dev libreadline6-dev \
		libncurses5-dev libssl-dev liblz4-dev
	touch $@
deps: .deps

src/sql/mysqld: src/.git .deps
	cd src && cmake . \
		-DCMAKE_INSTALL_PREFIX=../root \
		-DCMAKE_BUILD_TYPE=RelWithDebInfo \
		-DWITH_SSL=system \
		-DWITH_ZLIB=bundled \
		-DMYSQL_MAINTAINER_MODE=0 \
		-DENABLED_LOCAL_INFILE=1 \
		-DENABLE_DTRACE=0 \
		-DCMAKE_C_FLAGS="-march=native" \
		-DCMAKE_CXX_FLAGS="-march=native"
	${MAKE} -C src ${MAKEOPTS}
build: src/sql/mysqld

root/bin/mysqld:
	cd src && ${MAKE} install
install: root/bin/mysqld

root/data/.rocksdb/CURRENT: root/bin/mysqld
	cd root && ./scripts/mysql_install_db \
		--defaults-file=../my.cnf
	rm -f root/my*.cnf

deploy: root/data/.rocksdb/CURRENT
	@if test -f ./root/data/mysqld.pid; then \
		echo "Server is already started"; \
		exit 1; \
	fi
	cd root && ./bin/mysqld \
		--defaults-file=../my.cnf \
		--skip-grant-tables \
		--log-error=mysqld.err \
		--general_log=mysqld.log \
		--pid-file=mysqld.pid &
	sleep 2
	cat ./root/data/mysqld.err
	./root/bin/mysql < schema.sql
	./root/bin/mysql linkdb0 -e "SELECT * FROM nodetable LIMIT 0"
	./root/bin/mysql linkdb0 -e "SHOW TABLES"
	./root/bin/mysql linkdb0
	pkill -F ./root/data/mysqld.pid

clean:
	pkill -F ./root/data/mysqld.pid || :
	rm -rf root/data

bench:
	../bin/linkbench -c LinkConfig.properties -l
	../bin/linkbench -c LinkConfig.properties -r
