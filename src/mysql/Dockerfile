FROM ubuntu:xenial

#
# Based on http://myrocks.io/docs/getting-started/
#

# Skip interactive post-install scripts
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends  \
        git \
        ca-certificates \
        build-essential \
        cmake \
        libbz2-dev \
        libaio-dev \
        bison \
        zlib1g-dev \
        libsnappy-dev \
        libgflags-dev \
        libreadline6-dev \
        libncurses5-dev \
        libssl-dev \
        liblz4-dev && \
    rm -rf /var/lib/apt/lists/*

# Get MySQL sources && build mysql
WORKDIR /usr/src
RUN git clone --recursive https://github.com/facebook/mysql-5.6.git mysql \
        && \
    cd mysql \
        && \
    cmake . \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DINSTALL_LAYOUT=DEB \
        -DSYSCONFDIR=/etc/mysql \
        -DINSTALL_SCRIPTDIR=bin \
        -DINSTALL_MYSQLSHAREDIR=share/mysql \
        -DINSTALL_INCLUDEDIR=include/mysql \
        -DINSTALL_DOCDIR=share/doc/mysql \
        -DINSTALL_DOCREADMEDIR=share/doc/mysql \
        -DINSTALL_SUPPORTFILESDIR=share/mysql \
        -DINSTALL_SQLBENCHDIR=share \
        -DINSTALL_MYSQLTESTDIR=share/mysql-test \
        -DMYSQL_DATADIR=/var/lib/mysql \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DWITH_SSL=system \
        -DWITH_ZLIB=bundled \
        -DMYSQL_MAINTAINER_MODE=OFF \
        -DENABLE_DTRACE=OFF \
        -DWITH_DEBUG=OFF \
        -DWITH_EMBEDDED_SERVER=OFF \
        -DCMAKE_C_FLAGS="-O3 -mtune=generic" \
        -DCMAKE_CXX_FLAGS="-O3 -mtune=generic" \
        && \
    make -j8 \
        && \
    make install \
        && \
    mkdir /var/lib/mysql-files \
        && \
    rm -rf \
        /usr/local/share/mysql-test/ \
        /usr/local/share/sql-bench/ \
        /usr/local/share/doc/mysql \
        && \
    cd .. \
        && \
    rm -rf mysql

VOLUME /var/lib/mysql
WORKDIR /var/lib/mysql
ADD my.cnf /etc/mysql/
ADD mysqld-linkbench /usr/local/bin/
ADD linkbench.sql /etc/mysql/linkbench.sql
EXPOSE 3306
CMD ["/usr/local/bin/mysqld-linkbench"]
