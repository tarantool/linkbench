FROM ubuntu:xenial

# Skip interactive post-install scripts
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends  \
        git \
        ca-certificates \
        build-essential \
        cmake \
        zlib1g-dev \
        libiberty-dev \
         binutils-dev \
        libsnappy-dev \
        libgflags-dev \
        libreadline6-dev \
        libncurses5-dev \
        libssl-dev \
        liblz4-dev \
        libgoogle-perftools-dev \
        google-perftools \
        libicu-dev \
        libcurl4-openssl-dev \
    && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src
RUN git clone --recursive https://github.com/tarantool/tarantool.git -b 1.7 tarantool \
        && \
    cd tarantool && cmake . \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DCMAKE_C_FLAGS="-O3 -mtune=generic" \
        -DCMAKE_CXX_FLAGS="-O3 -mtune=generic" \
        && \
    make -j8 && make install \
        && \
    cd .. && rm -rf tarantool

RUN git clone --recursive https://github.com/tarantool/msgpuck.git msgpuck \
        && \
    cd msgpuck && cmake . \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DCMAKE_C_FLAGS="-O3 -mtune=generic" \
        -DCMAKE_CXX_FLAGS="-O3 -mtune=generic" \
        && \
    make -j8 && make install && \
    cd .. && rm -rf msgpuck

RUN git clone --recursive https://github.com/tarantool/gperftools.git gperftools \
        && \
    mkdir -p /usr/local/share/tarantool && \
    mv gperftools/gperftools /usr/local/share/tarantool/ && \
    rm -rf gperftools

ADD . /linkbenchapp
RUN CFLAGS="-O3 -mtune=generic" make -C /linkbenchapp

VOLUME /var/lib/tarantool
WORKDIR /var/lib/tarantool
EXPOSE 3301
CMD ["/linkbenchapp/app.lua"]
