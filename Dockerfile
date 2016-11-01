FROM alpine:3.5

RUN apk update && apk upgrade &&\
        apk add --no-cache \
        git \
        openjdk7 \
        python \
	gcc \
	g++ \
	libressl-dev \
	readline-dev \
	cmake \
	make \
	linux-headers \
        py-pip

ENV MAVEN_VERSION="3.3.9" \
    M2_HOME=/usr/lib/mvn
ENV JAVA_HOME=/usr/lib/jvm/java-1.7-openjdk
ENV JAVA=$JAVA_HOME/bin
ENV M2=$M2_HOME/bin
ENV PATH=$PATH:$JAVA_HOME:$JAVA:$M2_HOME:$M2

#install maven
RUN apk add --update wget && \
  cd /tmp && \
  wget "http://ftp.unicamp.br/pub/apache/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz" && \
  tar -zxvf "apache-maven-$MAVEN_VERSION-bin.tar.gz" && \
  mv "apache-maven-$MAVEN_VERSION" "$M2_HOME" && \
  ln -s "$M2_HOME/bin/mvn" /usr/bin/mvn && \
  apk del wget && \
  rm /tmp/* /var/cache/apk/*

RUN git clone https://github.com/tarantool/tarantool -b 1.7 && \
	cd tarantool && \
	git submodule init && \
	git submodule update --recursive && \
	cmake . && \
	make

RUN git clone https://github.com/IlyaMarkovMipt/linkbench &&\
	cd linkbench && \
	mvn package -Dmaven.test.skip=true

RUN cd linkbench && \
	mvn package -Dmaven.test.skip=true

RUN cd linkbench/src/tarantool_scripts && \
	gcc -shared cfunc.c -o cfunc.so -I ../../../tarantool/src -I /../../../tarantool/third_party/luajit/src/ -fPIC

VOLUME ["/log"]

VOLUME ["/credentials"]

ENTRYPOINT ["linkbench/src/tarantool_scripts/linkbench.sh"]
