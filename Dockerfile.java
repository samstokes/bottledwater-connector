FROM java:8

RUN mkdir /usr/src/app

WORKDIR /usr/src/app
COPY ./gradlew /usr/src/app
COPY ./gradle /usr/src/app/gradle
RUN ./gradlew --version

COPY . /usr/src/app

RUN ./gradlew assemble

ENTRYPOINT ["./gradlew"]
CMD ["run"]
