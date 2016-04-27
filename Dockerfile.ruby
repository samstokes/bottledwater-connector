FROM ruby:2.3.0-onbuild

ADD bottledwater-lib.tar.gz /

WORKDIR /usr/src/app
CMD ["bundle", "exec", "pry", "-Iruby", "-rlib"]
