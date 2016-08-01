FROM clojure:latest

RUN mkdir /app
WORKDIR /app

COPY project.clj .
RUN lein deps

COPY ./ .

ENTRYPOINT ["lein", "run"]
