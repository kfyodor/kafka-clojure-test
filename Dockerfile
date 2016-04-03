FROM clojure:latest

COPY . /app
WORKDIR /app

ENTRYPOINT ["lein", "run"]
