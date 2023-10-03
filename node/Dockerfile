FROM rust:1.70.0

ENV ROCKET_ADDRESS=0.0.0.0
ENV ROCKET_PORT=8000

WORKDIR /app
COPY . .

RUN rustup default nightly

ENV DATABASE_RUNNER_PATH="/app/target/release/database_runner"

EXPOSE 8000
EXPOSE 3000

RUN ["cargo", "build", "--release"]
ENTRYPOINT ["cargo", "run", "--bin", "server", "--release"]
