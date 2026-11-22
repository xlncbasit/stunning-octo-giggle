FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY src ./src
RUN cargo build --release
COPY gen ./gen
RUN cd gen && cargo build --release

FROM debian:bookworm-slim
COPY --from=build /src/target/release/shopping /app/shopping
COPY --from=build /src/gen/target/release/generate /app/generate
WORKDIR /app
CMD ["/app/shopping"]
