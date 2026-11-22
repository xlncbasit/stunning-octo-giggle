FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY src ./src
# AVX2 build (from .cargo/config.toml) and an AVX-512 build; the first execs the second when the CPU allows
RUN cargo build --release \
 && RUSTFLAGS="-C target-cpu=x86-64-v4" cargo build --release --target-dir target-v4

FROM debian:bookworm-slim
COPY --from=build /src/target/release/shopping /app/shopping
COPY --from=build /src/target-v4/release/shopping /app/shopping-v4
WORKDIR /app
CMD ["/app/shopping"]
