FROM rust:1.88 as builder
WORKDIR /usr/src/vkcp
COPY . .
RUN apt-get update && apt-get install -y protobuf-compiler
RUN cargo install --path .

FROM debian:stable-slim
COPY --from=builder /usr/local/cargo/bin/vkcp /usr/bin/vkcp
CMD ["vkcp"]
