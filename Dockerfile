FROM rust:latest as builder
WORKDIR /opt/tesseract
COPY . .
RUN cargo install --path ./tesseract-server

FROM debian:buster-slim
RUN groupadd -r tesseract &&\
    useradd -r -g tesseract tesseract &&\
    apt-get update &&\
    apt-get install --no-install-recommends -y libssl-dev &&\
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/tesseract-olap /usr/local/bin/tesseract-olap
USER tesseract
EXPOSE 7777
CMD ["tesseract-olap", "-a", "0.0.0.0:7777"]
