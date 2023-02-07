FROM rust:latest

RUN apt update && apt upgrade -y
RUN apt install -y g++-mingw-w64-x86-64

RUN rustup update nightly
RUN rustup default nightly

RUN rustup target add x86_64-pc-windows-gnu
RUN rustup toolchain install stable-x86_64-pc-windows-gnu
RUN rustup toolchain install nightly-x86_64-pc-windows-gnu

#RUN rustup target add aarch64-unknown-linux-gnu
#RUN rustup toolchain install stable-aarch64-unknown-linux-gnu
#RUN rustup toolchain install nightly-aarch64-unknown-linux-gnu

WORKDIR /app

CMD ["cargo", "build", "--target", "x86_64-pc-windows-gnu", "--release"]