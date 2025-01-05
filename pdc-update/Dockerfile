FROM --platform=$BUILDPLATFORM rust as build

RUN apt-get update && apt-get install -y musl-tools musl-dev
RUN case "$TARGETARCH" in \
        amd64) RUST_PLATFORM=x86_64-unknown-linux-musl ;; \
        arm64) RUST_PLATFORM=aarch64-unknown-linux-musl ;; \
        "") case "$(uname -m)" in \
            x86_64) RUST_PLATFORM=x86_64-unknown-linux-musl ;; \
            aarch64) RUST_PLATFORM=aarch64-unknown-linux-musl ;; \
            *) exit 1 ;; \
        esac ;; \
        *) exit 1 ;; \
    esac ; \
    rustup target add "$RUST_PLATFORM" ; \
    mkdir .cargo ; \
    printf '[build]\ntarget="%s"\n' "$RUST_PLATFORM" > .cargo/config.toml

WORKDIR /app


RUN cargo init
COPY Cargo.toml .
COPY Cargo.lock .

RUN cargo build --release

COPY src src
RUN touch src/main.rs && cargo build --release && mv target/*/release/pdc-update .

FROM scratch
COPY --from=build /app/pdc-update /pdc-update
USER 65534:65534
ENTRYPOINT ["/pdc-update"]
CMD []
