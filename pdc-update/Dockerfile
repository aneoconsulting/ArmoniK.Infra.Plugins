FROM --platform=$BUILDPLATFORM rust as build

RUN apt-get update && apt-get install -y musl-tools musl-dev
# Embed dep tree in the binary so the SBOM scanner sees it in the scratch image
# renovate: datasource=crate depName=cargo-auditable
ARG CARGO_AUDITABLE_VERSION=0.7.6
RUN cargo install cargo-auditable --locked --version "$CARGO_AUDITABLE_VERSION"
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
COPY Cargo.* .

RUN cargo auditable build --release

COPY src src
RUN touch src/main.rs && cargo auditable build --release && mv target/*/release/pdc-update .

FROM gcr.io/distroless/static-debian13
COPY --from=build /app/pdc-update /pdc-update
ENTRYPOINT ["/pdc-update"]
CMD []
