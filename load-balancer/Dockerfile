# BUILDPLATFORM is unset without BuildKit: "linux" means the host platform
FROM --platform=${BUILDPLATFORM:-linux} rust AS build

RUN apt-get update && apt-get install -y protobuf-compiler ca-certificates && update-ca-certificates
# Embed dep tree in the binary so the SBOM scanner sees it in the scratch image
# renovate: datasource=crate depName=cargo-auditable
ARG CARGO_AUDITABLE_VERSION=0.7.6
RUN cargo install cargo-auditable --locked --version "$CARGO_AUDITABLE_VERSION"

# Cross toolchains: Debian cross gcc + target musl (musl-dev ships <cpu>-linux-musl-gcc)
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=x86_64-linux-musl-gcc \
    CC_x86_64_unknown_linux_musl=x86_64-linux-musl-gcc \
    AR_x86_64_unknown_linux_musl=x86_64-linux-gnu-ar \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=aarch64-linux-musl-gcc \
    CC_aarch64_unknown_linux_musl=aarch64-linux-musl-gcc \
    AR_aarch64_unknown_linux_musl=aarch64-linux-gnu-ar
# Unset without BuildKit: build for the native arch
ARG TARGETARCH
RUN arch="${TARGETARCH:-$(dpkg --print-architecture)}" ; \
    case "$arch" in \
        amd64) cpu=x86_64 ;; \
        arm64) cpu=aarch64 ;; \
        *) exit 1 ;; \
    esac ; \
    dpkg --add-architecture "$arch" && \
    apt-get update && \
    apt-get install -y "gcc-$(echo "$cpu" | tr _ -)-linux-gnu" "musl-dev:$arch" && \
    rustup target add "$cpu-unknown-linux-musl"

WORKDIR /app

RUN cargo init
COPY Cargo.* ./

RUN cargo auditable build --release --target "$(rustup target list --installed | grep musl)"

COPY src src
RUN touch src/main.rs && cargo auditable build --release --target "$(rustup target list --installed | grep musl)" && mv target/*/release/load-balancer .

FROM gcr.io/distroless/static-debian13
COPY --from=build /app/load-balancer /load-balancer
ENTRYPOINT ["/load-balancer"]
CMD []
