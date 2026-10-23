# The release image: the binary that release.yml built for the platform,
# on distroless static, run as user nonroot (65532). Nothing runs during
# the build, so buildx makes both platforms without emulation.
FROM gcr.io/distroless/static-debian13:nonroot@sha256:d62067879de890a1def9da0e7766b7d20810a5e5555cef8506b752437d7a3e88
ARG TARGETARCH
ARG VERSION
COPY --chmod=0755 dist/tigerstore-${VERSION}-linux-${TARGETARCH} /usr/local/bin/tigerstore
ENTRYPOINT ["/usr/local/bin/tigerstore"]
