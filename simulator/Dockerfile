FROM golang:1.23-alpine AS build
RUN apk add --no-cache git build-base
WORKDIR /src
RUN git clone https://github.com/brocaar/chirpstack-simulator.git .
RUN go build -o /out/chirpstack-simulator ./cmd/chirpstack-simulator

FROM alpine:3.20
RUN apk add --no-cache bash ca-certificates
COPY --from=build /out/chirpstack-simulator /usr/local/bin/chirpstack-simulator
COPY entrypoint.sh /entrypoint.sh
COPY chirpstack-simulator.toml.template /etc/chirpstack-simulator.toml.template
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
