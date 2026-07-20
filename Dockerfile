FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/kvstore ./cmd/kvstore

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/kvstore /kvstore
EXPOSE 50051
WORKDIR /data
ENTRYPOINT ["/kvstore"]
