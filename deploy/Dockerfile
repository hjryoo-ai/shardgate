# ShardGate 서비스 공통 이미지. SERVICE 인자로 cmd/<SERVICE> 를 빌드한다.
FROM golang:1.26-alpine AS build

ARG SERVICE=gate
WORKDIR /src

# 의존성 레이어를 먼저 캐싱한다.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/service ./cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/service /service
USER nonroot:nonroot
ENTRYPOINT ["/service"]
