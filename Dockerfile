FROM golang:1.21

WORKDIR /go/project/priceService

ADD go.mod go.sum main.go ./
ADD internal ./internal
ADD proto ./proto

EXPOSE 7071 7073

CMD ["go", "run", "main.go"]
