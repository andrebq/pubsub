.PHONY: test install-go1.18-beta check-env watch tidy
.SILENT: check-env

goBeta?=go1.18beta2

# to use go1.17, add goTool=go which will force make to run commands
# using the default go tool instead of go1.18
goTool?=$(goBeta)

test: check-env
	$(goTool) test ./...

watch:
	modd -f modd.conf

tidy:
	go fmt ./...
	go mod tidy

install-go1.18-beta:
	go install golang.org/dl/go1.18beta2@latest
	$(goBeta) download

check-env:
	which go > /dev/null || { echo "Missing Go. Download at https://go.dev/dl/"; exit 1; }
	which $(goBeta) > /dev/null || { echo "Missing $(goBeta) install using `make install-go1.18-beta`"; exit 1; }
	which modd > /dev/null || { echo "Missing modd. Check install instructions at https://github.com/cortesi/modd#install"; exit 1; }
