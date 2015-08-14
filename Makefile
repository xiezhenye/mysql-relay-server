
default_target: all
.PHONY : default_target

ENV=GOPATH="$(CURDIR)" GOBIN="$(CURDIR)/bin"

bin/server:
	$(ENV) go install src/main/server.go

bin/rollback:
	$(ENV) go install src/main/rollback.go

all:bin/server,bin/rollback
.PHONY : all

clean:
	echo $(ENV)
	rm -f $(CURDIR)/bin/server $(CURDIR)/bin/rollback
