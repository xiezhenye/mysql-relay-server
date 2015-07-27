
default_target: all
.PHONY : default_target

ENV=GOPATH=$(CURDIR) GOBIN=$(CURDIR)/bin

bin/server:
	$(ENV) go install src/main/server.go	

all:bin/server
.PHONY : all

clean:
	rm -f $(CURDIR)/bin/server


