module github.com/shubhaankar/synctest/bugs

go 1.27

require (
	github.com/shubhaankar/synctest v0.0.0
	github.com/hashicorp/raft v1.7.4-0.20251117143445-9e3b7155dd6f
	github.com/hashicorp/go-hclog v1.6.2
	github.com/hashicorp/go-metrics v0.5.4
	github.com/hashicorp/go-msgpack/v2 v2.1.2
)

require (
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.0.0 // indirect
	github.com/hashicorp/golang-lru v0.5.0 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	golang.org/x/sys v0.13.0 // indirect
)

replace github.com/hashicorp/raft => ../../../raft

replace github.com/shubhaankar/synctest => ../../..
