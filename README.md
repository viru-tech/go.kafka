# go.kafka

This is a library to work with kafka producer/consumer with some convenient extra capabilities.

P.S. Currently only producer part is implemented.

## Async producer

1) convenient json, proto and raw message wrappers
2) configurable error logging
3) configurable fallback mechanism

### Fallback 

Fallback is a mechanism that prevents message loss when kafka is unavailable and
message cannot be pushed. Available fallbacks are:

1) another kafka cluster
2) filesystem: unsent messages will be stored on host's fs and resent in background
3) fallback chain: combine multiple fallbacks in a chain in case one fallback fails to deliver message

Any custom fallback may be added by implementing [Fallback](./fallback.go) interface.

See [examples](./examples).
