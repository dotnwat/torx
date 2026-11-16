//go:build unix

package tigerbeetle

/*
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

// The client library's own log hook (tb_client.h), which the Go binding
// links but does not expose.
extern int tb_client_register_log_callback(
	void (*callback)(int level, const uint8_t* message, uint32_t size), bool debug);

// quietLog passes on the client's errors and drops the rest: a client
// reaching a replica that is down logs a warning every few milliseconds,
// which under a nemesis is most of the run.
static void quietLog(int level, const uint8_t* message, uint32_t size) {
	if (level == 0) {
		fprintf(stderr, "tb_client: %.*s\n", (int)size, (const char*)message);
	}
}

static int quietClientLog(void) {
	return tb_client_register_log_callback(quietLog, false);
}
*/
import "C"

import (
	"sync"

	_ "github.com/tigerbeetle/tigerbeetle-go" // links the client library the hook is in
)

var quiet sync.Once

// QuietClientLog has every client in the process log only its errors. It
// must run before the first client is created.
func QuietClientLog() {
	quiet.Do(func() { C.quietClientLog() })
}
