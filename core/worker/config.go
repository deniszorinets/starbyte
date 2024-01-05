package worker

import (
	"encoding/json"
	"fmt"
	"os"
)

const LIGHTBYTE_WORKER_CONFIG = "LIGHTBYTE_WORKER_CONFIG"

func ReadConfigEnvVar(out any) error {
	config := []byte(os.Getenv(LIGHTBYTE_WORKER_CONFIG))

	err := json.Unmarshal(config, &out)
	if err != nil {
		return fmt.Errorf("failed to read json config from `LIGHTBYTE_WORKER_CONFIG` environment variable")
	}
	return nil
}
