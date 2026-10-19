package campaign

import (
	"fmt"
	"os"
	"strconv"
)

// TaskBindingFromEnv reads the identity assigned before runner dispatch.
// Nil means standalone execution. A partially supplied binding fails closed.
func TaskBindingFromEnv() (*TaskBinding, error) {
	names := []string{"CAMPAIGN_TASK_MANIFEST_DIGEST", "CAMPAIGN_TASK_ID", "CAMPAIGN_TASK_SEED", "CAMPAIGN_COMMAND_DIGEST", "CAMPAIGN_INPUT_DIGEST"}
	values := make([]string, len(names))
	anySet := false
	for i, n := range names {
		values[i] = os.Getenv(n)
		anySet = anySet || values[i] != ""
	}
	if !anySet {
		return nil, nil
	}
	for i, v := range values {
		if v == "" {
			return nil, fmt.Errorf("missing task binding %s", names[i])
		}
	}
	seed, err := strconv.ParseInt(values[2], 10, 64)
	if err != nil {
		return nil, err
	}
	return &TaskBinding{ManifestDigest: values[0], TaskID: values[1], Seed: seed, CommandDigest: values[3], InputDigest: values[4]}, nil
}
