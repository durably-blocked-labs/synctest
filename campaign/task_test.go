package campaign

import "testing"

func TestTaskBindingFromEnv(t *testing.T) {
	for _, n := range []string{"CAMPAIGN_TASK_MANIFEST_DIGEST", "CAMPAIGN_TASK_ID", "CAMPAIGN_TASK_SEED", "CAMPAIGN_COMMAND_DIGEST", "CAMPAIGN_INPUT_DIGEST"} {
		t.Setenv(n, "")
	}
	if b, err := TaskBindingFromEnv(); err != nil || b != nil {
		t.Fatalf("standalone: %+v %v", b, err)
	}
	t.Setenv("CAMPAIGN_TASK_ID", "one")
	if _, err := TaskBindingFromEnv(); err == nil {
		t.Fatal("partial task binding accepted")
	}
	t.Setenv("CAMPAIGN_TASK_MANIFEST_DIGEST", "manifest")
	t.Setenv("CAMPAIGN_TASK_SEED", "42")
	t.Setenv("CAMPAIGN_COMMAND_DIGEST", "command")
	t.Setenv("CAMPAIGN_INPUT_DIGEST", "input")
	binding, err := TaskBindingFromEnv()
	if err != nil || binding.Seed != 42 || binding.TaskID != "one" {
		t.Fatalf("%+v %v", binding, err)
	}
	m := testManifest()
	m.Task = binding
	m.Seed = 42
	if err = m.Validate(); err != nil {
		t.Fatal(err)
	}
	m.Seed = 7
	if err = m.Validate(); err == nil {
		t.Fatal("inconsistent effective seed accepted")
	}
	m.Seed = 42
	m.InputDigest = "different"
	if err = m.Validate(); err == nil {
		t.Fatal("inconsistent task input accepted")
	}
}
