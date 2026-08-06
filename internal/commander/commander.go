package commander

import (
	"context"
	"os/exec"
)

type Commander interface {
	CommandContext(ctx context.Context, name string, args ...string) *exec.Cmd
	Command(name string, args ...string) *exec.Cmd
}

func New() Commander {
	return RealCommander{}
}

type RealCommander struct{}

func (RealCommander) CommandContext(ctx context.Context, name string, args ...string) *exec.Cmd {
	return exec.CommandContext(ctx, name, args...)
}
func (RealCommander) Command(name string, args ...string) *exec.Cmd {
	return exec.Command(name, args...)
}
