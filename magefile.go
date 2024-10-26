//go:build mage
// +build mage

package main

import (
	"errors"
	"os"
	"os/exec"

	"github.com/magefile/mage/mg"
)

var (
	MAIN_PACKAGE = "./cmd/dmq"
	BIN_NAME     = "dmq"
	BIN_PATH     = "./bin/"
)

var Default = Build

func Build() error {
	return runCmd("go", "build", "-o", BIN_PATH+BIN_NAME, MAIN_PACKAGE)
}

func Lint() error {
	return errors.Join(
		runCmd("go", "vet", "./..."),
		runCmd("golangci-lint", "run"),
	)
}

func Run() error {
	mg.Deps(Build)
	return runCmd("./bin/dmq")
}

func Test() error {
	return runCmd("go", "test", "./...")
}

func Clean() error {
	return os.RemoveAll(BIN_PATH)
}

func runCmd(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
