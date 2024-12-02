package infoschema

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

func TestFunctions(t *testing.T) {
	_, dir, _, _ := runtime.Caller(0)
	fmt.Println(filepath.Dir(dir))
	fmt.Println(reflect.TypeOf(Function{}).PkgPath())
	fmt.Println(dir)
	fmt.Println(os.Getwd())
	dir, _ = os.Executable()
	fmt.Println(dir)
}
