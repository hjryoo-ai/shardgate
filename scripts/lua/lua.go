// Package lua 는 큐 상태 전이 Lua 스크립트를 바이너리에 임베드한다.
//
// 스크립트가 이 디렉터리에 있는 것은 우연이 아니다. 모든 상태 전이(enqueue, admit,
// 샤드 이동, 차단)는 여기 있는 스크립트의 단일 원자 실행으로만 일어나고, Go 쪽에
// 같은 로직을 다시 구현하지 않는다(CLAUDE.md 불변식 1). Go 코드는 이 파일들을
// 읽어 실행할 뿐이다.
//
// 임베드하는 이유: 배포된 컨테이너에 scripts/ 디렉터리를 따로 올리지 않아도 되고,
// 바이너리와 스크립트 버전이 어긋날 수 없다.
package lua

import (
	"embed"
	"fmt"
)

//go:embed *.lua
var files embed.FS

// Read 는 임베드된 스크립트 소스를 반환한다.
func Read(name string) (string, error) {
	b, err := files.ReadFile(name)
	if err != nil {
		return "", fmt.Errorf("lua: read %s: %w", name, err)
	}
	return string(b), nil
}

// MustRead 는 Read 와 같지만 없는 스크립트를 요구하면 즉시 패닉한다.
// 스크립트 이름은 컴파일 타임 상수이므로, 실패는 배포 사고가 아니라 프로그래밍 오류다.
func MustRead(name string) string {
	src, err := Read(name)
	if err != nil {
		panic(err)
	}
	return src
}

// Names 는 임베드된 스크립트 파일 이름을 반환한다(테스트/기동 시 문법 검증용).
func Names() ([]string, error) {
	entries, err := files.ReadDir(".")
	if err != nil {
		return nil, fmt.Errorf("lua: list scripts: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	return names, nil
}
