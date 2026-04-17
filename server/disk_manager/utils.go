package disk_manager

import (
	"path/filepath"
	"strconv"
	"os"
	"errors"

	"github.com/EricHayter/yakv/server/common"
)

func getFilePath(fileId FileId) string {
	return filepath.Join(common.YakvDirectory, strconv.FormatUint(uint64(fileId), 10))
}

func fileExists(filePath string) (bool, error) {
    _, err := os.Stat(filePath)
    if err == nil {
        return true, nil
    }
    if errors.Is(err, os.ErrNotExist) {
        return false, nil
    }
    return false, err
}
