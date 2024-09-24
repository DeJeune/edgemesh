package strings

import "encoding/json"

func Marshal(v any) []byte {
	bytes, _ := json.Marshal(v)
	return bytes
}
