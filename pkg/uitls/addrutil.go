package uitls

import "strings"

func TrimPort(s string) string {
	lastIndex := strings.LastIndex(s, ":")
	if lastIndex != -1 {
		s = s[:lastIndex]
	}
	return s
}

func TrimAllPort(slice []string) []string {
	newSlice := make([]string, len(slice))
	for i, s := range slice {
		newSlice[i] = TrimPort(s)
	}
	return newSlice
}
