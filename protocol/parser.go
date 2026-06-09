package protocol

import "strings"

// ExtractClientID retrieves the value of the "client=" parameter in the telemetry packet.
func ExtractClientID(packet string) (string, bool) {
	const prefix = "client="
	idx := strings.Index(packet, prefix)
	if idx == -1 {
		return "", false
	}
	start := idx + len(prefix)

	end := strings.Index(packet[start:], ",")
	if end == -1 {
		end = strings.Index(packet[start:], "\n")
		if end == -1 {
			end = len(packet[start:])
		}
	}

	clientID := strings.TrimSpace(packet[start : start+end])
	if clientID == "" {
		return "", false
	}
	return clientID, true
}

// ExtractWeather retrieves the value of the "weather=" parameter in the telemetry packet.
func ExtractWeather(packet string) (string, bool) {
	const prefix = "weather="
	idx := strings.Index(packet, prefix)
	if idx == -1 {
		return "", false
	}
	start := idx + len(prefix)

	end := strings.Index(packet[start:], ",")
	if end == -1 {
		end = strings.Index(packet[start:], "\n")
		if end == -1 {
			end = len(packet[start:])
		}
	}

	weather := strings.TrimSpace(packet[start : start+end])
	if weather == "" {
		return "", false
	}
	return weather, true
}

// ExtractMode retrieves the value of the "mode=" parameter in the telemetry packet.
func ExtractMode(packet string) (string, bool) {
	const prefix = "mode="
	idx := strings.Index(packet, prefix)
	if idx == -1 {
		return "", false
	}
	start := idx + len(prefix)

	end := strings.Index(packet[start:], ",")
	if end == -1 {
		end = strings.Index(packet[start:], "\n")
		if end == -1 {
			end = len(packet[start:])
		}
	}

	mode := strings.TrimSpace(packet[start : start+end])
	if mode == "" {
		return "", false
	}
	return mode, true
}
