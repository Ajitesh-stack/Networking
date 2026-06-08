package main

import (
	"testing"
)

// TestExtractClientID validates client ID parsing under various packet formats and edge cases.
func TestExtractClientID(t *testing.T) {
	tests := []struct {
		name       string
		packet     string
		expectedID string
		expectedOk bool
	}{
		{
			name:       "Standard packet format",
			packet:     "client=qp02zt,seq=1,lat=-5.462952,lon=90.686646,weather=clear\n",
			expectedID: "qp02zt",
			expectedOk: true,
		},
		{
			name:       "Client ID at the end of packet",
			packet:     "seq=1,lat=-5.462952,lon=90.686646,client=qp02zt\n",
			expectedID: "qp02zt",
			expectedOk: true,
		},
		{
			name:       "Client ID at the end (no newline)",
			packet:     "seq=1,lat=-5.462952,lon=90.686646,client=qp02zt",
			expectedID: "qp02zt",
			expectedOk: true,
		},
		{
			name:       "Empty packet",
			packet:     "",
			expectedID: "",
			expectedOk: false,
		},
		{
			name:       "Missing client prefix",
			packet:     "seq=1,lat=-5.462952,lon=90.686646,weather=clear\n",
			expectedID: "",
			expectedOk: false,
		},
		{
			name:       "Empty client value",
			packet:     "client=,seq=1\n",
			expectedID: "",
			expectedOk: false,
		},
		{
			name:       "Whitespace client value",
			packet:     "client=   ,seq=1\n",
			expectedID: "",
			expectedOk: false,
		},
		{
			name:       "Truncated string",
			packet:     "client=",
			expectedID: "",
			expectedOk: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id, ok := extractClientID(tc.packet)
			if ok != tc.expectedOk {
				t.Errorf("expected ok=%v, got %v", tc.expectedOk, ok)
			}
			if id != tc.expectedID {
				t.Errorf("expected clientID=%q, got %q", tc.expectedID, id)
			}
		})
	}
}

// TestExtractWeather validates weather parsing from the telemetry stream.
func TestExtractWeather(t *testing.T) {
	tests := []struct {
		name            string
		packet          string
		expectedWeather string
		expectedOk      bool
	}{
		{
			name:            "Standard packet format",
			packet:          "client=qp02zt,seq=1,lat=-5.462952,lon=90.686646,weather=clear\n",
			expectedWeather: "clear",
			expectedOk:      true,
		},
		{
			name:            "Weather in the middle of packet",
			packet:          "client=qp02zt,weather=rain,seq=1\n",
			expectedWeather: "rain",
			expectedOk:      true,
		},
		{
			name:            "Weather at the end (no newline)",
			packet:          "client=qp02zt,seq=1,weather=fog",
			expectedWeather: "fog",
			expectedOk:      true,
		},
		{
			name:            "Missing weather parameter",
			packet:          "client=qp02zt,seq=1,lat=-5.462952,lon=90.686646\n",
			expectedWeather: "",
			expectedOk:      false,
		},
		{
			name:            "Empty weather parameter value",
			packet:          "client=qp02zt,weather=\n",
			expectedWeather: "",
			expectedOk:      false,
		},
		{
			name:            "Whitespace weather parameter value",
			packet:          "client=qp02zt,weather=  \n",
			expectedWeather: "",
			expectedOk:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w, ok := extractWeather(tc.packet)
			if ok != tc.expectedOk {
				t.Errorf("expected ok=%v, got %v", tc.expectedOk, ok)
			}
			if w != tc.expectedWeather {
				t.Errorf("expected weather=%q, got %q", tc.expectedWeather, w)
			}
		})
	}
}
