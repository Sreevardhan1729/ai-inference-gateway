package main

import (
	"ai-inference-gateway/chat"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"
)

func getMockResponse(cnt int) chat.ChatCompletionResponse {
	return chat.ChatCompletionResponse{
		ID:      "mocking-1",
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   "GPT",
		Choices: []chat.ChatChoice{
			{
				Index: cnt,
				Message: chat.ChatMessage{
					Role:    "assistant",
					Content: "Hello, this is mock response",
				},
				FinishReason: "stop",
			},
		},
		Usage: chat.ChatUsage{
			PromptTokens:     14,
			CompletionTokens: 10,
			TotalTokens:      24,
		},
	}
}

func handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method Not Allowed. Use POST", http.StatusMethodNotAllowed)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func handleV1ChatCompletion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method Not Allowed. Use POST", http.StatusMethodNotAllowed)
		return
	}
	localAddr, _ := r.Context().Value(http.LocalAddrContextKey).(net.Addr)
	w.Header().Set("Server-Id", localAddr.String())
	time.Sleep(time.Second)
	var requestBody chat.ChatCompletionRequest
	errBody := json.NewDecoder(r.Body).Decode(&requestBody)
	if errBody != nil {
		http.Error(w, "Internal Server Error: Failed to read request", http.StatusInternalServerError)
		return
	}
	if requestBody.Stream {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		for i := range 5 {
			mockResponse := getMockResponse(i)
			var buf bytes.Buffer
			buf.WriteString("data: ")
			if err := json.NewEncoder(&buf).Encode(mockResponse); err != nil {
				http.Error(w, "Internal Server Error: Failed to encode response payload", http.StatusInternalServerError)
				return
			}
			w.Write(buf.Bytes())
			fmt.Fprint(w, "\n")
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(time.Second)
		}
		var buf bytes.Buffer
		buf.WriteString("data: [DONE]\n\n")
		w.Write(buf.Bytes())
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	} else {
		mockResponse := getMockResponse(0)
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(mockResponse); err != nil {
			http.Error(w, "Internal Server Error: Failed to encode response payload", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(buf.Bytes())
	}
}
func main() {
	port := ":9001"
	if len(os.Args) >= 2 {
		port = fmt.Sprintf(":%s", os.Args[1])
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", handleV1ChatCompletion)
	server := &http.Server{
		Addr:    port,
		Handler: mux,
	}
	log.Println("Server running on port:", port)
	if errServe := server.ListenAndServe(); errServe != nil && errServe != http.ErrServerClosed {
		log.Fatalf("Server error %v", errServe)
	}
}
