package main

import (
	"ai-inference-gateway/chat"
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func main() {
	client := http.Client{
		Timeout: 10 * time.Second,
	}
	prompt := chat.ChatCompletionRequest{
		Model: "GPT",
		Messages: []chat.ChatRequestMessage{
			{Role: "system", Content: "You are a helpful programming assistant."},
			{Role: "user", Content: "Explain struct tags in GO."},
		},
		Temperature: 0.9,
		MaxTokens:   151,
		Stream:      true,
	}
	jsonPayload, errPayload := json.Marshal(prompt)
	if errPayload != nil {
		fmt.Println("errPayload", errPayload)
		return
	}
	req, errReq := http.NewRequest(http.MethodPost, "http://localhost:8080/v1/chat/completions", bytes.NewBuffer(jsonPayload))
	if errReq != nil {
		fmt.Println("errReq", errReq)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, errResp := client.Do(req)
	if errResp != nil {
		fmt.Println("errResp", errResp)
		return
	}
	defer resp.Body.Close()
	fmt.Println(resp.Status)
	fmt.Println(resp.Header.Get("Server-ID"))
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		fmt.Println(line)
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("errScanner: ", errReq)
	}

}
