package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"example.com/pz14-rabbit-jobs/services/tasks/internal/jobs"
	"example.com/pz14-rabbit-jobs/services/tasks/internal/publisher"
)

type Handler struct {
	publisher *publisher.Publisher
}

func NewHandler(publisher *publisher.Publisher) *Handler {
	return &Handler{publisher: publisher}
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) ProcessTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		TaskID string `json:"task_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}
	req.TaskID = strings.TrimSpace(req.TaskID)
	if req.TaskID == "" {
		http.Error(w, "task_id is required", http.StatusBadRequest)
		return
	}

	job := jobs.TaskJob{
		Job:       "process_task",
		TaskID:    req.TaskID,
		Attempt:   1,
		MessageID: fmt.Sprintf("msg_%d", time.Now().UnixNano()),
	}
	if err := h.publisher.PublishJob(job); err != nil {
		http.Error(w, "publish job failed", http.StatusBadGateway)
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":     "accepted",
		"task_id":    job.TaskID,
		"message_id": job.MessageID,
		"attempt":    job.Attempt,
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
