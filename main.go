package main

import (
	"archive/zip"
	"compress/gzip"
	"compress/zlib"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"github.com/joho/godotenv"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	baseURL             = "http://api.511.org/transit/tripupdates"
	vehiclePositionsURL = "http://api.511.org/transit/vehiclepositions"
	datafeedsBase       = "http://api.511.org/transit/datafeeds"
	agencyParam         = "RG"
	operatorParam       = "RG"
)

var datafeedsFilePath string
var datafeedsDir string
var datafeedsJSONFiles []string
var datafeedsFiles []string
var datafeedsFileMap map[string]datafeedEntry

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println(".env not found or could not be loaded, falling back to existing env")
	}

	workingDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("failed to get working directory: %v", err)
	}
	datafeedsFilePath = filepath.Join(workingDir, "data", "gtfs.zip")
	datafeedsDir = filepath.Join(workingDir, "data", "gtfs")

	if err := downloadDatafeed(datafeedsFilePath); err != nil {
		log.Fatalf("failed to download datafeed: %v", err)
	}
	if err := unzipDatafeed(datafeedsFilePath, datafeedsDir); err != nil {
		log.Fatalf("failed to unzip datafeed: %v", err)
	}

	jsonFiles, err := indexJSONFiles(datafeedsDir)
	if err != nil {
		log.Fatalf("failed to index json files: %v", err)
	}
	datafeedsJSONFiles = jsonFiles

	files, err := indexDatafeedFiles(datafeedsDir)
	if err != nil {
		log.Fatalf("failed to index datafeed files: %v", err)
	}
	datafeedsFiles = files

	fileMap, err := buildDatafeedFileMap(datafeedsDir)
	if err != nil {
		log.Fatalf("failed to build datafeed file map: %v", err)
	}
	datafeedsFileMap = fileMap

	mux := http.NewServeMux()
	mux.HandleFunc("/tripupdates", tripUpdatesHandler)
	mux.HandleFunc("/vehiclepositions", vehiclePositionsHandler)
	mux.HandleFunc("/datafeeds/zip", datafeedsZipHandler)
	mux.HandleFunc("/datafeeds", datafeedsGTFSIndexHandler)
	mux.HandleFunc("/datafeeds/json", datafeedsJSONIndexHandler)
	mux.HandleFunc("/datafeeds/json/", datafeedsJSONFileHandler)
	mux.HandleFunc("/datafeeds/", datafeedsGTFSFileHandler)

	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Println("listening on :8080")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
}

func tripUpdatesHandler(w http.ResponseWriter, r *http.Request) {
	apiKey := os.Getenv("API_KEY")
	if apiKey == "" {
		http.Error(w, "API_KEY env var is not set", http.StatusInternalServerError)
		return
	}

	client := &http.Client{Timeout: 15 * time.Second}
	url := fmt.Sprintf("%s?api_key=%s&agency=%s", baseURL, apiKey, agencyParam)

	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, url, nil)
	if err == nil {
		req.Header.Set("Accept", "application/x-protobuf")
	}
	if err != nil {
		http.Error(w, "failed to create request", http.StatusInternalServerError)
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, "failed to fetch trip updates", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		http.Error(w, fmt.Sprintf("upstream error: %s", string(body)), http.StatusBadGateway)
		return
	}

	body, err := readResponseBody(resp)
	if err != nil {
		http.Error(w, "failed to read upstream response", http.StatusBadGateway)
		return
	}

	var feed gtfs.FeedMessage
	if err := proto.Unmarshal(body, &feed); err != nil {
		http.Error(w, "invalid GTFS-realtime protobuf from upstream", http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	marshaler := protojson.MarshalOptions{Indent: "  "}
	payload, err := marshaler.Marshal(&feed)
	if err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
	if _, err := w.Write(payload); err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
		return
	}
}

func vehiclePositionsHandler(w http.ResponseWriter, r *http.Request) {
	apiKey := os.Getenv("API_KEY")
	if apiKey == "" {
		http.Error(w, "API_KEY env var is not set", http.StatusInternalServerError)
		return
	}

	client := &http.Client{Timeout: 15 * time.Second}
	url := fmt.Sprintf("%s?api_key=%s&agency=%s", vehiclePositionsURL, apiKey, agencyParam)

	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, url, nil)
	if err == nil {
		req.Header.Set("Accept", "application/x-protobuf")
	}
	if err != nil {
		http.Error(w, "failed to create request", http.StatusInternalServerError)
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, "failed to fetch vehicle positions", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		http.Error(w, fmt.Sprintf("upstream error: %s", string(body)), http.StatusBadGateway)
		return
	}

	body, err := readResponseBody(resp)
	if err != nil {
		http.Error(w, "failed to read upstream response", http.StatusBadGateway)
		return
	}

	var feed gtfs.FeedMessage
	if err := proto.Unmarshal(body, &feed); err != nil {
		http.Error(w, "invalid GTFS-realtime protobuf from upstream", http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	marshaler := protojson.MarshalOptions{Indent: "  "}
	payload, err := marshaler.Marshal(&feed)
	if err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
	if _, err := w.Write(payload); err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
		return
	}
}

func datafeedsZipHandler(w http.ResponseWriter, r *http.Request) {
	if datafeedsFilePath == "" {
		http.Error(w, "datafeed not initialized", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", "attachment; filename=gtfs.zip")
	http.ServeFile(w, r, datafeedsFilePath)
}

func datafeedsJSONIndexHandler(w http.ResponseWriter, r *http.Request) {
	if datafeedsJSONFiles == nil {
		http.Error(w, "datafeed json index not initialized", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(datafeedsJSONFiles); err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
		return
	}
}

func datafeedsJSONFileHandler(w http.ResponseWriter, r *http.Request) {
	if datafeedsDir == "" {
		http.Error(w, "datafeed not initialized", http.StatusInternalServerError)
		return
	}

	relPath := strings.TrimPrefix(r.URL.Path, "/datafeeds/json/")
	if relPath == "" {
		http.NotFound(w, r)
		return
	}
	if !strings.HasSuffix(relPath, ".json") {
		http.NotFound(w, r)
		return
	}

	cleanRel := filepath.Clean(relPath)
	fullPath := filepath.Join(datafeedsDir, cleanRel)
	if !isSubpath(datafeedsDir, fullPath) {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}

	if _, err := os.Stat(fullPath); err != nil {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	http.ServeFile(w, r, fullPath)
}

func datafeedsGTFSIndexHandler(w http.ResponseWriter, r *http.Request) {
	if datafeedsFileMap == nil {
		http.Error(w, "datafeed file map not initialized", http.StatusInternalServerError)
		return
	}

	var names []string
	for name := range datafeedsFileMap {
		names = append(names, name)
	}
	sort.Strings(names)

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(names); err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
		return
	}
}

func datafeedsGTFSFileHandler(w http.ResponseWriter, r *http.Request) {
	if datafeedsFileMap == nil {
		http.Error(w, "datafeed file map not initialized", http.StatusInternalServerError)
		return
	}

	name := strings.TrimPrefix(r.URL.Path, "/datafeeds/")
	name = strings.TrimSpace(name)
	if name == "" {
		http.NotFound(w, r)
		return
	}
	if name == "zip" || strings.HasPrefix(name, "json") {
		http.NotFound(w, r)
		return
	}

	entry, ok := datafeedsFileMap[name]
	if !ok {
		http.NotFound(w, r)
		return
	}

	switch entry.ext {
	case ".txt", ".csv":
		w.Header().Set("Content-Type", "application/json")
		if err := streamCSVAsJSON(entry.path, w); err != nil {
			http.Error(w, "failed to encode csv as json", http.StatusInternalServerError)
			return
		}
	case ".md":
		content, err := os.ReadFile(entry.path)
		if err != nil {
			http.Error(w, "failed to read markdown", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		payload := map[string]string{"content": string(content)}
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		if err := enc.Encode(payload); err != nil {
			http.Error(w, "failed to write response", http.StatusInternalServerError)
			return
		}
	default:
		http.Error(w, "unsupported file type", http.StatusBadRequest)
	}
}

func downloadDatafeed(destination string) error {
	apiKey := os.Getenv("API_KEY")
	if apiKey == "" {
		return fmt.Errorf("API_KEY env var is not set")
	}

	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	client := &http.Client{Timeout: 60 * time.Second}
	url := fmt.Sprintf("%s?api_key=%s&operator_id=%s", datafeedsBase, apiKey, operatorParam)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Accept", "application/zip")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch datafeeds: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upstream error: %s", string(body))
	}

	body, err := readResponseBody(resp)
	if err != nil {
		return fmt.Errorf("failed to read upstream response: %w", err)
	}

	if err := os.WriteFile(destination, body, 0o644); err != nil {
		return fmt.Errorf("failed to write datafeed file: %w", err)
	}

	return nil
}

func unzipDatafeed(zipPath, destination string) error {
	if err := os.RemoveAll(destination); err != nil {
		return fmt.Errorf("failed to clear destination: %w", err)
	}
	if err := os.MkdirAll(destination, 0o755); err != nil {
		return fmt.Errorf("failed to create destination: %w", err)
	}

	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return fmt.Errorf("failed to open zip: %w", err)
	}
	defer reader.Close()

	for _, file := range reader.File {
		if err := extractZipFile(file, destination); err != nil {
			return err
		}
	}

	return nil
}

func extractZipFile(file *zip.File, destination string) error {
	cleanName := filepath.Clean(file.Name)
	if strings.HasPrefix(cleanName, "..") {
		return fmt.Errorf("invalid zip entry: %s", file.Name)
	}

	fullPath := filepath.Join(destination, cleanName)
	if !isSubpath(destination, fullPath) {
		return fmt.Errorf("invalid zip entry path: %s", file.Name)
	}

	if file.FileInfo().IsDir() {
		if err := os.MkdirAll(fullPath, file.Mode()); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	src, err := file.Open()
	if err != nil {
		return fmt.Errorf("failed to open zip entry: %w", err)
	}
	defer src.Close()

	dst, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, file.Mode())
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

func indexJSONFiles(root string) ([]string, error) {
	var files []string
	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(strings.ToLower(d.Name()), ".json") {
			rel, relErr := filepath.Rel(root, path)
			if relErr != nil {
				return relErr
			}
			files = append(files, filepath.ToSlash(rel))
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return files, nil
}

func indexDatafeedFiles(root string) ([]string, error) {
	var files []string
	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		files = append(files, filepath.ToSlash(rel))
		return nil
	}); err != nil {
		return nil, err
	}

	return files, nil
}

type datafeedEntry struct {
	path string
	ext  string
}

func buildDatafeedFileMap(root string) (map[string]datafeedEntry, error) {
	files, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}

	fileMap := make(map[string]datafeedEntry)
	for _, entry := range files {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		ext := strings.ToLower(filepath.Ext(name))
		if ext != ".txt" && ext != ".csv" && ext != ".md" {
			continue
		}
		base := strings.TrimSuffix(name, ext)
		if _, exists := fileMap[base]; exists {
			continue
		}
		fileMap[base] = datafeedEntry{path: filepath.Join(root, name), ext: ext}
	}

	return fileMap, nil
}

func streamCSVAsJSON(path string, w io.Writer) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1

	headers, err := reader.Read()
	if err != nil {
		return err
	}

	if _, err := w.Write([]byte("[")); err != nil {
		return err
	}

	first := true
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		row := make(map[string]string, len(headers))
		for i, header := range headers {
			if i < len(record) {
				row[header] = record[i]
			} else {
				row[header] = ""
			}
		}

		payload, err := json.Marshal(row)
		if err != nil {
			return err
		}

		if !first {
			if _, err := w.Write([]byte(",")); err != nil {
				return err
			}
		}
		first = false
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}

	if _, err := w.Write([]byte("]")); err != nil {
		return err
	}

	return nil
}

func isSubpath(basePath, targetPath string) bool {
	rel, err := filepath.Rel(basePath, targetPath)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func readResponseBody(resp *http.Response) ([]byte, error) {
	encoding := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Encoding")))

	switch encoding {
	case "", "identity":
		return io.ReadAll(resp.Body)
	case "gzip":
		reader, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		return io.ReadAll(reader)
	case "deflate":
		reader, err := zlib.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		return io.ReadAll(reader)
	default:
		return nil, fmt.Errorf("unsupported content-encoding: %s", encoding)
	}
}
