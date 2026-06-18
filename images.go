package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// VehicleImage represents an image for a transit vehicle.
type VehicleImage struct {
	ID          primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	VehicleID   string             `bson:"vehicle_id" json:"vehicle_id"`
	AgencyCode  string             `bson:"agency_code,omitempty" json:"agency_code,omitempty"`
	ImageURL    string             `bson:"image_url" json:"image_url"`
	Attribution string             `bson:"attribution,omitempty" json:"attribution,omitempty"`
	Description string             `bson:"description,omitempty" json:"description,omitempty"`
	UploadedAt  time.Time          `bson:"uploaded_at" json:"uploaded_at"`
}

// createImageRequest is the JSON body for creating an image record.
type createImageRequest struct {
	ImageURL    string `json:"image_url"`
	VehicleID   string `json:"vehicle_id"`
	AgencyCode  string `json:"agency_code,omitempty"`
	Attribution string `json:"attribution,omitempty"`
	Description string `json:"description,omitempty"`
}

var (
	mongoClient      *mongo.Client
	imagesCollection *mongo.Collection
)

// initMongoDB connects to MongoDB and ensures the images collection exists.
func initMongoDB() error {
	uri := os.Getenv("MONGODB_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}

	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(uri))
	if err != nil {
		return fmt.Errorf("mongo connect: %w", err)
	}

	// Ping to verify the connection.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("mongo ping: %w", err)
	}

	mongoClient = client
	db := client.Database("headways")
	imagesCollection = db.Collection("vehicle_images")

	log.Println("Connected to MongoDB")
	return nil
}

// closeMongoDB gracefully closes the MongoDB connection.
func closeMongoDB() {
	if mongoClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mongoClient.Disconnect(ctx)
	}
}

// --- HTTP Handlers ---

// createImageHandler handles POST /api/images/upload
// Accepts JSON with: image_url, vehicle_id, agency_code, attribution, description
func createImageHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if imagesCollection == nil {
		http.Error(w, "MongoDB is not connected. Check server logs for connection error.", http.StatusServiceUnavailable)
		return
	}

	var req createImageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	if req.VehicleID == "" {
		http.Error(w, "vehicle_id is required", http.StatusBadRequest)
		return
	}
	if req.ImageURL == "" {
		http.Error(w, "image_url is required", http.StatusBadRequest)
		return
	}

	img := VehicleImage{
		VehicleID:   req.VehicleID,
		AgencyCode:  req.AgencyCode,
		ImageURL:    req.ImageURL,
		Attribution: req.Attribution,
		Description: req.Description,
		UploadedAt:  time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := imagesCollection.InsertOne(ctx, img)
	if err != nil {
		log.Printf("mongo insert failed: %v", err)
		http.Error(w, fmt.Sprintf("failed to store image record: %v", err), http.StatusInternalServerError)
		return
	}

	img.ID = result.InsertedID.(primitive.ObjectID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(img)
}

// vehicleImagesHandler handles GET /api/images/vehicle/{vehicle_id}
func vehicleImagesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if imagesCollection == nil {
		http.Error(w, "MongoDB is not connected", http.StatusServiceUnavailable)
		return
	}

	vehicleID := r.PathValue("vehicle_id")
	if vehicleID == "" {
		http.Error(w, "vehicle_id is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filter := bson.M{"vehicle_id": vehicleID}
	opts := options.Find().SetSort(bson.M{"uploaded_at": -1})

	cursor, err := imagesCollection.Find(ctx, filter, opts)
	if err != nil {
		log.Printf("mongo find failed: %v", err)
		http.Error(w, fmt.Sprintf("failed to query images: %v", err), http.StatusInternalServerError)
		return
	}
	defer cursor.Close(ctx)

	var images []VehicleImage
	if err := cursor.All(ctx, &images); err != nil {
		log.Printf("mongo cursor decode failed: %v", err)
		http.Error(w, fmt.Sprintf("failed to decode results: %v", err), http.StatusInternalServerError)
		return
	}

	if images == nil {
		images = []VehicleImage{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(images)
}

// deleteImageHandler handles DELETE /api/images/{id}
func deleteImageHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if imagesCollection == nil {
		http.Error(w, "MongoDB is not connected", http.StatusServiceUnavailable)
		return
	}

	idStr := r.PathValue("id")
	if idStr == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	objID, err := primitive.ObjectIDFromHex(idStr)
	if err != nil {
		http.Error(w, "invalid image id", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := imagesCollection.DeleteOne(ctx, bson.M{"_id": objID})
	if err != nil {
		log.Printf("mongo delete failed: %v", err)
		http.Error(w, fmt.Sprintf("failed to delete image: %v", err), http.StatusInternalServerError)
		return
	}

	if result.DeletedCount == 0 {
		http.Error(w, "image not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// vehicleImagesListHandler handles GET /api/images - lists all images across all vehicles
func vehicleImagesListHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if imagesCollection == nil {
		http.Error(w, "MongoDB is not connected", http.StatusServiceUnavailable)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opts := options.Find().SetSort(bson.M{"uploaded_at": -1}).SetLimit(50)
	cursor, err := imagesCollection.Find(ctx, bson.M{}, opts)
	if err != nil {
		log.Printf("mongo list all failed: %v", err)
		http.Error(w, "failed to query images", http.StatusInternalServerError)
		return
	}
	defer cursor.Close(ctx)

	var images []VehicleImage
	if err := cursor.All(ctx, &images); err != nil {
		log.Printf("mongo cursor decode failed: %v", err)
		http.Error(w, "failed to decode results", http.StatusInternalServerError)
		return
	}

	if images == nil {
		images = []VehicleImage{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(images)
}

// imageUploadPageHandler serves a simple HTML page for adding an image URL
func imageUploadPageHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Headways - Add Vehicle Photo</title>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; background: #f9fafb; color: #111827; }
  h1 { font-size: 24px; margin-bottom: 20px; }
  form { background: white; padding: 24px; border-radius: 12px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
  label { display: block; font-weight: 600; margin-top: 16px; margin-bottom: 4px; font-size: 14px; color: #374151; }
  input, textarea { width: 100%; padding: 8px 12px; border: 1px solid #d1d5db; border-radius: 8px; font-size: 14px; box-sizing: border-box; }
  textarea { resize: vertical; font-family: inherit; }
  button { margin-top: 20px; background: #2563eb; color: white; border: none; padding: 10px 20px; border-radius: 8px; font-size: 16px; font-weight: 600; cursor: pointer; width: 100%; }
  button:hover { background: #1d4ed8; }
  .msg { margin-top: 16px; padding: 12px; border-radius: 8px; display: none; }
  .msg.success { display: block; background: #ecfdf5; color: #065f46; border: 1px solid #a7f3d0; }
  .msg.error { display: block; background: #fef2f2; color: #991b1b; border: 1px solid #fecaca; }
  .help { font-size: 12px; color: #9ca3af; margin-top: 4px; }
</style>
</head>
<body>
  <h1>Add Vehicle Photo</h1>
  <form id="uploadForm">
    <label for="vehicle_id">Vehicle ID *</label>
    <input type="text" id="vehicle_id" name="vehicle_id" required placeholder="e.g. 1015 or vehicle unique_id">
    <div class="help">The vehicle ID shown in the popup</div>

    <label for="image_url">Image URL *</label>
    <input type="url" id="image_url" name="image_url" required placeholder="https://example.com/photo.jpg">
    <div class="help">Direct link to the vehicle photo</div>

    <label for="agency_code">Agency Code</label>
    <input type="text" id="agency_code" name="agency_code" placeholder="e.g. SF, AC, VTA">

    <label for="attribution">Attribution</label>
    <input type="text" id="attribution" name="attribution" placeholder="e.g. Photo by John Doe">

    <label for="description">Description</label>
    <textarea id="description" name="description" rows="2" placeholder="Optional description"></textarea>

    <button type="submit">Save Photo</button>
  </form>
  <div id="message" class="msg"></div>

  <script>
    document.getElementById('uploadForm').addEventListener('submit', async (e) => {
      e.preventDefault();
      const form = e.target;
      const msg = document.getElementById('message');
      msg.className = 'msg';
      msg.style.display = 'none';

      try {
        const res = await fetch('/api/images/upload', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            image_url: form.image_url.value,
            vehicle_id: form.vehicle_id.value,
            agency_code: form.agency_code.value,
            attribution: form.attribution.value,
            description: form.description.value
          })
        });
        const data = await res.json();
        if (res.ok) {
          msg.className = 'msg success';
          msg.innerHTML = 'Saved! Vehicle: <strong>' + data.vehicle_id + '</strong> | Photo: <a href="' + data.image_url + '" target="_blank">view</a>';
          if (data.attribution) msg.innerHTML += '<br>Attribution: ' + data.attribution;
          msg.style.display = 'block';
          form.reset();
        } else {
          msg.className = 'msg error';
          msg.textContent = 'Error: ' + (data.error || JSON.stringify(data));
          msg.style.display = 'block';
        }
      } catch (err) {
        msg.className = 'msg error';
        msg.textContent = 'Network error: ' + err.message;
        msg.style.display = 'block';
      }
    });
  </script>
</body>
</html>`)
}
