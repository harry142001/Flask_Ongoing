# Curbside Search Backend

This is a small Flask API that serves property data from a local SQLite database (`Database1.db`).  
The main endpoint right now is `/search`.

---

## Setup

### 1. Requirements

- Python 3.9+
- pip (Python package installer)
- gunicorn

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the server

```bash
python app.py
```

The server will start at:

```
http://127.0.0.1:5001
```

---

## Endpoints

### Health check

- **URL**: `/health`
- **Example**:
  ```bash
  curl "http://127.0.0.1:5001/health"
  ```
- **Response**:
  ```json
  { "ok": true }
  ```

---

### Search

- **URL**: `/search`
- **Query parameters (optional):**
  - `city` — substring match
  - `broker` — substring match
  - `agent` — substring match
  - `postal` — prefix match (spaces ignored)
  - `state` — exact match (e.g. `ON` or `ONTARIO`)
  - `sort` — `price_asc` | `price_desc`
  - `limit` — number of results (default 200, max 1000)
  - `offset` — pagination offset (default 0)
  - `format` — `json` (default) or `map` (compact format: `"Full Address": "lat,lon"`)

---

## Example Requests

### 1. Get first 5 results

```bash
curl "http://127.0.0.1:5001/search?limit=5"
```

### 2. Search by city

```bash
curl "http://127.0.0.1:5001/search?city=Cooksville&limit=5"
```

### 3. Search by broker

```bash
curl "http://127.0.0.1:5001/search?broker=Homelife&limit=5"
```

### 4. Search by postal prefix

```bash
curl "http://127.0.0.1:5001/search?postal=L5B&limit=5"
```

### 5. Sort by price (ascending)

```bash
curl "http://127.0.0.1:5001/search?city=Cooksville&sort=price_asc&limit=5"
```

### 6. Compact map output

```bash
curl "http://127.0.0.1:5001/search?city=Cooksville&limit=5&format=map"
```

**Map format response example:**

```json
{
  "205 Calverley Trail, Cooksville, ON, L5B 2N1": "43.59,-79.64",
  "12 Euclid Avenue, Cooksville, ON, L5B 1J6": "43.79,-79.17"
}
```
