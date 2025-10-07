import express from "express";
import cors from "cors";
import path from "path";
import fs from "fs-extra";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = 3000;

app.use(cors());
app.use(express.json());

// Thư mục & file dữ liệu
const DATA_DIR = path.join(__dirname, "data");
const POSTS_FILE = path.join(DATA_DIR, "posts_to_process.json");      // USER POSTS (append)
const PROCESSED_FILE = path.join(DATA_DIR, "processed_markers.json");  // KẾT QUẢ HỆ THỐNG

await fs.ensureDir(DATA_DIR);
for (const f of [POSTS_FILE, PROCESSED_FILE]) {
  if (!(await fs.pathExists(f))) {
    await fs.writeJSON(f, [], { spaces: 2 });
  }
}

/**
 * POST /api/posts
 * Nhận 1 bài đăng và append vào posts_to_process.json
 * Payload: { lat:number, lng:number, event:string, author?:string, timestamp?:string }
 */
app.post("/api/posts", async (req, res) => {
  try {
    const { lat, lng, event, author, timestamp } = req.body || {};
    if (typeof lat !== "number" || typeof lng !== "number" || !event || typeof event !== "string") {
      return res.status(400).json({ error: "Invalid payload" });
    }

    const post = {
      lat,
      lng,
      event: event.trim(),
      author: (author || "Ẩn danh").trim(),
      timestamp: timestamp || new Date().toISOString()
    };

    const existing = await fs.readJSON(POSTS_FILE).catch(() => []);
    existing.push(post);
    await fs.writeJSON(POSTS_FILE, existing, { spaces: 2 });

    return res.json({ ok: true, count: existing.length });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Server error" });
  }
});

/**
 * GET /api/processed
 * Trả về dữ liệu đã xử lý để frontend tự hiển thị marker đỏ
 * Kỳ vọng: mảng object có tối thiểu {lat, lng} (hỗ trợ lon/longitude)
 * Có thể kèm: "sự kiện"/"su_kien"/event, "mức độ khẩn cấp"/"muc_do_khan_cap"/urgency/severity, "nguồn"/"nguon"/source
 */
app.get("/api/processed", async (_req, res) => {
  try {
    const data = await fs.readJSON(PROCESSED_FILE).catch(() => []);
    const stat = await fs.stat(PROCESSED_FILE).catch(() => null);
    if (stat) res.setHeader("Last-Modified", stat.mtime.toUTCString());
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: "Server error" });
  }
});

// (Tùy chọn) debug: xem toàn bộ posts đã nhận
app.get("/api/posts", async (_req, res) => {
  try {
    const data = await fs.readJSON(POSTS_FILE).catch(() => []);
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: "Server error" });
  }
});

// Phục vụ static frontend
app.use(express.static(path.join(__dirname, "public")));

app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
  console.log(`User posts file: ${POSTS_FILE}`);
  console.log(`Processed markers file: ${PROCESSED_FILE}`);
});
