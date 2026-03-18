const express = require("express");
const mysql = require("mysql2");

const app = express();
app.use(express.json());

// =======================
// CONEXÃO MYSQL (RAILWAY)
// =======================

let db;

if (process.env.DATABASE_URL) {
  console.log("[DB] Usando DATABASE_URL");

  db = mysql.createConnection(process.env.DATABASE_URL);
} else {
  console.log("[DB] Usando variáveis locais");

  db = mysql.createConnection({
    host: process.env.DB_HOST,
    port: Number(process.env.DB_PORT),
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
  });
}

// TESTE DE CONEXÃO
db.connect((err) => {
  if (err) {
    console.error("[FATAL] Falha ao conectar no MySQL:", err);
    process.exit(1);
  }
  console.log("✅ Conectado ao MySQL");
});

// =======================
// SERVIDOR
// =======================

const PORT = process.env.PORT || 3000;

app.get("/", (req, res) => {
  res.send("Servidor rodando");
});

app.listen(PORT, () => {
  console.log(`🚀 Server rodando na porta ${PORT}`);
});