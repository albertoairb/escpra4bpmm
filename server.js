require('dotenv').config();

const express = require('express');
const cors = require('cors');
const path = require('path');
const mysql = require('mysql2/promise');

const app = express();

app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

const publicDir = path.join(__dirname, 'public');
if (require('fs').existsSync(publicDir)) {
  app.use(express.static(publicDir));
}

function getPort() {
  const port = Number(process.env.PORT || 3000);
  return Number.isFinite(port) && port > 0 ? port : 3000;
}

function mask(value) {
  if (!value) return '(vazio)';
  return '***';
}

function buildDbConfig() {
  const databaseUrl = process.env.DATABASE_URL;

  if (databaseUrl && databaseUrl.trim()) {
    return {
      mode: 'url',
      config: databaseUrl.trim(),
      debug: {
        hasDatabaseUrl: true,
        host: '(via DATABASE_URL)',
        port: '(via DATABASE_URL)',
        user: '(via DATABASE_URL)',
        database: '(via DATABASE_URL)',
      },
    };
  }

  const host = process.env.DB_HOST;
  const portRaw = process.env.DB_PORT;
  const user = process.env.DB_USER;
  const password = process.env.DB_PASSWORD;
  const database = process.env.DB_NAME;
  const port = Number(portRaw || 3306);

  if (!host || !user || !database) {
    throw new Error(
      'Configuração de banco incompleta. Use DATABASE_URL ou preencha DB_HOST, DB_PORT, DB_USER, DB_PASSWORD e DB_NAME.'
    );
  }

  if (!Number.isFinite(port)) {
    throw new Error(`DB_PORT inválida: ${portRaw}`);
  }

  return {
    mode: 'fields',
    config: {
      host,
      port,
      user,
      password,
      database,
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0,
    },
    debug: {
      hasDatabaseUrl: false,
      host,
      port,
      user,
      database,
      password: mask(password),
    },
  };
}

let pool;

async function initDatabase() {
  const db = buildDbConfig();
  console.log('[DB CONFIG]', db.debug);

  pool = mysql.createPool(db.config);

  const conn = await pool.getConnection();
  try {
    await conn.query('SELECT 1');
    console.log('[DB] Conexão com MySQL OK');
  } finally {
    conn.release();
  }
}

app.get('/health', async (req, res) => {
  res.json({ ok: true, service: 'escala-pracas', timestamp: new Date().toISOString() });
});

app.get('/api/health/db', async (req, res) => {
  try {
    const [rows] = await pool.query('SELECT 1 AS ok');
    res.json({ ok: true, db: rows[0]?.ok === 1 });
  } catch (error) {
    console.error('[DB HEALTH] Erro:', error);
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.get('*', (req, res, next) => {
  const indexFile = path.join(publicDir, 'index.html');
  if (require('fs').existsSync(indexFile)) {
    return res.sendFile(indexFile);
  }
  return next();
});

app.use((err, req, res, next) => {
  console.error('[ERRO NÃO TRATADO]', err);
  res.status(500).json({ ok: false, error: 'Erro interno do servidor.' });
});

async function start() {
  try {
    await initDatabase();

    const port = getPort();
    app.listen(port, '0.0.0.0', () => {
      console.log(`[APP] Servidor rodando na porta ${port}`);
    });
  } catch (error) {
    console.error('[FATAL] Falha ao iniciar:', error);
    process.exit(1);
  }
}

start();
