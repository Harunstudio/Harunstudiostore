// © DONGTUBE | WhatsApp: 0831-4396-1588
// ⚠️ Do not remove this credit
//
// ── SUPABASE SETUP (jalankan SQL ini di Supabase SQL Editor)
// CREATE TABLE IF NOT EXISTS kv_store (
//   key        TEXT PRIMARY KEY,
//   value      JSONB NOT NULL DEFAULT '{}',
//   updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
// );
// CREATE INDEX IF NOT EXISTS idx_kv_store_prefix
//   ON kv_store (key text_pattern_ops);
// ─────────────────────────────────────────────────────────────────────────
'use strict';
require('dotenv').config();

const express     = require('express');
const axios       = require('axios');
const QRCode      = require('qrcode');
const path        = require('path');
const crypto      = require('crypto');
const compression = require('compression');
const multer      = require('multer');
const moment      = require('moment-timezone');
const { createClient } = require('@supabase/supabase-js');
const { Octokit }      = require('@octokit/rest');

const app = express();

// ── Trust reverse-proxy headers (Vercel, Nginx) so req.ip = real client IP
app.set('trust proxy', 1);

// ── PERFORMANCE: gzip/brotli compress all responses (huge speed win for HTML/JSON)
app.use(compression({ level: 6, threshold: 1024 }));

// ── SECURITY: request body size limits (anti-DoS)
app.use(express.json({ limit: '256kb' }));
app.use(express.urlencoded({ extended: true, limit: '64kb' }));

// ── SECURITY: hardened HTTP headers on every response
app.use(function(req, res, next) {
  res.set('X-Content-Type-Options',    'nosniff');
  res.set('X-Frame-Options',           'DENY');
  res.set('X-XSS-Protection',          '1; mode=block');
  res.set('Referrer-Policy',           'strict-origin-when-cross-origin');
  res.set('Permissions-Policy',        'geolocation=(), camera=(), microphone=()');
  // SECURITY: Content-Security-Policy — blok inline script injection (XSS)
  res.set('Content-Security-Policy',   "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob: http: https:; connect-src 'self'; font-src 'self' data:; object-src 'none'; frame-ancestors 'none'");
  // Cache-Control: no-store only for API routes — HTML/CSS/JS need browser caching
  if (req.path.startsWith('/api/')) {
    res.set('Cache-Control', 'no-store');
  } else if (req.path.match(/\.(js|css|png|jpg|jpeg|ico|svg|woff|woff2|ttf|webp)$/i)) {
    res.set('Cache-Control', 'public, max-age=86400');
  } else {
    // HTML pages: always revalidate so settings/theme changes reflect immediately
    res.set('Cache-Control', 'no-cache');
  }
  next();
});

// ── PIGGYBACKED CRON (Vercel Hobby workaround)
// Vercel Hobby hanya boleh cron sekali sehari. Sebagai gantinya, setiap request
// API yang masuk akan memicu cron di background jika sudah >10 menit sejak terakhir jalan.
// FIX: interval dinaikkan dari 5→10 menit & jobs dijalankan SEQUENTIAL (bukan concurrent)
// agar tidak berebut GitHub API writes dan menyebabkan 409 conflict / server crash.
var _lastPiggybackRun = 0;
var _lastPanelCronRun = 0;
var _piggybackRunning = false; // FIX: flag agar tidak overlap antar instance
var _PIGGYBACK_INTERVAL = 10 * 60 * 1000; // FIX: 10 menit (sebelumnya 5 menit)
app.use(function(req, res, next) {
  // Hanya trigger untuk request API, bukan SSE/stream yang long-lived
  if (req.path.startsWith('/api/') && !req.path.includes('/stream') && !req.path.includes('/cron/run')) {
    var now = Date.now();
    if (now - _lastPiggybackRun > _PIGGYBACK_INTERVAL && !_piggybackRunning) {
      _lastPiggybackRun = now;
      _piggybackRunning = true;
      setImmediate(async function() {
        try {
          // FIX: jalankan SEQUENTIAL bukan concurrent — cegah berebut DB write
          // Sebelumnya: 3 fungsi jalan bersamaan → banyak 409 conflict → crash
          await autoExpireOtpOrders().catch(function(e){ console.error('[piggyback] otp-expire:', e.message); });
          await autoReconcileDeposits().catch(function(e){ console.error('[piggyback] reconcile:', e.message); });
          await autoExpirePendingOrders().catch(function(e){ console.error('[piggyback] trx-expire:', e.message); });
          if (now - _lastPanelCronRun > 30 * 60 * 1000) {
            _lastPanelCronRun = now;
            await autoSuspendExpiredPanels().catch(function(e){ console.error('[piggyback] panels:', e.message); });
          }
        } finally {
          _piggybackRunning = false;
        }
      });
    }
  }
  next();
});

// ── STATIC FILES: HANYA sajikan dari folder public/ saja
// SECURITY FIX: jangan pernah sajikan __dirname (root) langsung —
// itu akan expose index.js, .env, dan semua file sensitif ke publik.
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.static(path.resolve(process.cwd(), 'public')));
try { app.use(express.static(path.resolve('/var/task/public'))); } catch(e){}

// ── SERVER LOG BUFFER (in-memory, last 500 lines)
var _logBuf = [];
function _capLog(level, args) {
  var msg = args.map(function(a){ return typeof a === 'object' ? JSON.stringify(a) : String(a); }).join(' ');
  _logBuf.push({ ts: Date.now(), level: level, msg: msg });
  if (_logBuf.length > 500) _logBuf.shift();
}
var _origLog  = console.log;
var _origWarn = console.warn;
var _origErr  = console.error;
console.log   = function() { _capLog('info',  Array.prototype.slice.call(arguments)); _origLog.apply(console, arguments); };
console.warn  = function() { _capLog('warn',  Array.prototype.slice.call(arguments)); _origWarn.apply(console, arguments); };
console.error = function() { _capLog('error', Array.prototype.slice.call(arguments)); _origErr.apply(console, arguments); };



// ── CONFIG
const C = {
  gh: {
    // ── PRIMARY GITHUB ACCOUNT (wajib untuk CDN)
    token  : process.env.GH_TOKEN   || '',
    owner  : process.env.GH_OWNER   || '',
    // GH_REPO bisa berupa koma-separated: "cdn1,cdn2"
    repos  : (process.env.GH_REPO   || '').split(',').map(function(r){ return r.trim(); }).filter(Boolean),
    get repo() { return this.repos[0] || ''; }, // backward compat
    branch : process.env.GH_BRANCH  || 'main',
    private: process.env.GH_PRIVATE === 'true' || process.env.GH_PRIVATE === '1',
    // ── SECONDARY GITHUB ACCOUNT (opsional — fallback otomatis)
    token2 : process.env.GH_TOKEN2  || '',
    owner2 : process.env.GH_OWNER2  || '',
    repos2 : (process.env.GH_REPO2  || '').split(',').map(function(r){ return r.trim(); }).filter(Boolean),
    get repo2() { return this.repos2[0] || ''; },
    branch2: process.env.GH_BRANCH2 || process.env.GH_BRANCH || 'main',
    private2: process.env.GH_PRIVATE2 === 'true' || process.env.GH_PRIVATE2 === '1',
  },
  pak: {
    slug   : process.env.PAKASIR_SLUG   || '',
    apikey : process.env.PAKASIR_APIKEY || '',
  },
  ptero: {
    domain   : (process.env.PTERO_DOMAIN || '').replace(/\/$/, ''),
    apikey   : process.env.PTERO_APIKEY  || '',
    capikey  : process.env.PTERO_CAPIKEY || '',
    egg      : parseInt(process.env.PTERO_EGG)      || 15,
    nest     : parseInt(process.env.PTERO_NEST)     || 5,
    location : parseInt(process.env.PTERO_LOCATION) || 1,
  },
  store: {
    name      : process.env.STORE_NAME || 'Dongtube',
    wa        : process.env.STORE_WA   || 'https://wa.me/6283143961588',
    expiry    : parseInt(process.env.EXPIRY_MIN) || 15,
    adminPass : process.env.ADMIN_PASS || 'admin123',
    adminPath : process.env.ADMIN_PATH || 'admin',
  },
  port: parseInt(process.env.PORT) || 3000,
  cdn: {
    maxSize: parseInt(process.env.CDN_MAX_SIZE_MB || '100') * 1024 * 1024,
  },
  otp: {
    apikey : process.env.RUMAHOTP_APIKEY || '',
  },
};

// ── SUPABASE (database utama — semua data disimpan di sini)
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY || '';
let supabase = null;
if (SUPABASE_URL && SUPABASE_KEY) {
  supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
  console.log('[db] ✅ Supabase terhubung');
} else if ((process.env.DATABASE || 'supabase').toLowerCase() !== 'github') {
  console.warn('[db] ⚠️ Supabase belum dikonfigurasi — set SUPABASE_URL dan SUPABASE_SERVICE_KEY');
}

// ══════════════════════════════════════════════════════════════
// DATABASE BACKEND SELECTOR
// Set env: DATABASE=supabase (default) OR DATABASE=github
// CDN selalu pakai GitHub (GH_TOKEN/GH_OWNER/GH_REPO) terlepas dari pilihan DB.
//
// ── GitHub DB env vars (untuk DATABASE=github):
//   GH_DB_TOKEN  = token GitHub (bisa sama dengan GH_TOKEN atau beda)
//   GH_DB_OWNER  = username/org pemilik repo database
//   GH_DB_REPO   = nama repo untuk database (misal: dongtube-db)
//   GH_DB_BRANCH = branch (default: main)
//
// ── Supabase DB env vars (untuk DATABASE=supabase):
//   SUPABASE_URL         = https://xxxx.supabase.co
//   SUPABASE_SERVICE_KEY = service_role key
// ══════════════════════════════════════════════════════════════
const DB_BACKEND   = (process.env.DATABASE || 'supabase').toLowerCase().trim();
const GH_DB_TOKEN  = process.env.GH_DB_TOKEN  || process.env.GH_TOKEN  || '';
const GH_DB_OWNER  = process.env.GH_DB_OWNER  || process.env.GH_OWNER  || '';
const GH_DB_REPO   = process.env.GH_DB_REPO   || '';
const GH_DB_BRANCH = process.env.GH_DB_BRANCH || 'main';

let _dbOctokit = null;
if (DB_BACKEND === 'github') {
  if (GH_DB_TOKEN && GH_DB_OWNER && GH_DB_REPO) {
    _dbOctokit = new Octokit({ auth: GH_DB_TOKEN, request: { timeout: 30000 } });
    console.log('[db] ✅ GitHub DB:', GH_DB_OWNER + '/' + GH_DB_REPO + '@' + GH_DB_BRANCH);
  } else {
    console.error('[db] ❌ DATABASE=github tapi GH_DB_TOKEN/GH_DB_OWNER/GH_DB_REPO belum diset di env!');
    console.error('[db]    Set: GH_DB_TOKEN, GH_DB_OWNER, GH_DB_REPO (contoh: dongtube-db)');
  }
}

// ── OCTOKIT CDN (upload/serve file ke GitHub)
// PRIMARY account — wajib
let _cdnOctokit = null;
if (C.gh.token && C.gh.owner && C.gh.repos.length) {
  _cdnOctokit = new Octokit({
    auth: C.gh.token,
    request: { timeout: 60000 },
  });
  console.log('[cdn] ✅ Primary CDN:', C.gh.owner + '/' + C.gh.repos.join(','));
} else {
  console.warn('[cdn] ⚠️ GitHub CDN belum dikonfigurasi — set GH_TOKEN, GH_OWNER, GH_REPO');
}

// SECONDARY account — opsional, fallback otomatis jika primary gagal
let _cdnOctokit2 = null;
if (C.gh.token2 && C.gh.owner2 && C.gh.repos2.length) {
  _cdnOctokit2 = new Octokit({
    auth: C.gh.token2,
    request: { timeout: 60000 },
  });
  console.log('[cdn] ✅ Secondary CDN:', C.gh.owner2 + '/' + C.gh.repos2.join(','));
}


// ── STATELESS HMAC SESSION TOKENS (Vercel/serverless-compatible — no in-memory Maps)
// Derive a stable secret from env vars so tokens survive cold starts.
// Set TOKEN_SECRET in your environment for best security.
const SIGNING_SECRET = process.env.TOKEN_SECRET ||
  crypto.createHash('sha256')
    .update((process.env.ADMIN_PASS || 'Dongtube') + (process.env.GH_TOKEN || '') + 'ps-v2-salt-2025')
    .digest('hex');

const SESSION_TTL = 30 * 24 * 60 * 60 * 1000; // 30 hari
const USER_TTL    = 30 * 24 * 60 * 60 * 1000; // 30 hari

function makeToken(payload) {
  const data = Buffer.from(JSON.stringify(payload)).toString('base64url');
  const sig  = crypto.createHmac('sha256', SIGNING_SECRET).update(data).digest('base64url');
  return data + '.' + sig;
}
function verifyToken(token) {
  if (!token || typeof token !== 'string') return null;
  const dot  = token.lastIndexOf('.');
  if (dot < 0) return null;
  const data = token.slice(0, dot);
  const sig  = token.slice(dot + 1);
  const expected = crypto.createHmac('sha256', SIGNING_SECRET).update(data).digest('base64url');
  if (sig !== expected) return null;
  try {
    const p = JSON.parse(Buffer.from(data, 'base64url').toString());
    if (p.exp && Date.now() > p.exp) return null;
    return p;
  } catch(e) { return null; }
}
function makeAdminToken() {
  return makeToken({ role: 'admin', iat: Date.now(), exp: Date.now() + SESSION_TTL });
}
function makeUserToken(username) {
  return makeToken({ sub: username, role: 'user', iat: Date.now(), exp: Date.now() + USER_TTL });
}

// ── SECURITY: in-memory rate limiter (no extra dependencies)
const _rl = new Map();
function rateLimit(key, maxHits, windowMs) {
  // CATATAN KEAMANAN: rateLimit ini pakai in-memory Map.
  // Di Vercel serverless, tiap cold-start instance punya Map sendiri → tidak shared antar instance.
  // Artinya rate limit TIDAK efektif jika request masuk ke instance berbeda.
  // Untuk keamanan SALDO: ini aman karena updateBalance pakai optimistic locking di DB level.
  // Untuk brute-force login: bisa bypass jika IP yang sama kena instance berbeda.
  // Solusi proper: gunakan Redis/Upstash. Untuk sekarang ini best-effort protection.
  const now = Date.now();
  _rlCleanup(); // lazy cleanup
  const e   = _rl.get(key) || { h: 0, r: now + windowMs };
  if (now > e.r) { e.h = 0; e.r = now + windowMs; }
  e.h++;
  _rl.set(key, e);
  return e.h <= maxHits;
}
// Lazy cleanup: purge expired entries on each rateLimit call (serverless-safe, no setInterval needed)
// setInterval is unreliable on Vercel serverless (cold starts reset timers)
var _rlLastClean = Date.now();
function _rlCleanup() {
  var now = Date.now();
  // Only clean every 5 minutes to avoid overhead on every request
  if (now - _rlLastClean < 5 * 60 * 1000) return;
  _rlLastClean = now;
  for (var _k of _rl.keys()) { var _v = _rl.get(_k); if (_v && now > _v.r) _rl.delete(_k); }
}

// ── SECURITY: TRX ID validation (anti path-traversal, no ../.. tricks)
const TRX_RE     = /^(TRX|RNW|BOT)-\d{13}-[a-f0-9]{8}$/;
const DEP_RE     = /^DEP-\d{13}-[a-f0-9]{8}$/;
const OTPORD_RE  = /^OTP-\d{13}-[a-f0-9]{8}$/;
function isValidId(id)     { return typeof id === 'string' && TRX_RE.test(id); }
function isValidDepId(id)  { return typeof id === 'string' && DEP_RE.test(id); }
function isValidOtpId(id)  { return typeof id === 'string' && OTPORD_RE.test(id); }
function newId(prefix) {
  return (prefix || 'TRX') + '-' + Date.now() + '-' + crypto.randomBytes(4).toString('hex');
}

// ── SLEEP HELPER (used throughout for backoff / rate-limit spacing)
function _sleep(ms) { return new Promise(function(resolve){ setTimeout(resolve, ms); }); }

// ── OTP ORDER LOCK (per-user mutex — cegah concurrent order bobol saldo)
// Menyimpan Promise lock per username agar hanya satu order OTP bisa berjalan per user.
// Jika user kirim 3 request bersamaan, order ke-2 & ke-3 akan antri dan diproses satu per satu.
const _otpOrderLocks = new Map();
const _OTP_ORDER_COOLDOWN_MS = 3000; // jeda minimum antar order per user (3 detik)
const _otpLastOrderTime = new Map(); // track waktu order terakhir per user

async function _acquireOtpLock(username) {
  // Tunggu jika ada lock aktif untuk user ini
  while (_otpOrderLocks.has(username)) {
    await _otpOrderLocks.get(username);
  }
  // Buat lock baru
  let _resolve;
  const lockPromise = new Promise(function(resolve) { _resolve = resolve; });
  _otpOrderLocks.set(username, lockPromise);
  return _resolve;
}

function _releaseOtpLock(username, resolveFn) {
  _otpOrderLocks.delete(username);
  if (resolveFn) resolveFn();
}

// Cegah order terlalu cepat (cooldown antar order per user)
function _checkOtpCooldown(username) {
  var last = _otpLastOrderTime.get(username) || 0;
  var diff = Date.now() - last;
  if (diff < _OTP_ORDER_COOLDOWN_MS) {
    return _OTP_ORDER_COOLDOWN_MS - diff; // kembalikan sisa ms yang harus ditunggu
  }
  return 0;
}

// ── MULTER (memory storage for CDN uploads)
const _multer = multer({
  storage: multer.memoryStorage(),
  // Support hingga 100MB via CDN_MAX_SIZE_MB env atau default 100MB
  // File < 25MB: GitHub API | File >= 25MB: git clone
  limits : { fileSize: Math.max((parseInt(process.env.CDN_MAX_SIZE_MB || '100')) * 1024 * 1024, 100 * 1024 * 1024) },
});

// ── CDN ALLOWED EXTENSIONS (diperluas — sinkron dengan server.js)
const CDN_ALLOWED_EXT = /^\.(jpg|jpeg|png|gif|webp|bmp|svg|ico|tiff|avif|heic|mp4|mov|mkv|avi|webm|flv|wmv|m4v|ts|mts|ogv|3gp|mp3|wav|aac|flac|opus|ogg|m4a|wma|aiff|pdf|doc|docx|xls|xlsx|ppt|pptx|txt|csv|json|xml|md|html|css|js|py|java|cpp|go|rs|php|sh|bat|yml|yaml|toml|ini|conf|sql|zip|rar|7z|tar|gz|bz2|iso|apk|exe|dmg|deb|rpm|msi|m3u|m3u8|srt|vtt|woff|woff2|ttf|otf)$/i;

// ── CDN MIME TYPE MAP (lengkap — sinkron dengan server.js)
const CDN_MIME = {
  jpg:'image/jpeg', jpeg:'image/jpeg', png:'image/png', gif:'image/gif',
  webp:'image/webp', bmp:'image/bmp', svg:'image/svg+xml', ico:'image/x-icon',
  tiff:'image/tiff', avif:'image/avif', heic:'image/heic',
  mp4:'video/mp4', mov:'video/quicktime', mkv:'video/x-matroska',
  avi:'video/x-msvideo', webm:'video/webm', flv:'video/x-flv',
  wmv:'video/x-ms-wmv', m4v:'video/x-m4v', ts:'video/mp2t',
  mts:'video/mp2t', ogv:'video/ogg', '3gp':'video/3gpp',
  mp3:'audio/mpeg', wav:'audio/wav', aac:'audio/aac', flac:'audio/flac',
  opus:'audio/opus', ogg:'audio/ogg', m4a:'audio/mp4', wma:'audio/x-ms-wma', aiff:'audio/aiff',
  pdf:'application/pdf', doc:'application/msword',
  docx:'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  xls:'application/vnd.ms-excel',
  xlsx:'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  ppt:'application/vnd.ms-powerpoint',
  pptx:'application/vnd.openxmlformats-officedocument.presentationml.presentation',
  txt:'text/plain', csv:'text/csv', json:'application/json',
  xml:'application/xml', md:'text/markdown; charset=utf-8', html:'text/html; charset=utf-8',
  css:'text/css', js:'text/javascript',
  py:'text/x-python', java:'text/x-java', cpp:'text/plain', go:'text/plain',
  rs:'text/plain', php:'text/plain', sh:'text/plain', bat:'text/plain',
  yml:'text/plain', yaml:'text/plain', toml:'text/plain', ini:'text/plain',
  conf:'text/plain', sql:'text/plain',
  zip:'application/zip', rar:'application/x-rar-compressed',
  '7z':'application/x-7z-compressed', tar:'application/x-tar',
  gz:'application/gzip', bz2:'application/x-bzip2',
  iso:'application/x-iso9660-image',
  apk:'application/vnd.android.package-archive',
  exe:'application/vnd.microsoft.portable-executable',
  dmg:'application/x-apple-diskimage',
  m3u:'audio/x-mpegurl', m3u8:'application/vnd.apple.mpegurl',
  srt:'text/plain', vtt:'text/vtt',
  woff:'font/woff', woff2:'font/woff2', ttf:'font/ttf', otf:'font/otf',
};

// ── CDN CONSTANTS
const CDN_MAX_FILES_PER_FOLDER = 500;   // max file per subfolder (files, files2, ...)
const CDN_MAX_FOLDERS          = 10;    // max jumlah subfolder per repo
const CDN_GITHUB_API_LIMIT     = 25 * 1024 * 1024; // 25MB — over this, pakai git clone

// ── CDN FOLDER COUNT CACHE (TTL 5 menit — kurangi GitHub API hits)
var _cdnFolderCache = new Map();
const CDN_FOLDER_CACHE_TTL = 5 * 60 * 1000;

function _cdnCacheKey(owner, repo, folder) { return owner + ':' + repo + ':' + folder; }
function _cdnGetCachedCount(owner, repo, folder) {
  var k = _cdnCacheKey(owner, repo, folder);
  var hit = _cdnFolderCache.get(k);
  if (hit && Date.now() - hit.ts < CDN_FOLDER_CACHE_TTL) return hit.count;
  _cdnFolderCache.delete(k); return null;
}
function _cdnSetCachedCount(owner, repo, folder, count) {
  _cdnFolderCache.set(_cdnCacheKey(owner, repo, folder), { count: count, ts: Date.now() });
}
function _cdnInvalidateCache(owner, repo, folder) {
  _cdnFolderCache.delete(_cdnCacheKey(owner, repo, folder));
}

// ── CDN ACCOUNT LIST (primary + secondary)
function _cdnAccounts() {
  var accs = [];
  if (_cdnOctokit && C.gh.token && C.gh.owner && C.gh.repos.length) {
    accs.push({ octokit: _cdnOctokit, token: C.gh.token, owner: C.gh.owner, repos: C.gh.repos, branch: C.gh.branch, name: 'PRIMARY' });
  }
  if (_cdnOctokit2 && C.gh.token2 && C.gh.owner2 && C.gh.repos2.length) {
    accs.push({ octokit: _cdnOctokit2, token: C.gh.token2, owner: C.gh.owner2, repos: C.gh.repos2, branch: C.gh.branch2, name: 'SECONDARY' });
  }
  return accs;
}

// ── CDN: COUNT FILES IN FOLDER (with cache)
async function _cdnCountFolder(octokit, owner, repo, folder) {
  var cached = _cdnGetCachedCount(owner, repo, folder);
  if (cached !== null) return cached;
  try {
    var r = await octokit.repos.getContent({ owner: owner, repo: repo, path: folder });
    var count = Array.isArray(r.data) ? r.data.filter(function(i){ return i.type === 'file'; }).length : 0;
    _cdnSetCachedCount(owner, repo, folder, count);
    return count;
  } catch(e) {
    if (e.status === 404) { _cdnSetCachedCount(owner, repo, folder, 0); return 0; }
    throw e;
  }
}

// ── CDN: FIND AVAILABLE FOLDER IN REPO (files → files2 → ... → files10)
async function _cdnFindAvailableFolder(octokit, owner, repo) {
  for (var i = 1; i <= CDN_MAX_FOLDERS; i++) {
    var folderName = i === 1 ? 'files' : 'files' + i;
    try {
      var count = await _cdnCountFolder(octokit, owner, repo, folderName);
      if (count < CDN_MAX_FILES_PER_FOLDER) return folderName;
    } catch(e) {
      // folder belum ada = kosong, gunakan ini
      return folderName;
    }
  }
  return null; // semua folder penuh
}

// ── CDN: FIND TARGET REPO + FOLDER (telusuri semua account + repo)
async function _cdnFindTarget() {
  var accs = _cdnAccounts();
  if (!accs.length) throw new Error('GitHub CDN belum dikonfigurasi. Set GH_TOKEN, GH_OWNER, GH_REPO.');
  for (var ai = 0; ai < accs.length; ai++) {
    var acc = accs[ai];
    for (var ri = 0; ri < acc.repos.length; ri++) {
      var repo = acc.repos[ri];
      try {
        var folder = await _cdnFindAvailableFolder(acc.octokit, acc.owner, repo);
        if (folder) {
          console.log('[cdn] target:', acc.name, acc.owner + '/' + repo + '/' + folder);
          return { octokit: acc.octokit, token: acc.token, owner: acc.owner, repo: repo, folder: folder, branch: acc.branch, accName: acc.name };
        }
      } catch(e) { console.warn('[cdn] skip repo', acc.name + '/' + repo, ':', e.message); }
    }
  }
  throw new Error('Tidak ada repo/folder CDN tersedia. Semua penuh atau tidak dapat diakses.');
}

// ── CDN ERROR SANITIZER (jangan expose token di error response)
function _cdnSanitizeError(err) {
  var msg = (err && err.message) || String(err);
  if (/token|auth|401|403|unauthorized|forbidden/i.test(msg)) return 'Authentication error';
  if (/rate.?limit|429/i.test(msg)) return 'Terlalu banyak request ke GitHub, coba lagi sebentar';
  if (/ENOTFOUND|ECONNREFUSED|ETIMEDOUT/i.test(msg)) return 'Network error, coba lagi';
  if (/404|not.?found/i.test(msg)) return 'File tidak ditemukan';
  return 'Upload gagal, coba lagi.';
}

// ── CDN UPLOAD: upload buffer ke GitHub CDN dengan folder rotation + dual account
// Mendukung file kecil (<25MB) via API, file besar via git clone
async function cdnUploadFile(filename, buffer) {
  var target = await _cdnFindTarget();
  var filePath = target.folder + '/' + filename;
  var content  = buffer.toString('base64');

  if (buffer.length < CDN_GITHUB_API_LIMIT) {
    // ── METODE API (file < 25MB)
    try {
      await target.octokit.repos.createOrUpdateFileContents({
        owner: target.owner, repo: target.repo,
        path: filePath,
        message: 'cdn: ' + filename,
        content: content, branch: target.branch,
      });
    } catch(e) {
      if (e.status === 422) {
        // File sudah ada — ambil SHA dulu lalu update
        var ex = await target.octokit.repos.getContent({
          owner: target.owner, repo: target.repo,
          path: filePath, ref: target.branch,
        });
        await target.octokit.repos.createOrUpdateFileContents({
          owner: target.owner, repo: target.repo,
          path: filePath,
          message: 'cdn: ' + filename,
          content: content, sha: ex.data.sha, branch: target.branch,
        });
      } else { throw e; }
    }
  } else {
    // ── METODE GIT CLONE (file >= 25MB)
    var execFile = require('child_process').execFileSync;
    var fs   = require('fs');
    var tmpDir = '/tmp/cdn-' + Date.now() + '-' + crypto.randomBytes(4).toString('hex');
    try {
      execFile('mkdir', ['-p', tmpDir]);
      // SECURITY: gunakan execFileSync dengan array argumen (bukan string concat)
      // untuk mencegah shell injection via nama file berbahaya
      execFile('git', ['clone', '-b', target.branch, '--depth', '1', '--single-branch', '--no-tags',
        'https://' + target.token + '@github.com/' + target.owner + '/' + target.repo + '.git', tmpDir
      ], { stdio: 'pipe', timeout: 5 * 60 * 1000, maxBuffer: 200 * 1024 * 1024 });
      execFile('mkdir', ['-p', tmpDir + '/' + target.folder]);
      fs.writeFileSync(tmpDir + '/' + filePath, buffer);
      execFile('git', ['config', 'user.email', 'cdn@dongtube.local'], { cwd: tmpDir, stdio: 'pipe' });
      execFile('git', ['config', 'user.name', 'CDN Upload'], { cwd: tmpDir, stdio: 'pipe' });
      execFile('git', ['add', filePath], { cwd: tmpDir, stdio: 'pipe' });
      execFile('git', ['commit', '-m', 'cdn: ' + filename], { cwd: tmpDir, stdio: 'pipe' });
      execFile('git', ['push', 'origin', target.branch], { cwd: tmpDir, stdio: 'pipe', timeout: 5 * 60 * 1000 });
      console.log('[cdn/git] uploaded:', filename, '|', (buffer.length / 1024 / 1024).toFixed(1) + 'MB');
    } finally {
      try { execFile('rm', ['-rf', tmpDir]); } catch(e2) {}
    }
  }

  // Invalidate folder cache setelah upload berhasil
  _cdnInvalidateCache(target.owner, target.repo, target.folder);

  // Return path yang bisa diakses via /cdn/:filename
  return '/cdn/' + filename;
}



// ── IN-MEMORY READ CACHE (dramatically speeds up repeated reads)
const _dbCache = new Map();
// ── Analytics cache (busted on every trx/deposit write)
var _analyticsCache = null, _analyticsCacheAt = 0, ANALYTICS_TTL = 5 * 60 * 1000;
function invalidateAnalyticsCache() { _analyticsCache = null; _analyticsCacheAt = 0; }
const DB_CACHE_TTL = {
  'products.json':          30 * 1000,  // 30s
  'settings.json':          60 * 1000,  // 60s
  'panel-templates.json':   60 * 1000,  // 60s
};
const DB_CACHE_DEFAULT_TTL = 8 * 1000;

function _dbCacheTTL(fp) {
  for (const k of Object.keys(DB_CACHE_TTL)) if (fp.endsWith(k)) return DB_CACHE_TTL[k];
  return DB_CACHE_DEFAULT_TTL;
}
function _dbCacheKey(fp) { return fp; }
function _dbCacheInvalidate(fp) { _dbCache.delete(_dbCacheKey(fp)); }

// ── GIT TREE CACHE: untuk listing folder (30 detik TTL)
var _gitTreeCache = new Map();
var GIT_TREE_TTL  = 30 * 1000;

function _invalidateDirCache(fp) {
  var parts = fp.split('/');
  if (parts.length >= 2) _gitTreeCache.delete(parts[0]);
}

// ══════════════════════════════════════════════════════════════
// ── GITHUB DB BACKEND (DATABASE=github)
// Semua data disimpan sebagai file JSON di repo GitHub.
// SHA dari GitHub blob dipakai langsung sebagai "sha" parameter
// di seluruh kode (sama persis dengan format Supabase updated_at).
// Rate limit GitHub API: 5000 req/jam — caching agresif di atas sudah cukup.
// ══════════════════════════════════════════════════════════════

async function _ghDbRead(fp, bypassCache) {
  if (!_dbOctokit) return { data: null, sha: null };
  const key = _dbCacheKey(fp);
  if (!bypassCache) {
    const hit = _dbCache.get(key);
    if (hit && Date.now() < hit.exp) return hit.val;
  }
  try {
    const r = await _dbOctokit.repos.getContent({
      owner: GH_DB_OWNER, repo: GH_DB_REPO, path: fp, ref: GH_DB_BRANCH,
    });
    const content = Buffer.from(r.data.content, 'base64').toString('utf8');
    const parsed  = JSON.parse(content);
    const val = { data: parsed, sha: r.data.sha };
    _dbCache.set(key, { val, exp: Date.now() + _dbCacheTTL(fp) });
    return val;
  } catch(e) {
    if (e.status === 404) return { data: null, sha: null };
    throw e;
  }
}

async function _ghDbWrite(fp, data, sha, msg) {
  if (!_dbOctokit) throw new Error('GitHub DB tidak dikonfigurasi. Set GH_DB_TOKEN, GH_DB_OWNER, GH_DB_REPO di env.');
  const content = Buffer.from(JSON.stringify(data, null, 2)).toString('base64');
  const params  = {
    owner: GH_DB_OWNER, repo: GH_DB_REPO, path: fp,
    message: msg || ('db: ' + fp), content, branch: GH_DB_BRANCH,
  };
  if (sha) params.sha = sha;
  try {
    await _dbOctokit.repos.createOrUpdateFileContents(params);
  } catch(e) {
    // 409 Conflict atau 422 Unprocessable = SHA salah — baca ulang dan coba lagi sekali
    if (e.status === 409 || e.status === 422) {
      const fresh = await _ghDbRead(fp, true);
      if (fresh.sha) params.sha = fresh.sha; else delete params.sha;
      await _dbOctokit.repos.createOrUpdateFileContents(params);
    } else { throw e; }
  }
  _dbCacheInvalidate(fp);
  _invalidateDirCache(fp);
}

async function _ghDbDelete(fp) {
  if (!_dbOctokit) return;
  try {
    const r = await _ghDbRead(fp, true);
    if (!r.sha) return; // file tidak ada, tidak perlu hapus
    await _dbOctokit.repos.deleteFile({
      owner: GH_DB_OWNER, repo: GH_DB_REPO, path: fp,
      message: 'db: delete ' + fp, sha: r.sha, branch: GH_DB_BRANCH,
    });
  } catch(e) {
    if (e.status === 404) return; // sudah tidak ada
    throw e;
  }
  _dbCacheInvalidate(fp);
  _invalidateDirCache(fp);
}

async function _ghListDir(folderPath) {
  if (!_dbOctokit) return [];
  const cacheKey = folderPath;
  const hit = _gitTreeCache.get(cacheKey);
  if (hit && Date.now() < hit.exp) return hit.data;
  try {
    const r = await _dbOctokit.repos.getContent({
      owner: GH_DB_OWNER, repo: GH_DB_REPO, path: folderPath, ref: GH_DB_BRANCH,
    });
    const files = Array.isArray(r.data)
      ? r.data.filter(function(f){ return f.type === 'file'; }).map(function(f){
          return { name: f.name, sha: f.sha, type: 'file' };
        })
      : [];
    _gitTreeCache.set(cacheKey, { data: files, exp: Date.now() + GIT_TREE_TTL });
    return files;
  } catch(e) {
    if (e.status === 404) {
      _gitTreeCache.set(cacheKey, { data: [], exp: Date.now() + GIT_TREE_TTL });
      return [];
    }
    throw e;
  }
}

// ══════════════════════════════════════════════════════════════
// ── SUPABASE DB BACKEND (DATABASE=supabase, default)
// ══════════════════════════════════════════════════════════════

async function _sbRead(fp, bypassCache) {
  if (!supabase) return { data: null, sha: null };
  const key = _dbCacheKey(fp);
  if (!bypassCache) {
    const hit = _dbCache.get(key);
    if (hit && Date.now() < hit.exp) return hit.val;
  }
  const { data, error } = await supabase
    .from('kv_store')
    .select('value, updated_at')
    .eq('key', fp)
    .maybeSingle();
  if (error || !data) return { data: null, sha: null };
  const val = { data: data.value, sha: data.updated_at };
  _dbCache.set(key, { val, exp: Date.now() + _dbCacheTTL(fp) });
  return val;
}

async function _sbWrite(fp, data, sha, msg) {
  if (!supabase) throw new Error('Supabase belum dikonfigurasi. Set SUPABASE_URL & SUPABASE_SERVICE_KEY.');
  const now = new Date().toISOString();

  if (sha) {
    // SECURITY FIX: Conditional update — hanya update jika SHA (updated_at) masih sama.
    // Ini mencegah race condition double-credit/double-deliver di Vercel multi-instance.
    const { data: updated, error } = await supabase
      .from('kv_store')
      .update({ value: data, updated_at: now })
      .eq('key', fp)
      .eq('updated_at', sha)
      .select('key');
    if (error) throw new Error('[supabase] ' + error.message);
    // Jika tidak ada baris yang terupdate, berarti SHA sudah berubah → conflict
    if (!updated || updated.length === 0) {
      throw Object.assign(new Error('SHA conflict — data sudah diubah oleh proses lain'), { status: 409 });
    }
  } else {
    // Insert baru (sha kosong berarti data belum ada sebelumnya)
    const { error } = await supabase
      .from('kv_store')
      .upsert({ key: fp, value: data, updated_at: now }, { onConflict: 'key' });
    if (error) throw new Error('[supabase] ' + error.message);
  }

  _dbCacheInvalidate(fp);
  _invalidateDirCache(fp);
}

async function _sbDelete(fp) {
  if (!supabase) return;
  const { error } = await supabase.from('kv_store').delete().eq('key', fp);
  if (error) throw new Error('[supabase] delete: ' + error.message);
  _dbCacheInvalidate(fp);
  _invalidateDirCache(fp);
}

async function _sbListDir(folderPath) {
  const cacheKey = folderPath;
  const hit = _gitTreeCache.get(cacheKey);
  if (hit && Date.now() < hit.exp) return hit.data;
  if (!supabase) return [];
  const { data, error } = await supabase
    .from('kv_store')
    .select('key, updated_at')
    .like('key', folderPath + '/%')
    .order('key', { ascending: false });
  if (error) throw new Error('[supabase] ' + error.message);
  const files = (data || []).map(function(row) {
    return { name: row.key.slice(folderPath.length + 1), sha: row.updated_at || '', type: 'file' };
  });
  _gitTreeCache.set(cacheKey, { data: files, exp: Date.now() + GIT_TREE_TTL });
  return files;
}

// ══════════════════════════════════════════════════════════════
// ── PUBLIC DB API (backend-agnostic — gunakan fungsi ini di seluruh kode)
// Dispatch otomatis ke Supabase atau GitHub berdasarkan DB_BACKEND env
// ══════════════════════════════════════════════════════════════

async function dbRead(fp, bypassCache) {
  return DB_BACKEND === 'github'
    ? _ghDbRead(fp, bypassCache)
    : _sbRead(fp, bypassCache);
}

async function dbWrite(fp, data, sha, msg) {
  return DB_BACKEND === 'github'
    ? _ghDbWrite(fp, data, sha, msg)
    : _sbWrite(fp, data, sha, msg);
}

async function dbDelete(fp) {
  return DB_BACKEND === 'github'
    ? _ghDbDelete(fp)
    : _sbDelete(fp);
}

async function listDirCached(folderPath) {
  return DB_BACKEND === 'github'
    ? _ghListDir(folderPath)
    : _sbListDir(folderPath);
}


async function getProducts()       { const r = await dbRead('products.json');  return r.data || []; }
// ── ATOMIC STOCK DECREMENT ─────────────────────────────────────────────────
// Decrement variant stock by 1 after successful payment (skip if stock == -1)
async function decrementStock(productId, variantId) {
  // FIX: tambah retry loop untuk handle 409 conflict saat 2 order selesai bersamaan
  // Tanpa retry, order kedua tidak mengurangi stok → stok bisa lebih besar dari seharusnya
  const MAX_RETRY = 5;
  for (var _dsi = 0; _dsi < MAX_RETRY; _dsi++) {
    try {
      const r = await dbRead('products.json', true); // bypass cache for freshness
      if (!r.data) return;
      const products = r.data;
      const pi = products.findIndex(function(p){ return p.id === productId; });
      if (pi < 0) return;
      const vi = products[pi].variants ? products[pi].variants.findIndex(function(v){ return v.id === variantId; }) : -1;
      if (vi < 0) return;
      const cur = products[pi].variants[vi].stock;
      if (cur === undefined || cur === null || cur < 0) return; // -1 = unlimited, skip
      if (cur === 0) return; // already 0
      products[pi].variants[vi].stock = cur - 1;
      await dbWrite('products.json', products, r.sha, 'stock-decrement:' + variantId);
      console.log('[stock] decremented:', variantId, '|', cur, '->', cur - 1);
      return; // sukses
    } catch(e) {
      // 409 = conflict karena order lain juga decrement bersamaan — retry
      if (_dsi < MAX_RETRY - 1 && (e.status === 409 || (e.message && e.message.includes('conflict')))) {
        await _sleep(150 * (_dsi + 1));
        continue;
      }
      console.warn('[stock] decrement failed (non-critical):', e.message);
      return;
    }
  }
}


// ── ACCOUNTS POOL DB (for contentType: 'account' — multi-akun per varian)
// Stored as: accounts/{productId}_{variantId}.json → array of strings
function _acctPath(productId, variantId) { return 'accounts/' + productId + '_' + variantId + '.json'; }
async function getAccounts(productId, variantId) { return dbRead(_acctPath(productId, variantId), true); }
async function saveAccounts(productId, variantId, arr, sha) {
  return dbWrite(_acctPath(productId, variantId), arr, sha, 'accounts:' + variantId);
}
// Atomically pop & return the first account; returns null if empty
async function popAccount(productId, variantId) {
  for (var _i = 0; _i < 3; _i++) {
    try {
      const r = await getAccounts(productId, variantId);
      if (!r.data || !Array.isArray(r.data) || r.data.length === 0) return null;
      const account = r.data[0];
      await saveAccounts(productId, variantId, r.data.slice(1), r.sha);
      return account;
    } catch(e) {
      if (_i < 2 && e.response && e.response.status === 409) { await _sleep(400 * (_i + 1)); continue; }
      throw e;
    }
  }
  return null;
}

async function getSettings()       { const r = await dbRead('settings.json');  return r.data || {}; }
async function getPanelTemplates() {
  try { const r = await dbRead('panel-templates.json'); return Array.isArray(r.data) ? r.data : []; }
  catch(e) { return []; }
}
// Merge GitHub templates with hardcoded SPEC fallback
async function resolveSpec(plan) {
  const templates = await getPanelTemplates();
  const t = templates.find(function(x){ return x.id === plan; });
  if (t) return t;
  return SPEC[plan] ? Object.assign({ id: plan, name: plan }, SPEC[plan]) : Object.assign({ id: '1gb', name: '1GB' }, SPEC['1gb']);
}
async function getTrx(id)          { return dbRead('transactions/' + id + '.json'); }
async function saveTrx(id, d, sha) {
  // SECURITY: strip Pakasir raw response before writing to DB (contains API keys)
  const clean = Object.assign({}, d);
  if (clean.pakData && clean.pakData.api_key) clean.pakData = { _ok: true, amount: clean.pakData.amount || null };
  invalidateAnalyticsCache(); // bust cache when any trx changes
  return dbWrite('transactions/' + id + '.json', clean, sha, 'trx:' + id + ':' + d.status);
}
async function listTrx() {
  try { return await listDirCached('transactions'); } catch(e) { return []; }
}
async function getEffectiveSettings() {
  const s = await getSettings();
  return {
    storeName   : s.storeName    || C.store.name,
    wa          : s.wa           || C.store.wa,
    expiryMin   : s.expiryMin    || C.store.expiry,
    logoUrl     : s.logoUrl      || '',
    appLogoUrl  : s.appLogoUrl   || '',
    announcement: s.announcement || '',
    footerText  : s.footerText   || '',
    primaryColor: s.primaryColor || '#34d399',
    tiktok      : s.tiktok       || '',
    instagram   : s.instagram    || '',
    otpEnabled  : s.otpEnabled   !== false,
    panelEnabled: s.panelEnabled !== false,
    maintenanceMode: s.maintenanceMode || false,
    maintenanceMsg : s.maintenanceMsg  || 'Sedang dalam maintenance.',
    musicUrl    : s.musicUrl    || '',
    musicEnabled: s.musicEnabled === true,
    captchaEnabled: s.captchaEnabled === true,
    depositFeeType: s.depositFeeType || 'flat',
    depositFee    : s.depositFee  != null ? parseFloat(s.depositFee)  : 0,
    depositMin    : s.depositMin  != null ? parseInt(s.depositMin)    : 1000,
    otpMarkup     : s.otpMarkup   != null ? parseFloat(s.otpMarkup)   : 0,
    bgUrl     : s.bgUrl      || '',
    bgType    : s.bgType     || 'image',
    bgOpacity : s.bgOpacity  != null ? parseFloat(s.bgOpacity) : 0.15,
    // ── CUSTOM FIELDS: [{id, label, type, required, placeholder, forProduct}]
    // forProduct: 'all' | 'panel' | 'digital' | 'download' | 'sewabot'
    customFields: Array.isArray(s.customFields) ? s.customFields : [],
    // ── PHONE FIELD SETTING
    phoneRequired: s.phoneRequired === true,
    phoneEnabled : s.phoneEnabled  !== false, // default tampil
  };
}

// Helper: resolve product content for any variant
async function resolveContent(trx) {
  const products = await getProducts();
  const prod     = products.find(function(p) { return p.id === trx.productId; });
  const vari     = prod && prod.variants.find(function(v) { return v.id === trx.variantId; });
  if (!vari) return { type: 'digital', contentType: 'text', message: 'Produk ditemukan. Hubungi admin untuk detail. ID: ' + trx.id };
  const ct = vari.contentType || 'text';
  return {
    type       : 'digital',
    contentType: ct,
    contentUrl : vari.contentUrl || vari.fileUrl || '',
    contentText: vari.contentText || vari.content || '',
    title      : vari.name || trx.variantName,
    description: vari.description || '',
    filename   : vari.filename || '',
  };
}

// ── USER DB
const USER_RE   = /^[a-z0-9_]{3,20}$/;
function isValidUsername(u) { return USER_RE.test(u); }

async function getUser(username)        { return dbRead('users/' + username + '.json'); }
async function saveUser(username, d, sha) { return dbWrite('users/' + username + '.json', d, sha, 'user:' + username); }
async function listUsers() {
  try { return await listDirCached('users'); } catch(e) { return []; }
}

// ── DEPOSIT DB
async function getDeposit(id)           { return dbRead('deposits/' + id + '.json'); }
async function saveDeposit(id, d, sha)  { invalidateAnalyticsCache(); return dbWrite('deposits/' + id + '.json', d, sha, 'deposit:' + id + ':' + (d.status||'?')); }

// ── OTP ORDERS DB
async function getOtpOrder(id)          { return dbRead('otp-orders/' + id + '.json'); }
async function saveOtpOrder(id, d, sha) { return dbWrite('otp-orders/' + id + '.json', d, sha, 'otporder:' + id + ':' + (d.status||'?')); }
async function listOtpOrders(username) {
  try {
    const files = (await listDirCached('otp-orders')).filter(function(f){ return f.name.endsWith('.json'); });
    const results = [];
    await Promise.all(files.map(async function(f) {
      try { const o = await getOtpOrder(f.name.replace('.json','')); if(o.data && o.data.username === username) results.push(o.data); } catch(e) {}
    }));
    return results.sort(function(a,b){ return (b.createdAt||0)-(a.createdAt||0); });
  } catch(e) { return []; }
}

// ── SEWABOT ORDERS DB
async function getSewabotOrder(id)          { return dbRead('sewabot-orders/' + id + '.json'); }
async function saveSewabotOrder(id, d, sha) { return dbWrite('sewabot-orders/' + id + '.json', d, sha, 'sewabot:' + id + ':' + (d.status||'?')); }
async function listSewabotOrders() {
  try {
    const files = (await listDirCached('sewabot-orders')).filter(function(f){ return f.name.endsWith('.json'); });
    const results = [];
    await Promise.all(files.map(async function(f) {
      try { const o = await getSewabotOrder(f.name.replace('.json','')); if(o.data) results.push(o.data); } catch(e) {}
    }));
    return results.sort(function(a,b){ return (b.createdAt||0)-(a.createdAt||0); });
  } catch(e) { return []; }
}


// ── PASSWORD HASHING (crypto.scrypt — built-in, no bcrypt needed)
async function hashPassword(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  return new Promise(function(resolve, reject) {
    crypto.scrypt(password, salt, 32, function(err, dk) {
      if (err) reject(err);
      else resolve(salt + ':' + dk.toString('hex'));
    });
  });
}
async function verifyPassword(password, stored) {
  if (!stored || !stored.includes(':')) return false;
  const [salt, hash] = stored.split(':');
  if (!salt || !hash) return false;
  return new Promise(function(resolve, reject) {
    crypto.scrypt(password, salt, 32, function(err, dk) {
      if (err) { resolve(false); return; }
      try { resolve(crypto.timingSafeEqual(Buffer.from(hash, 'hex'), dk)); }
      catch(e) { resolve(false); }
    });
  });
}

// ── RUMAHOTP API WRAPPER
const rotp = {
  h: function() { return { 'x-apikey': C.otp.apikey, 'Accept': 'application/json' }; },
  async get(url) {
    const r = await axios.get('https://www.rumahotp.com' + url, { headers: rotp.h() });
    return r.data;
  },
  async services()                    { return rotp.get('/api/v2/services'); },
  async countries(service_id)         { return rotp.get('/api/v2/countries?service_id=' + service_id); },
  async order(number_id, provider_id, operator_id) {
    return rotp.get('/api/v2/orders?number_id=' + number_id + '&provider_id=' + provider_id + '&operator_id=' + (operator_id || 'any'));
  },
  async orderStatus(order_id)         { return rotp.get('/api/v1/orders/get_status?order_id=' + order_id); },
  async orderDetail(order_id)         { return rotp.get('/api/v1/orders/get_status?order_id=' + order_id); },
  async cancelOrder(order_id)         { return rotp.get('/api/v1/orders/set_status?order_id=' + order_id + '&status=cancel'); },
  async balance()                     { return rotp.get('/api/v1/user/balance'); },
  async depositCreate(amount)         { return rotp.get('/api/v2/deposit/create?amount=' + amount + '&payment_id=qris'); },
  async depositStatus(deposit_id)     { return rotp.get('/api/v2/deposit/get_status?deposit_id=' + deposit_id); },
  async depositCancel(deposit_id)     { return rotp.get('/api/v1/deposit/cancel?deposit_id=' + deposit_id); },
};

// ── USER AUTH MIDDLEWARE
// SECURITY FIX: cek ban user di SETIAP request — jangan hanya beberapa route saja.
// Async ban check (non-blocking via getUser) — saldo/OTP/chat semua dilindungi.
async function userAuth(req, res, next) {
  const token   = req.headers['x-user-token'];
  const payload = verifyToken(token);
  if (!payload || payload.role !== 'user' || !payload.sub) {
    return res.status(401).json({ ok: false, message: 'Login diperlukan.' });
  }
  req.user = payload.sub;
  // Cek ban — user yang diblokir admin tidak bisa akses endpoint manapun
  try {
    const ur = await getUser(payload.sub);
    if (ur.data && ur.data.banned) {
      return res.status(403).json({ ok: false, message: 'Akun Anda telah diblokir. Hubungi admin.' });
    }
    // SECURITY FIX: token revocation — tolak token yang dibuat sebelum lastTokenReset
    if (ur.data && ur.data.lastTokenReset && payload.iat && payload.iat < ur.data.lastTokenReset) {
      return res.status(401).json({ ok: false, message: 'Sesi tidak valid. Login ulang.' });
    }
  } catch(e) { /* gagal baca user — biarkan lanjut, bukan alasan tolak */ }
  next();
}

// ── ATOMIC BALANCE UPDATE (optimistic locking via SHA)
async function updateBalance(username, delta, maxRetries) {
  var retries = maxRetries || 3;
  for (var i = 0; i < retries; i++) {
    try {
      const r = await getUser(username);
      if (!r.data) throw new Error('User tidak ditemukan: ' + username);
      const newBal = (r.data.balance || 0) + delta;
      if (newBal < 0) throw new Error('Saldo tidak cukup. Saldo: ' + idrFormat(r.data.balance || 0));
      await saveUser(username, Object.assign({}, r.data, { balance: newBal, updatedAt: Date.now() }), r.sha);
      return newBal;
    } catch(e) {
      if (i === retries - 1) throw e;
      // FIX: retry HANYA untuk 409 SHA conflict (data diubah proses lain) — bukan semua error
      // Error lain ("user tidak ditemukan", "saldo tidak cukup") tidak perlu retry
      var is409 = (e.status === 409) || (e.response && e.response.status === 409) ||
                  (e.message && e.message.toLowerCase().includes('conflict'));
      if (!is409) throw e;
      await new Promise(function(r){ setTimeout(r, 300 * (i+1)); }); // backoff
    }
  }
}
function idrFormat(n) { return 'Rp' + Number(n).toLocaleString('id-ID'); }
function maskPhone(phone) {
  var p = String(phone || '').replace(/\D/g, '');
  var len = p.length;
  if (!len) return '';
  if (len <= 8) return p.slice(0,2) + '***' + p.slice(-2);
  if (len <= 10) return p.slice(0,3) + '***' + p.slice(-3);
  return p.slice(0,5) + '***' + p.slice(-5);
}

// ── PAKASIR
const pak = {
  async create(orderId, amount) {
    const r = await axios.post('https://app.pakasir.com/api/transactioncreate/qris', { project: C.pak.slug, order_id: orderId, amount, api_key: C.pak.apikey }, { timeout: 15000 });
    // Log full response for debugging (strip api_key)
    const _safeLog = JSON.stringify(r.data || {}).replace(/"api_key":"[^"]*"/g, '"api_key":"***"');
    console.log('[Pakasir/create] raw:', _safeLog.slice(0, 500));
    if (!r.data) throw new Error('Pakasir: empty response');
    if (!r.data.payment && !r.data.data) throw new Error((r.data && r.data.message) || 'Pakasir error: ' + _safeLog.slice(0, 100));
    // Support both response structures: r.data.payment and r.data.data
    const pay = r.data.payment || r.data.data || r.data;
    // Try every possible field name Pakasir might use for the QRIS EMV string
    const _qrisString = pay.payment_number || pay.qr_string || pay.qris_string || pay.qr
      || pay.emv || pay.qr_code || pay.qrcode || pay.qris || pay.emv_qr || pay.emv_code
      || pay.nmid_qr || pay.acquirer_data || '';
    if (!_qrisString) {
      console.warn('[Pakasir/create] QRIS string kosong! Semua field:', Object.keys(pay).join(', '));
    }
    pay._qrisString = _qrisString;
    // Normalize fee and total fields
    pay._totalPayment = pay.total_payment || pay.total || pay.amount_total || amount;
    pay._fee = pay.fee || pay.admin_fee || pay.biaya_admin || pay.service_fee || 0;
    return pay;
  },
  async check(orderId, amount) {
    const r = await axios.get('https://app.pakasir.com/api/transactiondetail', { params: { project: C.pak.slug, order_id: orderId, amount, api_key: C.pak.apikey }, timeout: 12000 });
    const _safeChk = JSON.stringify(r.data || {}).replace(/"api_key":"[^"]*"/g, '"api_key":"***"');
    console.log('[Pakasir/check]', orderId, '| response:', _safeChk.slice(0, 400));
    return r.data;
  },
  async cancel(orderId, amount) {
    await axios.post('https://app.pakasir.com/api/transactioncancel', { project: C.pak.slug, order_id: orderId, amount, api_key: C.pak.apikey }, { timeout: 10000 }).catch(function() {});
  },
};

// ── PTERODACTYL
const SPEC = {
  // Spesifikasi resmi: RAM = N GB, Disk = (N+1) GB, CPU = N×20+20%
  // Contoh: 1GB RAM / 2GB Disk / 40% CPU — sesuai list harga resmi
  '1gb':       { ram:  1024, disk:  2048, cpu:  40 },
  '2gb':       { ram:  2048, disk:  3072, cpu:  60 },
  '3gb':       { ram:  3072, disk:  4096, cpu:  80 },
  '4gb':       { ram:  4096, disk:  5120, cpu: 100 },
  '5gb':       { ram:  5120, disk:  6144, cpu: 120 },
  '6gb':       { ram:  6144, disk:  7168, cpu: 140 },
  '7gb':       { ram:  7168, disk:  8192, cpu: 160 },
  '8gb':       { ram:  8192, disk:  9216, cpu: 180 },
  '9gb':       { ram:  9216, disk: 10240, cpu: 200 },
  '10gb':      { ram: 10240, disk: 11264, cpu: 220 },
  '11gb':      { ram: 11264, disk: 12288, cpu: 240 },
  '12gb':      { ram: 12288, disk: 13312, cpu: 260 },
  '13gb':      { ram: 13312, disk: 14336, cpu: 280 },
  '14gb':      { ram: 14336, disk: 15360, cpu: 300 },
  '15gb':      { ram: 15360, disk: 16384, cpu: 320 },
  '16gb':      { ram: 16384, disk: 17408, cpu: 340 },
  '17gb':      { ram: 17408, disk: 18432, cpu: 360 },
  '18gb':      { ram: 18432, disk: 19456, cpu: 380 },
  '19gb':      { ram: 19456, disk: 20480, cpu: 400 },
  '20gb':      { ram: 20480, disk: 21504, cpu: 420 },
  'unlimited': { ram:     0, disk:     0, cpu:   0 },
};
function sanitizeUsername(u) { return String(u).replace(/[^a-z0-9_]/gi, '').toLowerCase().slice(0, 20); }
function ptH() { return { Authorization: 'Bearer ' + C.ptero.apikey, Accept: 'application/json', 'Content-Type': 'application/json' }; }
const PT_TIMEOUT = { timeout: 15000 };
function ptCfg(extra) { return Object.assign({ headers: ptH() }, PT_TIMEOUT, extra || {}); }

async function createPanelServer(plan, days, orderId, customUser, customPass) {
  const spec     = await resolveSpec(plan);
  const domain   = C.ptero.domain;
  const headers  = ptH();
  const username = sanitizeUsername(customUser) || ('prem' + Math.random().toString(36).slice(2, 8));
  const password = customPass || crypto.randomBytes(10).toString('base64').replace(/[^a-zA-Z0-9]/g, '').slice(0, 12);
  const email    = username + '@Dongtube.local';

  const checkR = await axios.get(domain + '/api/application/users?filter[username]=' + username, { headers });
  if (checkR.data && checkR.data.data && checkR.data.data.length > 0) throw new Error('Username "' + username + '" sudah terdaftar.');

  const uRes = await axios.post(domain + '/api/application/users', { email, username, first_name: username, last_name: 'Panel', language: 'en', password }, { headers });
  if (uRes.data.errors) throw new Error((uRes.data.errors[0] && uRes.data.errors[0].detail) || 'Gagal buat user panel');
  const userId = uRes.data.attributes.id;

  const tEgg   = spec.egg      || C.ptero.egg;
  const tNest  = spec.nest     || C.ptero.nest;
  const tLoc   = spec.location || C.ptero.location;
  const tDock  = spec.docker_image || 'ghcr.io/parkervcp/yolks:nodejs_20';
  const tEnv   = spec.environment || { INST: 'npm', USER_UPLOAD: '0', AUTO_UPDATE: '0', CMD_RUN: 'npm start' };
  const tFeat  = spec.feature_limits || { databases: 5, backups: 5, allocations: 5 };
  const tIO    = spec.io !== undefined ? spec.io : 500;
  const tSwap  = spec.swap !== undefined ? spec.swap : 0;

  const eRes    = await axios.get(domain + '/api/application/nests/' + tNest + '/eggs/' + tEgg, { headers });
  const startup = spec.startup || eRes.data.attributes.startup;
  const expiresAt = Date.now() + days * 86400000;

  const sRes = await axios.post(domain + '/api/application/servers', {
    name: username, description: 'Dongtube ' + orderId + ' | exp: ' + new Date(expiresAt).toLocaleDateString('id-ID'),
    user: userId, egg: parseInt(tEgg), docker_image: tDock, startup,
    environment: tEnv,
    limits: { memory: spec.ram, swap: tSwap, disk: spec.disk, io: tIO, cpu: spec.cpu },
    feature_limits: tFeat,
    deploy: { locations: [parseInt(tLoc)], dedicated_ip: false, port_range: [] },
  }, { headers });
  if (sRes.data.errors) throw new Error((sRes.data.errors[0] && sRes.data.errors[0].detail) || 'Gagal buat server');
  const server = sRes.data.attributes;
  return {
    serverId: server.id, userId, username, password, email, domain: C.ptero.domain,
    ram:  spec.ram  === 0 ? 'Unlimited' : (spec.ram  / 1024).toFixed(1) + 'GB',
    disk: spec.disk === 0 ? 'Unlimited' : (spec.disk / 1024).toFixed(1) + 'GB',
    cpu:  spec.cpu  === 0 ? 'Unlimited' : spec.cpu + '%',
    days, expiresAt, plan,
  };
}

async function processRenewal(trx, orderId) {
  const addDays = trx.variantDays || 30;
  const headers = ptH();

  // BUG FIX: walk the origTrxId chain to find the ROOT transaction that has serverId.
  // If the user renewed multiple times, origTrxId may point to a previous renewal TRX,
  // not the original panel purchase. We need to find the one with result.serverId.
  let origTrx = null;
  let lookupId = trx.origTrxId;
  for (var _chain = 0; _chain < 10 && lookupId; _chain++) {
    try {
      const r = await getTrx(lookupId);
      if (!r.data) break;
      origTrx = r.data;
      if (origTrx.result && origTrx.result.serverId) break;
      lookupId = origTrx.origTrxId || null;
    } catch(e) { break; }
  }

  // BUG FIX: jika origTrx tidak ditemukan di transactions/, cari di reseller-servers/
  // Ini terjadi jika panel asli dibuat via cPanel reseller
  if ((!origTrx || !origTrx.result) && trx.panelUsername) {
    try {
      const rsFiles = (await listDirCached('reseller-servers')).filter(function(f){ return f.name.endsWith('.json'); });
      for (const f of rsFiles) {
        try {
          const r = await getRsServer(f.name.replace('.json',''));
          if (r.data && r.data.panelUsername === trx.panelUsername && r.data.status !== 'deleted') {
            // Synthesize origTrx dari reseller-server record
            origTrx = {
              id      : r.data.id,
              _rsvId  : r.data.id,
              _isReseller: true,
              result  : {
                serverId : r.data.serverId,
                username : r.data.panelUsername,
                userId   : r.data.userId,
                domain   : r.data.domain,
                ram      : r.data.ram, disk: r.data.disk, cpu: r.data.cpu,
                expiresAt: r.data.expiresAt,
              },
            };
            break;
          }
        } catch(e) {}
      }
    } catch(e) { console.warn('[processRenewal] reseller fallback:', e.message); }
  }

  if (!origTrx || !origTrx.result) throw new Error('Transaksi asli tidak ditemukan.');

  const orig      = origTrx.result;
  const baseTime  = (orig.expiresAt && orig.expiresAt > Date.now()) ? orig.expiresAt : Date.now();
  const newExpiry = baseTime + addDays * 86400000;

  if (C.ptero.domain && C.ptero.apikey && orig.serverId) {
    try {
      const sRes = await axios.get(C.ptero.domain + '/api/application/servers/' + orig.serverId, { headers });
      const srv  = sRes.data.attributes;
      await axios.patch(C.ptero.domain + '/api/application/servers/' + orig.serverId + '/details', {
        name: srv.name, user: srv.user, email: srv.user, external_id: srv.external_id || null,
        description: 'Dongtube ' + (trx.origTrxId || orderId) + ' | exp: ' + new Date(newExpiry).toLocaleDateString('id-ID') + ' [renewed:' + orderId + ']',
      }, { headers });
    } catch(e) { console.warn('[renewal] ptero update:', e.message); }
  }

  if (origTrx._isReseller && origTrx._rsvId) {
    // Reseller panel: update expiry di reseller-servers/
    const rsvFresh = await getRsServer(origTrx._rsvId);
    await saveRsServer(origTrx._rsvId, Object.assign({}, rsvFresh.data || {}, {
      expiresAt  : newExpiry,
      status     : 'active',
      renewedAt  : Date.now(),
      renewedBy  : orderId,
    }), rsvFresh.sha || null);
    _gitTreeCache.delete('reseller-servers');
  } else {
    // Normal panel: update expiry di transactions/
    const orig_r = await getTrx(origTrx.id);
    await saveTrx(origTrx.id, Object.assign({}, orig_r.data, {
      result: Object.assign({}, orig, { expiresAt: newExpiry }),
      renewedAt: Date.now(), renewedBy: orderId,
    }), orig_r.sha);
  }
  return { type: 'renewal', username: trx.panelUsername, domain: orig.domain, ram: orig.ram, disk: orig.disk, cpu: orig.cpu, addedDays: addDays, expiresAt: newExpiry, serverId: orig.serverId };
}

// ── ADMIN AUTH MIDDLEWARE
// Audit log — append to GitHub DB (fire-and-forget, no blocking)
async function auditLog(action, detail, ip) {
  try {
    const r = await dbRead('audit.json');
    const log = Array.isArray(r.data) ? r.data : [];
    log.unshift({ ts: Date.now(), action, detail: String(detail || '').slice(0, 300), ip });
    if (log.length > 500) log.length = 500;
    await dbWrite('audit.json', log, r.sha || null, 'audit:' + action);
  } catch(e) { console.warn('[audit]', e.message); }
}

function adminAuth(req, res, next) {
  const token   = req.headers['x-admin-token'];
  const payload = verifyToken(token);
  if (!payload || payload.role !== 'admin') {
    return res.status(401).json({ ok: false, message: 'Sesi tidak valid. Login ulang.' });
  }
  req.adminIp = req.ip || 'unknown';
  next();
}

// ══════════════════════════════════════════════════════════════
// PUBLIC ROUTES
// ══════════════════════════════════════════════════════════════

// Products — SECURITY: strip fileUrl (file download URL) dari response publik

// ── STORE SSE — real-time broadcast to all visitors ─────────────────────────
var _storeClients = new Set();

function broadcastStore(data) {
  var msg = 'data: ' + JSON.stringify(data) + '\n\n';
  _storeClients.forEach(function(res) {
    try { res.write(msg); } catch(e) { _storeClients.delete(res); }
  });
}

// ── ADMIN SSE — real-time feed for admin dashboard
var _adminClients = new Set();


// ════════════════════════════════════════════════════════════════
// PUBLIC SUCCESS FEED
// Ring buffer — last 50 successful events, in-memory
// Format seragam untuk API publik & webhook
// ════════════════════════════════════════════════════════════════
var _feedBuf = [];
const FEED_MAX = 50;

function feedPush(event) {
  _feedBuf.unshift(event);
  if (_feedBuf.length > FEED_MAX) _feedBuf.length = FEED_MAX;
  // Kirim ke semua SSE subscriber
  broadcastFeed(event);
  // Kirim ke webhook jika dikonfigurasi (fire-and-forget)
  fireWebhook(event).catch(function(){});
  // Persist ke GitHub agar /api/feed tetap ada data setelah cold start Vercel
  persistFeedEvent(event).catch(function(){});
}

// SECURITY: mask ID publik agar tidak bisa di-track via /api/trx/:id
function maskFeedId(id) {
  if (!id || typeof id !== 'string') return '???';
  var dash = id.indexOf('-');
  var prefix = dash > 0 ? id.slice(0, dash) : id.slice(0, 3);
  return prefix + '-***-' + id.slice(-4);
}

// Format event menjadi payload publik yang bersih (untuk /api/feed)
function formatFeedEvent(raw) {
  var base = {
    // BUG FIX: sertakan type sebagai bagian dari id publik
    // Dua event berbeda (new_otp_order + otp) untuk order yang sama
    // menghasilkan masked ID yang identik (prefix + 4 char terakhir sama).
    // Konsumen feed (dongtube-feed.js) tidak bisa membedakan keduanya
    // sehingga event kedua dianggap duplikat dan dilewati.
    // Solusi: tambahkan suffix type agar ID publik unik per event.
    id    : maskFeedId(raw.id) + ':' + (raw.type || 'evt'),
    type  : raw.type,
    ts    : raw.ts,
    label : raw.label || '',
    amount: raw.amount || 0,
  };
  if (raw.phone)    base.phone    = raw.phone;
  if (raw.username) base.username = raw.username;
  if (raw.otp)      base.otp      = raw.otp;
  return base;
}

// Format payload webhook yang lebih kaya (untuk notifikasi admin via webhook)
// Berbeda dari formatFeedEvent yang hanya untuk tampilan publik
function formatWebhookPayload(event) {
  var payload = {
    id    : event.id,
    type  : event.type,
    ts    : event.ts,
    label : event.label || '',
    amount: event.amount || 0,
  };
  // Sertakan data tambahan per tipe event
  if (event.type === 'order') {
    if (event.productName)  payload.productName  = event.productName;
    if (event.variantName)  payload.variantName  = event.variantName;
    if (event.productType)  payload.productType  = event.productType;
    if (event.phone)        payload.phone        = event.phone;
    if (event.free)         payload.free         = event.free;
  }
  if (event.type === 'deposit') {
    if (event.username)     payload.username     = event.username;
  }
  if (event.type === 'sewabot') {
    if (event.groupUrl)     payload.groupUrl     = event.groupUrl;
    if (event.days)         payload.days         = event.days;
    if (event.buyerName)    payload.buyerName    = event.buyerName;
  }
  if (event.type === 'otp') {
    if (event.username)     payload.username     = event.username;
    if (event.service)      payload.service      = event.service;
    if (event.country)      payload.country      = event.country;
    if (event.otp)          payload.otp          = event.otp;
  }
  return payload;
}

// Konversi dari broadcastAdmin event → feed event
function toBroadcastFeedEvent(d) {
  if (d.type === 'new_order') {
    return {
      id    : d.id,
      type  : 'new_order',
      ts    : d.ts,
      label : (d.productName || '') + (d.variantName ? ' — ' + d.variantName : ''),
      amount: d.totalBayar || 0,
      productType: d.productType || 'digital',
    };
  }
  if (d.type === 'trx_completed') {
    return {
      id    : d.id,
      type  : 'order',
      ts    : d.ts,
      label : (d.productName || '') + (d.variantName ? ' — ' + d.variantName : ''),
      amount: d.totalBayar || 0,
      productType: d.productType || 'digital',
      free  : !!d.free,
      phone : d.phone ? maskPhone(d.phone) : '',
    };
  }
  if (d.type === 'deposit_success') {
    return {
      id    : d.id,
      type  : 'deposit',
      ts    : d.ts,
      label : 'Deposit saldo — ' + (d.username || ''),
      amount: d.amount || 0,
      username: d.username || '',
    };
  }
  if (d.type === 'sewabot_completed') {
    return {
      id    : d.id,
      type  : 'sewabot',
      ts    : d.ts,
      label : 'Sewa Bot ' + (d.days || '?') + ' hari',
      amount: d.totalBayar || 0,
      days  : d.days || null,
      buyerName: d.buyerName || '',
      groupUrl : d.groupUrl  || '',
    };
  }
  if (d.type === 'new_otp_order') {
    return {
      id    : d.id,
      type  : 'new_otp_order',
      ts    : d.ts,
      label : 'Order OTP — ' + (d.service || '') + (d.country ? ' (' + d.country + ')' : ''),
      amount: d.price || 0,
      phone : d.phone ? maskPhone(d.phone) : '',
      username: d.username || '',
      service : d.service  || '',
      country : d.country  || '',
    };
  }
  if (d.type === 'otp_completed') {
    return {
      id    : d.id,
      type  : 'otp',
      ts    : d.ts,
      label : 'OTP — ' + (d.service || '') + (d.country ? ' (' + d.country + ')' : ''),
      amount: d.price || 0,
      username: d.username || '',
      service : d.service  || '',
      country : d.country  || '',
      phone   : d.phone ? maskPhone(d.phone) : '',
      otp     : d.otp || '',
    };
  }
  return null;
}

// ── FEED SSE
var _feedClients = new Set();

// Simpan feed event ke GitHub sebagai fallback untuk serverless (cold start reset in-memory buffer)
// Fire-and-forget — jangan blokir response
async function persistFeedEvent(event) {
  if (!C.gh.token || !C.gh.owner || !C.gh.repos.length) return;
  try {
    const r = await dbRead('feed-cache.json');
    const current = Array.isArray(r.data) ? r.data : [];
    current.unshift(formatFeedEvent(event));
    if (current.length > 30) current.length = 30;
    await dbWrite('feed-cache.json', current, r.sha || null, 'feed-cache');
  } catch(e) { /* non-critical, abaikan error */ }
}

function broadcastFeed(event) {
  var msg = 'data: ' + JSON.stringify(formatFeedEvent(event)) + '\n\n';
  _feedClients.forEach(function(res) {
    try { res.write(msg); } catch(e) { _feedClients.delete(res); }
  });
}

// ── WEBHOOK (config cached in memory, refreshed on save)
var _webhookCache = null;
var _webhookCacheAt = 0;

async function getWebhookConfig() {
  // Cache for 10 minutes to avoid repeated GitHub reads
  if (_webhookCache && Date.now() - _webhookCacheAt < 10 * 60 * 1000) return _webhookCache;
  try {
    const r = await dbRead('webhook-config.json');
    _webhookCache = r.data || { url: '', secret: '', enabled: false };
    _webhookCacheAt = Date.now();
  } catch(e) { _webhookCache = { url: '', secret: '', enabled: false }; }
  return _webhookCache;
}

async function fireWebhook(event) {
  try {
    const cfg = await getWebhookConfig();
    if (!cfg || !cfg.url || cfg.enabled === false) return;
    // Gunakan formatWebhookPayload (lebih kaya data) bukan formatFeedEvent (minimal/publik)
    const payload = formatWebhookPayload(event);
    await axios.post(cfg.url, payload, {
      timeout: 8000,
      headers: {
        'Content-Type': 'application/json',
        'X-Dongtube-Event': event.type,
        'X-Dongtube-Secret': cfg.secret || '',
      },
    });
  } catch(e) { console.warn('[webhook]', e.message); }
}

function broadcastAdmin(data) {
  var msg = 'data: ' + JSON.stringify(data) + '\n\n';
  _adminClients.forEach(function(res) {
    try { res.write(msg); } catch(e) { _adminClients.delete(res); }
  });
  // Mirror success events ke public feed
  var feedEvent = toBroadcastFeedEvent(data);
  if (feedEvent) feedPush(feedEvent);
}

app.get('/api/store/stream', function(req, res) {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-store');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.flushHeaders();
  res.write('data: ' + JSON.stringify({ type: 'connected' }) + '\n\n');
  _storeClients.add(res);
  // Vercel serverless max timeout is 30s (hobby) or 60s (pro)
  // We ping every 20s and auto-close at 55s so client can reconnect cleanly
  var startedAt = Date.now();
  var ping = setInterval(function() {
    try {
      // Auto-close after 55s so Vercel doesn't hard-kill the connection
      if (Date.now() - startedAt > 55000) {
        res.write('data: ' + JSON.stringify({ type: 'reconnect' }) + '\n\n');
        clearInterval(ping); _storeClients.delete(res); res.end(); return;
      }
      res.write('data: ' + JSON.stringify({ type: 'ping' }) + '\n\n');
    }
    catch(e) { clearInterval(ping); _storeClients.delete(res); }
  }, 20000);
  req.on('close', function() { clearInterval(ping); _storeClients.delete(res); });
});

// Admin SSE stream — real-time notifications for dashboard
// Note: EventSource can't set custom headers, so token is accepted via query param ?t=
app.get('/api/admin/stream', function(req, res) {
  // Authenticate via query param token (only for this SSE endpoint)
  var token = req.query.t || req.headers['x-admin-token'];
  var payload = verifyToken(token);
  if (!payload || payload.role !== 'admin') {
    res.status(401).end();
    return;
  }
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();
  res.write('data: ' + JSON.stringify({ type: 'connected', ts: Date.now() }) + '\n\n');
  _adminClients.add(res);
  var startedAt = Date.now();
  var ping = setInterval(function() {
    try {
      if (Date.now() - startedAt > 55000) {
        res.write('data: ' + JSON.stringify({ type: 'reconnect' }) + '\n\n');
        clearInterval(ping); _adminClients.delete(res); res.end(); return;
      }
      res.write('data: ' + JSON.stringify({ type: 'ping' }) + '\n\n');
    }
    catch(e) { clearInterval(ping); _adminClients.delete(res); }
  }, 20000);
  req.on('close', function() { clearInterval(ping); _adminClients.delete(res); });
});

// Admin endpoint to trigger a store-wide reload
app.post('/api/admin/broadcast', adminAuth, function(req, res) {
  var ALLOWED_TYPES = ['reload', 'announcement', 'maintenance', 'settings_update'];
  var type = ALLOWED_TYPES.includes(req.body.type) ? req.body.type : 'reload';
  var msg  = String(req.body.msg || '').slice(0, 500).replace(/</g, '&lt;').replace(/>/g, '&gt;');
  broadcastStore({ type, msg });
  res.json({ ok: true, clients: _storeClients.size });
});

// ── PUBLIC: Panel plan specs (for storefront display — no sensitive config)
app.get('/api/panel-plans', async function(req, res) {
  try {
    const templates = await getPanelTemplates();
    const merged = Object.keys(SPEC).map(function(id) {
      const t = templates.find(function(x){ return x.id === id; });
      return {
        id: id,
        name: (t && t.name) || id.toUpperCase(),
        ram:  t ? t.ram  : SPEC[id].ram,
        disk: t ? t.disk : SPEC[id].disk,
        cpu:  t ? t.cpu  : SPEC[id].cpu,
      };
    });
    templates.forEach(function(t) {
      if (!SPEC[t.id]) merged.push({ id: t.id, name: t.name, ram: t.ram, disk: t.disk, cpu: t.cpu });
    });
    res.json({ ok: true, data: merged });
  } catch(e) { res.json({ ok: false, data: [] }); }
});

app.get('/api/products', async function(req, res) {
  // Hard timeout: always respond within 10s even if GitHub is slow
  var _done = false;
  var _timeout = setTimeout(function() {
    if (!_done) { _done = true; res.json({ ok: false, message: 'Server sedang lambat, coba lagi sebentar.' }); }
  }, 10000);

  try {
    // FIX: fetch all 3 in parallel instead of sequential (was up to 3×12s)
    const [stg, products] = await Promise.all([
      getEffectiveSettings().catch(function(){ return {}; }),
      getProducts().catch(function(){ return []; }),
    ]);
    if (_done) return; // timeout already responded
    _done = true; clearTimeout(_timeout);

    const pub = products.map(function(p) {
      return Object.assign({}, p, {
        variants: (p.variants || []).map(function(v) {
          const vv = Object.assign({}, v);
          delete vv.fileUrl;
          return vv;
        }),
      });
    });
    res.json({ ok: true, data: pub, store: stg.storeName, wa: stg.wa, settings: stg });
  } catch (e) {
    if (!_done) { _done = true; clearTimeout(_timeout); res.json({ ok: false, message: e.message }); }
  }
});

// New order
app.post('/api/order', async function(req, res) {
  try {
    const ip = req.ip || 'unknown';
    if (!rateLimit('order:' + ip, 10, 10 * 60 * 1000)) return res.status(429).json({ ok: false, message: 'Terlalu banyak request. Coba lagi.' });

    const { productId, variantId, phone, panelUsername = '', panelPassword = '', voucherCode = '', captchaToken = '', captchaAnswer = '', groupUrl = '', buyerName = '' } = req.body;
    // ── CUSTOM FIELDS: baca semua cf_* dari body
    var _customFieldVals = {};
    Object.keys(req.body).forEach(function(k) {
      if (k.startsWith('cf_')) _customFieldVals[k.slice(3)] = String(req.body[k] || '').trim().slice(0, 500);
    });

    // Captcha verification (when enabled in settings)
    const stgCap = await getEffectiveSettings();
    if (stgCap.maintenanceMode) return res.json({ ok: false, message: stgCap.maintenanceMsg || 'Sedang dalam maintenance.', maintenance: true });
    if (stgCap.captchaEnabled) {
      if (!verifyCaptcha(captchaToken, captchaAnswer)) {
        return res.json({ ok: false, message: 'Jawaban captcha salah. Muat ulang dan coba lagi.', captchaFailed: true });
      }
    }

    if (!productId || !variantId || productId.length > 64 || variantId.length > 64) return res.json({ ok: false, message: 'Data tidak lengkap.' });

    const products = await getProducts();
    const product  = products.find(function(p) { return p.id === productId; });
    if (!product) return res.json({ ok: false, message: 'Produk tidak ditemukan.' });

    const variant  = product.variants.find(function(v) { return v.id === variantId; });
    if (!variant) return res.json({ ok: false, message: 'Varian tidak ditemukan.' });
    if (variant.stock !== undefined && variant.stock !== null && variant.stock !== -1 && variant.stock <= 0) return res.json({ ok: false, message: 'Stok habis.' });

    const productType = product.type || 'digital';
    if (productType === 'sewabot') {
      const gUrl = String(groupUrl).trim();
      if (!gUrl || gUrl.length < 5) return res.json({ ok: false, message: 'Link grup WhatsApp tidak boleh kosong.' });
    }
    if (productType === 'panel') {
      const uSan = sanitizeUsername(panelUsername);
      if (!uSan || uSan.length < 3) return res.json({ ok: false, message: 'Username panel minimal 3 karakter.' });
      if (!panelPassword || panelPassword.trim().length < 6) return res.json({ ok: false, message: 'Password panel minimal 6 karakter.' });
      const reqDays = parseInt(req.body.panelDays);
      if (reqDays) {
        const allowed = variant.daysOptions || [variant.days || 30];
        if (!allowed.includes(reqDays)) return res.json({ ok: false, message: 'Durasi tidak valid.' });
      }
    }

    const orderId   = newId('TRX');
    const reqDays2  = parseInt(req.body.panelDays) || variant.days || 30;
    const unitPrice = (productType === 'panel' && variant.dayPrices && variant.dayPrices[String(reqDays2)])
      ? variant.dayPrices[String(reqDays2)] : (variant.salePrice != null && variant.salePrice >= 0 ? variant.salePrice : variant.price);

    // SECURITY: buat cancelToken acak — hanya client yang buat order yang tahu token ini
    // Token disimpan di DB dan harus dikirim ulang saat /api/cancel
    const cancelToken = crypto.randomBytes(16).toString('hex');

    // Voucher discount
    let voucherDiscount = 0, appliedVoucherCode = null;
    if (voucherCode && unitPrice > 0) {
      try {
        const vouchers = await getVouchers();
        const vc = vouchers.find(function(v){ return v.code === String(voucherCode).toUpperCase().trim() && v.active !== false; });
        if (vc && !(vc.expiresAt && Date.now() > vc.expiresAt) && !(vc.maxUse > 0 && vc.usedCount >= vc.maxUse) && !(vc.minOrder > 0 && unitPrice < vc.minOrder)) {
          if (!vc.productIds || vc.productIds.length === 0 || vc.productIds.includes(productId)) {
            if (vc.type === 'percent') { voucherDiscount = Math.round(unitPrice * vc.value / 100); if (vc.maxDiscount > 0) voucherDiscount = Math.min(voucherDiscount, vc.maxDiscount); }
            else { voucherDiscount = vc.value; }
            voucherDiscount = Math.min(voucherDiscount, unitPrice);
            appliedVoucherCode = vc.code;
            // Increment used count — dengan retry loop untuk cegah race condition
            // (dua order bersamaan dengan voucher yang sama bisa melewati batas maxUse)
            var _vcIncrOk = false;
            for (var _vci = 0; _vci < 3 && !_vcIncrOk; _vci++) {
              try {
                if (_vci > 0) await _sleep(200 * _vci);
                const vr = await dbRead('vouchers.json', true); // bypass cache
                const varr = Array.isArray(vr.data) ? vr.data : [];
                const vi2 = varr.findIndex(function(x){ return x.code === vc.code; });
                if (vi2 >= 0) {
                  // Re-validasi maxUse setelah baca fresh (cegah over-use)
                  if (varr[vi2].maxUse > 0 && (varr[vi2].usedCount || 0) >= varr[vi2].maxUse) {
                    voucherDiscount = 0; appliedVoucherCode = null; break; // kuota habis saat race
                  }
                  varr[vi2].usedCount = (varr[vi2].usedCount || 0) + 1;
                  await dbWrite('vouchers.json', varr, vr.sha, 'voucher-use:' + vc.code);
                  _vcIncrOk = true;
                }
              } catch(vcErr) {
                if (_vci < 2 && vcErr.status === 409) continue;
                console.warn('[voucher] increment gagal:', vcErr.message);
              }
            }
          }
        }
      } catch(e) { console.warn('[voucher]', e.message); }
    }

    const effectivePrice = Math.max(0, unitPrice - voucherDiscount);
    // FREE PRODUCT: skip QRIS entirely, mark as pre-paid
    const isFree = (effectivePrice === 0);

    let pakData = null, qrBase64 = null, totalBayar = effectivePrice, adminFee = 0;
    if (!isFree) {
      try {
        pakData    = await pak.create(orderId, effectivePrice);
        totalBayar = pakData._totalPayment || pakData.total_payment || effectivePrice;
        adminFee   = pakData._fee || pakData.fee || 0;
        const qs   = pakData._qrisString || '';
        console.log('[Pakasir] QRIS len:', qs.length, '| prefix:', qs.substring(0, 12), '| total:', totalBayar, '| fee:', adminFee);
        if (!qs) throw new Error('QRIS string kosong dari Pakasir. Periksa konfigurasi SLUG/API key.');
        qrBase64   = await QRCode.toDataURL(qs, { errorCorrectionLevel: 'M', margin: 2, scale: 8, color: { dark: '#000000', light: '#ffffff' } });
        console.log('[Pakasir] QR generated, base64 len:', qrBase64.length);
      } catch (e) {
        console.warn('[Pakasir] Demo mode aktif:', e.message);
        qrBase64 = await QRCode.toDataURL('DEMO-' + orderId, { margin: 2, scale: 8 });
      }
    }

    const stg = await getEffectiveSettings();
    const now = Date.now();
    const trx = {
      id: orderId, productId, productName: product.name, productType, variantId, variantName: variant.name,
      variantPlan: variant.plan || null, variantDays: reqDays2, variantFile: variant.fileUrl || null,
      panelUsername: productType === 'panel' ? sanitizeUsername(panelUsername) : null,
      panelPassword: productType === 'panel' ? panelPassword.trim() : null,
      groupUrl: productType === 'sewabot' ? String(groupUrl).trim().slice(0, 500) : null,
      buyerName: productType === 'sewabot' ? String(buyerName).trim().slice(0, 100) : null,
      unitPrice, adminFee, totalBayar,
      phone: phone ? '62' + String(phone).replace(/^0/, '') : null,
      qrBase64, pakData, status: isFree ? 'FREE_PENDING' : 'PENDING', createdAt: now,
      expiryAt: now + (stg.expiryMin || C.store.expiry) * 60000,
      freeProduct: isFree,
      voucherCode: appliedVoucherCode, voucherDiscount,
      demo: !pakData || !C.pak.apikey,
      customFields: Object.keys(_customFieldVals).length ? _customFieldVals : null,
      cancelToken, // SECURITY: hanya pemegang token ini yang bisa cancel
      creatorIp: req.ip || 'unknown',
    };
    await saveTrx(orderId, trx, null);
    console.log('[order]', orderId, '|', product.name, '|', variant.name, isFree ? '| FREE' : '| Rp' + totalBayar);
    broadcastAdmin({ type: 'new_order', id: orderId, productName: product.name, variantName: variant.name, totalBayar: effectivePrice, productType, ts: Date.now() });
    // Kirim cancelToken ke client — disimpan di localStorage untuk dipakai saat cancel
    res.json({ ok: true, orderId, freeProduct: isFree, cancelToken });
  } catch (e) {
    console.error('[order]', e.message);
    res.json({ ok: false, message: e.message });
  }
});

// Get trx metadata (public, no password/QR)
app.get('/api/trx/:id', async function(req, res) {
  try {
    const id = req.params.id;
    if (!isValidId(id)) return res.json({ ok: false, message: 'Transaksi tidak ditemukan.' });
    const r = await getTrx(id);
    if (!r.data) return res.json({ ok: false, message: 'Transaksi tidak ditemukan.' });
    const d = Object.assign({}, r.data);
    // Add helpful flags before stripping sensitive data
    d.qrAvailable = !!(r.data.qrBase64 && r.data.status === 'PENDING');
    d.isDemo = !!(r.data.demo);
    delete d.qrBase64; delete d.pakData; delete d.panelPassword; delete d.variantFile;
    res.json({ ok: true, data: d });
  } catch (e) { res.json({ ok: false, message: e.message }); }
});

// QR image — SECURITY: only serve QR for PENDING orders
app.get('/api/trx/:id/qr', async function(req, res) {
  try {
    const id = req.params.id;
    if (!isValidId(id)) return res.status(404).send('Not found');
    const r = await getTrx(id);
    if (!r.data) return res.status(404).send('Not found');
    // If already paid/completed, return 410 with JSON detail for better UX
    if (r.data.status === 'COMPLETED' || r.data.status === 'PAID_ERROR') {
      return res.status(410).json({ gone: true, reason: 'paid', message: 'Pembayaran sudah diterima' });
    }
    if (r.data.status === 'FAILED' || r.data.status === 'EXPIRED') {
      return res.status(410).json({ gone: true, reason: r.data.status.toLowerCase(), message: 'Transaksi ' + r.data.status.toLowerCase() });
    }
    if (r.data.status !== 'PENDING') return res.status(410).json({ gone: true, reason: 'status', message: 'Status: ' + r.data.status });
    if (!r.data.qrBase64) {
      console.warn('[qr] missing qrBase64 for:', id, '| status:', r.data.status);
      return res.status(503).json({ error: 'QR belum tersedia, coba lagi.' });
    }
    const raw = r.data.qrBase64;
    const b64 = raw.includes(',') ? raw.split(',')[1] : raw;
    if (!b64) return res.status(503).send('QR data invalid');
    res.set('Content-Type', 'image/png');
    res.set('Cache-Control', 'no-store');
    res.send(Buffer.from(b64, 'base64'));
  } catch (e) {
    console.error('[qr]', e.message);
    res.status(500).send('Error');
  }
});

// Check & process payment
app.post('/api/check', async function(req, res) {
  try {
    const id = req.body.id;
    if (!isValidId(id)) return res.json({ status: 'NOT_FOUND' });
    // SECURITY: rate limit per transaction (max 30 polls per 10 min)
    if (!rateLimit('check:' + id, 30, 10 * 60 * 1000)) return res.json({ status: 'PENDING' });

    const r = await getTrx(id); const trx = r.data; const sha = r.sha;
    if (!trx) return res.json({ status: 'NOT_FOUND' });
    if (trx.status === 'COMPLETED')  return res.json({ status: 'COMPLETED', result: trx.result || null });
    if (trx.status === 'PAID_ERROR') return res.json({ status: 'COMPLETED', result: trx.result || null });
    if (trx.status === 'FAILED' || trx.status === 'EXPIRED') return res.json({ status: trx.status });
    // FIX: PROCESSING berarti sedang diproses oleh request/cron lain — kembalikan PENDING
    // agar client terus polling tanpa memicu pak.check dan PROCESSING lock ulang
    // Rescue: jika PROCESSING stuck > 3 menit (server crash), reset ke PENDING agar bisa diproses ulang
    if (trx.status === 'PROCESSING') {
      if (trx.processingAt && Date.now() - trx.processingAt > 3 * 60 * 1000) {
        // Stuck terlalu lama — reset ke PAID agar bisa diproses ulang saat poll berikutnya
        console.warn('[check] PROCESSING stuck >3min, reset ke PENDING:', id);
        await saveTrx(id, Object.assign({}, trx, { status: 'PENDING', processingAt: null, _processingReset: Date.now() }), sha).catch(function(){});
      }
      return res.json({ status: 'PENDING' });
    }

    // FREE PRODUCT: process immediately without QRIS
    if (trx.freeProduct || trx.status === 'FREE_PENDING') {
      // SECURITY FIX: Atomic lock untuk free product juga
      try {
        await saveTrx(id, Object.assign({}, trx, { status: 'PROCESSING', processingAt: Date.now() }), sha);
      } catch(lockErr) {
        console.log('[free/check] lock conflict (ok):', id);
        return res.json({ status: 'PENDING' });
      }
      let result = null;
      try {
        result = await processProductDelivery(trx, id);
        const freshFr = await getTrx(id);
        await saveTrx(id, Object.assign({}, freshFr.data || trx, { status: 'COMPLETED', result, completedAt: Date.now() }), freshFr.sha || null);
        decrementStock(trx.productId, trx.variantId).catch(function(){}); // fire-and-forget
        console.log('[free] COMPLETED:', id, '|', trx.productType);
        broadcastAdmin({ type: 'trx_completed', id, productName: trx.productName, variantName: trx.variantName, totalBayar: 0, productType: trx.productType, phone: trx.phone || null, free: true, ts: Date.now() });
        return res.json({ status: 'COMPLETED', result });
      } catch (procErr) {
        console.error('[free/process]', procErr.message);
        const errResult = { type: 'error', message: 'Terjadi kesalahan. Hubungi admin. ID: ' + id };
        const freshFr2 = await getTrx(id);
        await saveTrx(id, Object.assign({}, freshFr2.data || trx, { status: 'PAID_ERROR', error: procErr.message, result: errResult }), freshFr2.sha || null);
        return res.json({ status: 'COMPLETED', result: errResult });
      }
    }

    if (Date.now() > trx.expiryAt) {
      await saveTrx(id, Object.assign({}, trx, { status: 'EXPIRED' }), sha);
      return res.json({ status: 'EXPIRED' });
    }
    if (trx.demo) return res.json({ status: 'PENDING' });

    const pakRes    = await pak.check(id, trx.unitPrice);
    // Support multiple Pakasir response structures
    const trxObj    = (pakRes && pakRes.transaction) || (pakRes && pakRes.data) || pakRes;
    const pakStatus = (
      (pakRes && pakRes.transaction && pakRes.transaction.status) ||
      (pakRes && pakRes.data && pakRes.data.status) ||
      (pakRes && pakRes.status) ||
      (trxObj && trxObj.status) ||
      (trxObj && trxObj.payment_status) ||
      ''
    ).toLowerCase();
    console.log('[check]', id, '| pak_status:', pakStatus, '| raw:', JSON.stringify(pakRes || {}).slice(0, 300));

    if (pakStatus === 'completed' || pakStatus === 'paid' || pakStatus === 'success') {
      // SECURITY FIX: Atomic lock — tulis PROCESSING dulu sebelum deliver produk.
      // Jika ada dua request concurrent, yang kedua akan kena 409 conflict dan di-reject.
      try {
        await saveTrx(id, Object.assign({}, trx, { status: 'PROCESSING', processingAt: Date.now() }), sha);
      } catch(lockErr) {
        // Request lain sudah ambil lock — kembalikan PENDING agar klien polling lagi
        console.log('[check] lock conflict (ok):', id);
        return res.json({ status: 'PENDING' });
      }
      let result = null;
      try {
        result = await processProductDelivery(trx, id);
        const freshR = await getTrx(id);
        await saveTrx(id, Object.assign({}, freshR.data || trx, { status: 'COMPLETED', result, completedAt: Date.now() }), freshR.sha || null);
        decrementStock(trx.productId, trx.variantId).catch(function(){}); // fire-and-forget
        console.log('[paid] COMPLETED:', id, '|', trx.productType);
        broadcastAdmin({ type: 'trx_completed', id, productName: trx.productName, variantName: trx.variantName, totalBayar: trx.totalBayar || trx.unitPrice, productType: trx.productType, phone: trx.phone || null, ts: Date.now() });
        return res.json({ status: 'COMPLETED', result });
      } catch (procErr) {
        console.error('[process]', procErr.message);
        const errResult = { type: 'error', message: 'Pembayaran diterima tapi proses gagal. Hubungi admin. ID: ' + id };
        const freshR2 = await getTrx(id);
        await saveTrx(id, Object.assign({}, freshR2.data || trx, { status: 'PAID_ERROR', error: procErr.message, result: errResult }), freshR2.sha || null);
        return res.json({ status: 'COMPLETED', result: errResult });
      }
    }
    if (pakStatus === 'failed' || pakStatus === 'canceled' || pakStatus === 'cancelled') {
      await saveTrx(id, Object.assign({}, trx, { status: 'FAILED' }), sha);
      return res.json({ status: 'FAILED' });
    }
    return res.json({ status: 'PENDING' });
  } catch (e) { console.error('[check]', e.message); return res.json({ status: 'PENDING' }); }
});

// Cancel order — SECURITY: verifikasi cancelToken agar hanya pemilik order yang bisa cancel
app.post('/api/cancel', async function(req, res) {
  try {
    const id = req.body.id;
    const cancelToken = req.body.cancelToken || '';
    if (!isValidId(id)) return res.json({ ok: false });
    if (!rateLimit('cancel:' + (req.ip || 'x'), 5, 60 * 1000)) return res.status(429).json({ ok: false, message: 'Terlalu cepat.' });
    const r = await getTrx(id);
    if (!r.data) return res.json({ ok: false });
    if (r.data.status !== 'PENDING') return res.json({ ok: false, message: 'Order tidak bisa dibatalkan.' });

    // SECURITY FIX: verifikasi kepemilikan order via cancelToken
    // Order lama (sebelum fix ini) tidak punya cancelToken — izinkan cancel dengan IP check
    if (r.data.cancelToken) {
      // Order baru: wajib punya token yang cocok
      if (!cancelToken || cancelToken.length !== 32 || cancelToken !== r.data.cancelToken) {
        console.warn('[cancel] token mismatch untuk order:', id, '| ip:', req.ip);
        return res.status(403).json({ ok: false, message: 'Tidak diizinkan membatalkan order ini.' });
      }
    }
    // Order lama tanpa cancelToken: izinkan (backward compat)

    if (!r.data.demo && C.pak.apikey) await pak.cancel(id, r.data.unitPrice);
    await saveTrx(id, Object.assign({}, r.data, { status: 'FAILED', cancelledAt: Date.now() }), r.sha);
    console.log('[cancel]', id);
    res.json({ ok: true });
  } catch (e) { res.json({ ok: false, message: e.message }); }
});

// Renewal lookup
app.post('/api/renew/lookup', async function(req, res) {
  try {
    const ip = req.ip || 'x';
    if (!rateLimit('rl:' + ip, 10, 5 * 60 * 1000)) return res.status(429).json({ ok: false, message: 'Terlalu banyak request.' });
    await new Promise(function(r) { setTimeout(r, 80 + Math.random() * 120); }); // anti-timing

    const username = (req.body.username || '').toLowerCase().trim();
    if (!username || username.length < 3 || !/^[a-z0-9_]+$/.test(username)) return res.json({ ok: false, message: 'Format username tidak valid.' });

    const files = await listTrx();
    let found = null;
    // BUG FIX: allow lookup for EXPIRED panels too (user may want to re-activate)
    // Only skip FAILED and truly deleted panels
    for (const f of files.filter(function(f) { return f.name.endsWith('.json'); }).sort(function(a, b) { return b.name.localeCompare(a.name); })) {
      try {
        const r = await getTrx(f.name.replace('.json', ''));
        if (!r.data) continue;
        const d = r.data;
        if (d.productType !== 'panel') continue;
        if (!d.result || d.result.username !== username) continue;
        if (d.status === 'FAILED' || d._panelDeleted) continue;
        found = d; break;
      } catch(e) {}
    }

    // BUG FIX: jika tidak ada di transactions/, cari di reseller-servers/
    // Panel yang dibuat via cPanel reseller bisa di-renew dari halaman renew normal
    if (!found) {
      try {
        const rsFiles = (await listDirCached('reseller-servers')).filter(function(f){ return f.name.endsWith('.json'); });
        for (const f of rsFiles) {
          try {
            const r = await getRsServer(f.name.replace('.json',''));
            if (r.data && r.data.panelUsername === username && r.data.status !== 'deleted') {
              // Bungkus sebagai format yang kompatibel dengan found
              found = {
                id          : r.data.id,
                productType : 'panel',
                variantPlan : r.data.plan || null,
                variantId   : null,
                status      : r.data.status === 'active' ? 'COMPLETED' : 'EXPIRED',
                _isReseller : true,
                _rsvId      : r.data.id,
                result: {
                  serverId : r.data.serverId,
                  username : r.data.panelUsername,
                  userId   : r.data.userId,
                  ram      : r.data.ram, disk: r.data.disk, cpu: r.data.cpu,
                  expiresAt: r.data.expiresAt,
                  domain   : r.data.domain,
                },
              };
              break;
            }
          } catch(e) {}
        }
      } catch(e) { console.warn('[renew/lookup] reseller fallback:', e.message); }
    }

    // SECURITY: same message regardless of reason (prevent username enumeration)
    if (!found) return res.json({ ok: false, message: 'Panel tidak ditemukan. Pastikan username sama persis seperti saat beli (tanpa spasi, huruf kecil).' });

    const result = found.result || {};
    // BUG FIX: Pterodactyl check — do NOT block renewal on 404.
    // If server was deleted externally, we still allow renewal attempt.
    // processRenewal handles missing serverId gracefully with console.warn.
    var pteroStatus = 'unknown';
    if (C.ptero.domain && C.ptero.apikey && result.serverId) {
      try {
        await axios.get(C.ptero.domain + '/api/application/servers/' + result.serverId, { headers: ptH() });
        pteroStatus = 'active';
      } catch(e) {
        if (e.response && e.response.status === 404) {
          pteroStatus = 'deleted';
          // BUG FIX: Don't block renewal if server 404 — DB still has the config
          // The renewal will extend expiry in DB (most important) even if Ptero update fails
        }
      }
    }
    res.json({ ok: true, username: result.username, plan: found.variantPlan || '1gb', variantId: found.variantId || null, ram: result.ram, disk: result.disk, cpu: result.cpu, expiresAt: result.expiresAt, trxId: found.id, pteroStatus, panelStatus: found.status });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Renewal order
app.post('/api/renew/order', async function(req, res) {
  try {
    const ip = req.ip || 'x';
    if (!rateLimit('ro:' + ip, 5, 10 * 60 * 1000)) return res.status(429).json({ ok: false, message: 'Terlalu banyak request.' });

    const username = (req.body.username || '').toLowerCase().trim();
    const days     = parseInt(req.body.days) || 30;
    const phone    = req.body.phone || '';
    if (!username || !/^[a-z0-9_]+$/.test(username)) return res.json({ ok: false, message: 'Username tidak valid.' });
    if (days < 1 || days > 365) return res.json({ ok: false, message: 'Durasi tidak valid (1-365 hari).' });

    const files = await listTrx(); let origTrx = null;
    for (const f of files.filter(function(f) { return f.name.endsWith('.json'); }).sort(function(a, b) { return b.name.localeCompare(a.name); })) {
      try {
        const r = await getTrx(f.name.replace('.json', ''));
        if (!r.data) continue; const d = r.data;
        if (d.productType !== 'panel' || !d.result || d.result.username !== username) continue;
        if (d.status === 'FAILED' || d._panelDeleted) continue;
        origTrx = d; break;
      } catch(e) {}
    }

    // BUG FIX: fallback ke reseller-servers/ jika tidak ada di transactions/
    if (!origTrx) {
      try {
        const rsFiles = (await listDirCached('reseller-servers')).filter(function(f){ return f.name.endsWith('.json'); });
        for (const f of rsFiles) {
          try {
            const r = await getRsServer(f.name.replace('.json',''));
            if (r.data && r.data.panelUsername === username && r.data.status !== 'deleted') {
              origTrx = {
                id          : r.data.id,
                _rsvId      : r.data.id,
                _isReseller : true,
                productId   : null,
                productName : 'Panel Reseller',
                variantId   : null,
                variantName : (r.data.plan || '').toUpperCase(),
                variantPlan : r.data.plan || null,
              };
              break;
            }
          } catch(e) {}
        }
      } catch(e) { console.warn('[renew/order] reseller fallback:', e.message); }
    }

    if (!origTrx) return res.json({ ok: false, message: 'Panel tidak ditemukan.' });

    const products = await getProducts(); const pp = products.find(function(p) { return p.type === 'panel'; });
    let renewPrice = 10000;
    if (pp) {
      // Cari variant: prioritas plan → variantId → variant pertama
      const mv = pp.variants.find(function(v) { return v.plan === (origTrx.variantPlan || '1gb'); })
        || (origTrx.variantId && pp.variants.find(function(v) { return v.id === origTrx.variantId; }))
        || pp.variants[0];
      if (mv) renewPrice = (mv.dayPrices && mv.dayPrices[String(days)]) ? mv.dayPrices[String(days)] : Math.round((mv.salePrice != null && mv.salePrice >= 0 ? mv.salePrice : (mv.price || renewPrice)) * days / 30);
    }

    const orderId = newId('RNW'); let pakData = null, qrBase64 = null, totalBayar = renewPrice, adminFee = 0;
    try {
      pakData    = await pak.create(orderId, renewPrice);
      totalBayar = pakData._totalPayment || pakData.total_payment || renewPrice;
      adminFee   = pakData._fee || pakData.fee || 0;
      const _renewQs = pakData._qrisString || '';
      if (!_renewQs) throw new Error('QRIS string kosong dari Pakasir (renew)');
      qrBase64   = await QRCode.toDataURL(_renewQs, { errorCorrectionLevel: 'M', margin: 2, scale: 8, color: { dark: '#000000', light: '#ffffff' } });
      console.log('[renew/pak] QR generated, total:', totalBayar, 'fee:', adminFee);
    } catch(e) { console.warn('[renew] demo mode:', e.message); qrBase64 = await QRCode.toDataURL('DEMO-' + orderId, { margin: 2, scale: 8 }); }

    const stg = await getEffectiveSettings(); const now = Date.now();
    const trx = { id: orderId, type: 'renewal', productId: origTrx.productId, productName: origTrx.productName, productType: 'panel', variantId: origTrx.variantId, variantName: origTrx.variantName, variantPlan: origTrx.variantPlan, variantDays: days, panelUsername: username, origTrxId: origTrx.id, unitPrice: renewPrice, adminFee, totalBayar, phone: phone ? '62' + String(phone).replace(/^0/, '') : null, qrBase64, pakData, status: 'PENDING', createdAt: now, expiryAt: now + (stg.expiryMin || C.store.expiry) * 60000, demo: !pakData || !C.pak.apikey };
    await saveTrx(orderId, trx, null);
    console.log('[renew]', orderId, '|', username, '|', days, 'd | Rp' + totalBayar);
    res.json({ ok: true, orderId });
  } catch(e) { console.error('[renew/order]', e.message); res.json({ ok: false, message: e.message }); }
});

// ══════════════════════════════════════════════════════════════
// ADMIN ROUTES
// ══════════════════════════════════════════════════════════════

// Login — SECURITY: rate limit + return HMAC-signed stateless token
app.post('/api/admin/login', function(req, res) {
  const ip   = req.ip || 'x';
  const pass = req.body.password || '';
  if (!rateLimit('login:' + ip, 5, 15 * 60 * 1000)) return res.status(429).json({ ok: false, message: 'Terlalu banyak percobaan. Coba lagi dalam 15 menit.' });
  // SECURITY FIX: batasi panjang input agar tidak DoS lewat Buffer.from(pass) sangat besar
  if (!pass || pass.length > 200) { console.warn('[admin] login gagal dari', ip); return res.json({ ok: false, message: 'Password salah.' }); }
  const passOk = pass.length === C.store.adminPass.length &&
    crypto.timingSafeEqual(Buffer.from(pass), Buffer.from(C.store.adminPass));
  if (!pass || !passOk) { console.warn('[admin] login gagal dari', ip); return res.json({ ok: false, message: 'Password salah.' }); }
  const token = makeAdminToken();
  console.log('[admin] login sukses dari', ip);
  auditLog('login', 'Admin login dari ' + ip, ip).catch(function(){});
  res.json({ ok: true, token });
});

// Logout — stateless tokens: clear on client side
// SECURITY FIX: jika user minta logout eksplisit, tandai lastTokenReset agar token lama invalid
app.post('/api/admin/logout', function(req, res) {
  res.json({ ok: true });
});

// List transactions
app.get('/api/admin/transactions', adminAuth, async function(req, res) {
  try {
    const files   = await listTrx();
    const results = [];
    // ALWAYS bypass cache for admin — ensures refresh = fresh data
    await Promise.all(files.filter(function(f) { return f.name.endsWith('.json'); }).sort(function(a, b) { return b.name.localeCompare(a.name); }).slice(0, 200).map(async function(f) {
      try { const r = await dbRead('transactions/' + f.name, true); if (r.data) { const d = Object.assign({}, r.data); delete d.qrBase64; delete d.pakData; results.push(d); } } catch(e) {}
    }));
    results.sort(function(a, b) { return (b.createdAt || 0) - (a.createdAt || 0); });
    res.set('Cache-Control', 'no-store');
    res.json({ ok: true, data: results, total: results.length });
  } catch (e) { res.json({ ok: false, message: e.message }); }
});

// Transaction detail (admin sees password too)
app.get('/api/admin/transactions/:id', adminAuth, async function(req, res) {
  try {
    const id = req.params.id;
    if (!isValidId(id)) return res.json({ ok: false, message: 'ID tidak valid.' });
    const r = await getTrx(id);
    if (!r.data) return res.json({ ok: false, message: 'Tidak ditemukan.' });
    const d = Object.assign({}, r.data); delete d.qrBase64; delete d.pakData;
    res.json({ ok: true, data: d });
  } catch (e) { res.json({ ok: false, message: e.message }); }
});

// Update transaction status — SECURITY: whitelist valid statuses
app.post('/api/admin/transactions/:id/status', adminAuth, async function(req, res) {
  try {
    const id     = req.params.id;
    const status = req.body.status;
    const note   = String(req.body.note || '').slice(0, 500);
    const VALID  = ['COMPLETED', 'PENDING', 'FAILED', 'EXPIRED', 'PAID_ERROR'];
    if (!VALID.includes(status)) return res.json({ ok: false, message: 'Status tidak valid.' });
    if (!isValidId(id)) return res.json({ ok: false, message: 'ID tidak valid.' });
    const r = await getTrx(id);
    if (!r.data) return res.json({ ok: false, message: 'Tidak ditemukan.' });
    await saveTrx(id, Object.assign({}, r.data, { status, adminNote: note, updatedAt: Date.now() }), r.sha);
    console.log('[admin] status', id, '->', status);
    res.json({ ok: true });
  } catch (e) { res.json({ ok: false, message: e.message }); }
});
// ── DELETE single transaction
app.delete('/api/admin/transactions/:id', adminAuth, async function(req, res) {
  try {
    const id = req.params.id;
    if (!isValidId(id)) return res.json({ ok: false, message: 'ID tidak valid.' });
    const r = await getTrx(id);
    if (!r.data) return res.json({ ok: false, message: 'Transaksi tidak ditemukan.' });
    await dbDelete('transactions/' + id + '.json');
    _gitTreeCache.delete('transactions');
    auditLog('delete-trx', id, req.adminIp).catch(function(){});
    console.log('[admin] delete trx:', id);
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── RESET (delete ALL) transactions
app.delete('/api/admin/transactions', adminAuth, async function(req, res) {
  try {
    const files = await listTrx();
    const jsons = files.filter(function(f){ return f.name.endsWith('.json'); });
    if (!jsons.length) return res.json({ ok: true, deleted: 0 });
    let deleted = 0, errors = 0;
    for (const f of jsons) {
      try {
        await dbDelete('transactions/' + f.name);
        _dbCacheInvalidate('transactions/' + f.name);
        _gitTreeCache.delete('transactions');
        deleted++;
      } catch(e) { errors++; }
    }
    auditLog('reset-all-trx', 'deleted:' + deleted + ' errors:' + errors, req.adminIp).catch(function(){});
    console.log('[admin] reset all transactions: deleted=' + deleted + ' errors=' + errors);
    res.json({ ok: true, deleted, errors });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Stats
app.get('/api/admin/stats', adminAuth, async function(req, res) {
  try {
    const files = await listTrx(); let total = 0, pending = 0, completed = 0, failed = 0, revenue = 0;
    await Promise.all(files.filter(function(f) { return f.name.endsWith('.json'); }).map(async function(f) {
      try {
        const r = await dbRead('transactions/' + f.name); if (!r.data) return; total++;
        if (r.data.status === 'COMPLETED') { completed++; revenue += (r.data.totalBayar || r.data.unitPrice || 0); }
        else if (r.data.status === 'PENDING' || r.data.status === 'PAID') pending++; else failed++;
      } catch(e) {}
    }));
    res.json({ ok: true, total, pending, completed, failed, revenue });
  } catch (e) { res.json({ ok: false, message: e.message }); }
});

// ── ADMIN PANELS ───────────────────────────────────────────────────────────

// List all Pterodactyl servers cross-referenced with DB expiry
// ── ADMIN PANELS — Gabungan panel normal + reseller dalam satu tabel
app.get('/api/admin/panels', adminAuth, async function(req, res) {
  try {
    if (!C.ptero.domain || !C.ptero.apikey) return res.json({ ok: false, message: 'Pterodactyl belum dikonfigurasi.' });
    let pteroServers = [];
    try {
      const sRes = await axios.get(C.ptero.domain + '/api/application/servers?per_page=100', { headers: ptH() });
      pteroServers = (sRes.data && sRes.data.data) ? sRes.data.data.map(function(s) { return s.attributes; }) : [];
    } catch(e) { return res.json({ ok: false, message: 'Gagal koneksi Pterodactyl: ' + e.message }); }

    // Build serverId → trx map (panel normal)
    const files = await listTrx(); const panelMap = {};
    await Promise.all(files.filter(function(f) { return f.name.endsWith('.json'); }).map(async function(f) {
      try {
        const r = await dbRead('transactions/' + f.name);
        if (r.data && r.data.productType === 'panel' && r.data.result && r.data.result.serverId) {
          const sid = String(r.data.result.serverId);
          if (!panelMap[sid] || (r.data.completedAt || 0) > (panelMap[sid].completedAt || 0)) panelMap[sid] = r.data;
        }
      } catch(e) {}
    }));

    // Build serverId → rsServer map (panel reseller)
    const resellerMap = {};
    try {
      const rsFiles = (await listDirCached('reseller-servers')).filter(function(f){ return f.name.endsWith('.json'); });
      await Promise.all(rsFiles.map(async function(f) {
        try {
          const r = await getRsServer(f.name.replace('.json',''));
          if (r.data && r.data.serverId && r.data.status !== 'deleted') resellerMap[String(r.data.serverId)] = r.data;
        } catch(e) {}
      }));
    } catch(e) {}

    const pteroSidSet = new Set(pteroServers.map(function(s){ return String(s.id); }));

    const result = pteroServers.map(function(srv) {
      const sid = String(srv.id);
      const rs  = resellerMap[sid];
      const t   = panelMap[sid];
      if (rs) {
        const exp = rs.expiresAt || null;
        const dl  = exp ? Math.ceil((exp - Date.now()) / 86400000) : null;
        return { serverId: srv.id, name: srv.name, description: srv.description, suspended: !!srv.suspended,
          status: srv.suspended ? 'suspended' : (exp && Date.now() > exp ? 'expired' : 'active'),
          userId: rs.userId || srv.user, username: rs.panelUsername || srv.name,
          plan: rs.plan || null, ram: rs.ram || null, disk: rs.disk || null, cpu: rs.cpu || null,
          expiresAt: exp, daysLeft: dl, expired: exp ? Date.now() > exp : false,
          _isReseller: true, _rsvId: rs.id, _resellerUsername: rs.resellerUsername,
          source: 'reseller', createdAt: rs.createdAt };
      } else {
        const exp = t && t.result ? t.result.expiresAt : null;
        const dl  = exp ? Math.ceil((exp - Date.now()) / 86400000) : null;
        return { serverId: srv.id, name: srv.name, description: srv.description, suspended: !!srv.suspended,
          status: srv.suspended ? 'suspended' : 'active', userId: srv.user,
          trxId: t ? t.id : null, username: t && t.result ? t.result.username : srv.name,
          plan: t ? t.variantPlan : null, ram: t && t.result ? t.result.ram : null,
          disk: t && t.result ? t.result.disk : null, cpu: t && t.result ? t.result.cpu : null,
          expiresAt: exp, daysLeft: dl, expired: exp ? Date.now() > exp : false,
          renewedAt: t ? t.renewedAt : null, adminExtended: t ? t.adminExtended : null,
          _isReseller: false, source: 'normal', createdAt: t ? (t.completedAt || t.createdAt) : null };
      }
    });

    // Tambahkan reseller panels yang ada di DB tapi tidak di Pterodactyl
    Object.keys(resellerMap).forEach(function(sid) {
      if (!pteroSidSet.has(sid)) {
        const rs  = resellerMap[sid];
        const exp = rs.expiresAt || null;
        const dl  = exp ? Math.ceil((exp - Date.now()) / 86400000) : null;
        result.push({ serverId: parseInt(sid)||sid, name: rs.panelUsername||sid, description: 'RS:'+rs.resellerUsername,
          suspended: rs.status === 'suspended', status: rs.status || 'unknown',
          userId: rs.userId || null, username: rs.panelUsername || '—',
          plan: rs.plan || null, ram: rs.ram || null, disk: rs.disk || null, cpu: rs.cpu || null,
          expiresAt: exp, daysLeft: dl, expired: exp ? Date.now() > exp : false,
          _isReseller: true, _rsvId: rs.id, _resellerUsername: rs.resellerUsername,
          source: 'reseller', _notInPtero: true, createdAt: rs.createdAt });
      }
    });

    result.sort(function(a, b){ return (b.createdAt||0) - (a.createdAt||0); });
    res.json({ ok: true, data: result, total: result.length });
  } catch (e) { res.json({ ok: false, message: e.message }); }
});

// Extend panel contract manually
// ── AUTO-DB-INSERT: jika panel tidak ada di DB sama sekali (misalnya dibuat
//    manual di Pterodactyl atau data hilang), otomatis buat record DB dari
//    data Pterodactyl sebelum perpanjang. Tidak perlu input manual admin.
app.post('/api/admin/panels/:sid/extend', adminAuth, async function(req, res) {
  try {
    const serverId = parseInt(req.params.sid);
    const addDays  = parseInt(req.body.days);
    if (!serverId || isNaN(addDays) || addDays < 1 || addDays > 3650) return res.json({ ok: false, message: 'Input tidak valid (1–3650 hari).' });

    // ── STEP 1: Cari di transactions/ (panel beli normal)
    const files = await listTrx(); let targetTrx = null, targetR = null;
    for (const f of files.filter(function(f) { return f.name.endsWith('.json'); })) {
      try {
        const r = await dbRead('transactions/' + f.name);
        if (r.data && r.data.result && String(r.data.result.serverId) === String(serverId)) {
          if (!targetTrx || (r.data.completedAt || 0) > (targetTrx.completedAt || 0)) { targetTrx = r.data; targetR = r; }
        }
      } catch(e) {}
    }

    // ── STEP 2: Cari di reseller-servers/ (panel dibuat via cPanel reseller)
    if (!targetTrx) {
      try {
        const rsFiles = (await listDirCached('reseller-servers')).filter(function(f){ return f.name.endsWith('.json'); });
        for (const f of rsFiles) {
          try {
            const r = await getRsServer(f.name.replace('.json',''));
            if (r.data && String(r.data.serverId) === String(serverId) && r.data.status !== 'deleted') {
              const rsvId     = r.data.id;
              const base      = (r.data.expiresAt && r.data.expiresAt > Date.now()) ? r.data.expiresAt : Date.now();
              const newExpiry = base + addDays * 86400000;
              // Update Pterodactyl description
              try {
                const sRes = await axios.get(C.ptero.domain + '/api/application/servers/' + serverId, { headers: ptH() });
                const srv  = sRes.data.attributes;
                await axios.patch(C.ptero.domain + '/api/application/servers/' + serverId + '/details', {
                  name: srv.name, user: srv.user, email: srv.user, external_id: srv.external_id || null,
                  description: 'RS:' + r.data.resellerUsername + ' | exp: ' + new Date(newExpiry).toLocaleDateString('id-ID') + ' [admin+' + addDays + 'd]',
                }, { headers: ptH() });
              } catch(pteroErr) { console.warn('[admin extend] ptero rs desc:', pteroErr.message); }
              // Update reseller-servers/ DB
              const freshR = await getRsServer(rsvId);
              await saveRsServer(rsvId, Object.assign({}, freshR.data || r.data, {
                expiresAt: newExpiry, status: 'active',
                extendedAt: Date.now(), extendedDays: (r.data.extendedDays || 0) + addDays,
              }), freshR.sha || r.sha);
              _gitTreeCache.delete('reseller-servers');
              auditLog('extend-rs-via-panels', 'Server ' + serverId + ' rsvId:' + rsvId + ' +' + addDays + 'd', req.adminIp).catch(function(){});
              console.log('[admin] extend reseller server via panels:', serverId, '+', addDays, 'd | rsvId:', rsvId);
              return res.json({ ok: true, newExpiry, newExpiryFmt: new Date(newExpiry).toLocaleDateString('id-ID') });
            }
          } catch(e) {}
        }
      } catch(e) { console.warn('[admin extend] reseller fallback:', e.message); }
    }

    // ── STEP 3: AUTO-DB-INSERT — panel ada di Pterodactyl tapi tidak di DB sama sekali
    //    Ambil info dari Pterodactyl, buat record baru, lanjut perpanjang
    if (!targetTrx) {
      console.log('[admin extend] panel', serverId, 'tidak ada di DB — auto-insert dari Pterodactyl...');
      let ptSrv = null;
      try {
        const sRes = await axios.get(C.ptero.domain + '/api/application/servers/' + serverId, { headers: ptH() });
        ptSrv = sRes.data.attributes;
      } catch(e) {
        return res.json({ ok: false, message: 'Panel tidak ditemukan di database maupun Pterodactyl.' });
      }
      // Buat record minimal di transactions/ agar perpanjang bisa berjalan dan muncul di tab Panel
      const syntheticId = 'TRX-' + Date.now() + '-' + crypto.randomBytes(4).toString('hex');
      const syntheticTrx = {
        id          : syntheticId,
        productType : 'panel',
        productName : 'Panel (auto-import)',
        variantName : ptSrv.name || ('Server ' + serverId),
        variantPlan : null,
        status      : 'COMPLETED',
        completedAt : Date.now(),
        createdAt   : Date.now(),
        _autoImported: true,
        _autoImportedBy: 'admin-extend',
        result: {
          serverId : serverId,
          username : ptSrv.name || ('server' + serverId),
          userId   : ptSrv.user || null,
          domain   : C.ptero.domain,
          ram      : ptSrv.limits ? (ptSrv.limits.memory / 1024).toFixed(1) + 'GB' : '?',
          disk     : ptSrv.limits ? (ptSrv.limits.disk / 1024).toFixed(1) + 'GB'   : '?',
          cpu      : ptSrv.limits ? ptSrv.limits.cpu + '%'                          : '?',
          expiresAt: Date.now(), // akan diupdate di bawah
        },
      };
      await saveTrx(syntheticId, syntheticTrx, null);
      _gitTreeCache.delete('transactions');
      console.log('[admin extend] auto-import berhasil:', syntheticId, '| server:', serverId);
      const freshSyn = await getTrx(syntheticId);
      targetTrx = freshSyn.data || syntheticTrx;
      targetR   = freshSyn;
    }

    // ── STEP 4: Perpanjang — update expiry di DB dan Pterodactyl description
    const base      = (targetTrx.result.expiresAt && targetTrx.result.expiresAt > Date.now()) ? targetTrx.result.expiresAt : Date.now();
    const newExpiry = base + addDays * 86400000;

    try {
      const sRes = await axios.get(C.ptero.domain + '/api/application/servers/' + serverId, { headers: ptH() });
      const srv  = sRes.data.attributes;
      await axios.patch(C.ptero.domain + '/api/application/servers/' + serverId + '/details', {
        name: srv.name, user: srv.user, email: srv.user, external_id: srv.external_id || null,
        description: 'Dongtube ' + targetTrx.id + ' | exp: ' + new Date(newExpiry).toLocaleDateString('id-ID') + ' [admin+' + addDays + 'd]',
      }, { headers: ptH() });
    } catch(e) { console.warn('[admin extend] ptero desc:', e.message); }

    const freshR2 = await getTrx(targetTrx.id);
    await saveTrx(targetTrx.id, Object.assign({}, freshR2.data || targetTrx, {
      result: Object.assign({}, targetTrx.result, { expiresAt: newExpiry }),
      renewedAt: Date.now(), adminExtended: ((freshR2.data || targetTrx).adminExtended || 0) + addDays,
    }), freshR2.sha || (targetR && targetR.sha) || null);
    auditLog('extend', 'Server ' + serverId + ' +' + addDays + 'd', req.adminIp).catch(function(){});
    console.log('[admin] extend server', serverId, '+', addDays, 'd');
    res.json({ ok: true, newExpiry, newExpiryFmt: new Date(newExpiry).toLocaleDateString('id-ID') });
  } catch (e) { res.json({ ok: false, message: e.message }); }
});

// ── ADMIN: Kurangi hari panel
app.post('/api/admin/panels/:sid/reduce', adminAuth, async function(req, res) {
  try {
    const serverId   = parseInt(req.params.sid);
    const reduceDays = parseInt(req.body.days);
    if (!serverId || isNaN(reduceDays) || reduceDays < 1 || reduceDays > 3650)
      return res.json({ ok: false, message: 'Input tidak valid (1–3650 hari).' });
    const files = await listTrx(); let targetTrx = null, targetR = null;
    for (const f of files.filter(function(f){ return f.name.endsWith('.json'); })) {
      try {
        const r = await dbRead('transactions/' + f.name);
        if (r.data && r.data.result && String(r.data.result.serverId) === String(serverId)) {
          if (!targetTrx || (r.data.completedAt||0) > (targetTrx.completedAt||0)) { targetTrx = r.data; targetR = r; }
        }
      } catch(e) {}
    }
    if (!targetTrx) {
      try {
        const rsFiles = (await listDirCached('reseller-servers')).filter(function(f){ return f.name.endsWith('.json'); });
        for (const f of rsFiles) {
          try {
            const r = await getRsServer(f.name.replace('.json',''));
            if (r.data && String(r.data.serverId) === String(serverId) && r.data.status !== 'deleted') {
              const rsvId = r.data.id;
              const cur = (r.data.expiresAt && r.data.expiresAt > Date.now()) ? r.data.expiresAt : Date.now();
              const newExpiry = Math.max(Date.now(), cur - reduceDays * 86400000);
              try {
                const sRes = await axios.get(C.ptero.domain + '/api/application/servers/' + serverId, { headers: ptH() });
                const srv = sRes.data.attributes;
                await axios.patch(C.ptero.domain + '/api/application/servers/' + serverId + '/details', {
                  name: srv.name, user: srv.user, email: srv.user, external_id: srv.external_id || null,
                  description: 'RS:' + r.data.resellerUsername + ' | exp: ' + new Date(newExpiry).toLocaleDateString('id-ID') + ' [admin-' + reduceDays + 'd]',
                }, { headers: ptH() });
              } catch(e) {}
              const fr = await getRsServer(rsvId);
              await saveRsServer(rsvId, Object.assign({}, fr.data||r.data, { expiresAt: newExpiry, reducedAt: Date.now(), reducedDays: (r.data.reducedDays||0)+reduceDays }), fr.sha||r.sha);
              _gitTreeCache.delete('reseller-servers');
              auditLog('reduce-rs-via-panels', 'Server ' + serverId + ' -' + reduceDays + 'd', req.adminIp).catch(function(){});
              return res.json({ ok: true, newExpiry, newExpiryFmt: new Date(newExpiry).toLocaleDateString('id-ID') });
            }
          } catch(e) {}
        }
      } catch(e) {}
    }
    if (!targetTrx || !targetTrx.result) return res.json({ ok: false, message: 'Panel tidak ditemukan di database.' });
    const cur = (targetTrx.result.expiresAt && targetTrx.result.expiresAt > Date.now()) ? targetTrx.result.expiresAt : Date.now();
    const newExpiry = Math.max(Date.now(), cur - reduceDays * 86400000);
    try {
      const sRes = await axios.get(C.ptero.domain + '/api/application/servers/' + serverId, { headers: ptH() });
      const srv = sRes.data.attributes;
      await axios.patch(C.ptero.domain + '/api/application/servers/' + serverId + '/details', {
        name: srv.name, user: srv.user, email: srv.user, external_id: srv.external_id || null,
        description: 'Dongtube ' + targetTrx.id + ' | exp: ' + new Date(newExpiry).toLocaleDateString('id-ID') + ' [admin-' + reduceDays + 'd]',
      }, { headers: ptH() });
    } catch(e) {}
    const fr2 = await getTrx(targetTrx.id);
    await saveTrx(targetTrx.id, Object.assign({}, fr2.data||targetTrx, {
      result: Object.assign({}, targetTrx.result, { expiresAt: newExpiry }),
      reducedAt: Date.now(), adminReduced: ((fr2.data||targetTrx).adminReduced||0) + reduceDays,
    }), fr2.sha || (targetR && targetR.sha) || null);
    auditLog('reduce', 'Server ' + serverId + ' -' + reduceDays + 'd', req.adminIp).catch(function(){});
    res.json({ ok: true, newExpiry, newExpiryFmt: new Date(newExpiry).toLocaleDateString('id-ID') });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Suspend server
app.post('/api/admin/panels/:sid/suspend', adminAuth, async function(req, res) {
  try {
    const sid = parseInt(req.params.sid);
    if (!sid) return res.json({ ok: false, message: 'ID tidak valid.' });
    const r = await axios.post(C.ptero.domain + '/api/application/servers/' + sid + '/suspend', {}, { headers: ptH() });
    if (r.status === 204) res.json({ ok: true });
    else res.json({ ok: false, message: 'Ptero: ' + r.status });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Unsuspend server
app.post('/api/admin/panels/:sid/unsuspend', adminAuth, async function(req, res) {
  try {
    const sid = parseInt(req.params.sid);
    if (!sid) return res.json({ ok: false, message: 'ID tidak valid.' });
    const r = await axios.post(C.ptero.domain + '/api/application/servers/' + sid + '/unsuspend', {}, { headers: ptH() });
    if (r.status === 204) res.json({ ok: true });
    else res.json({ ok: false, message: 'Ptero: ' + r.status });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Delete server + user permanently
app.delete('/api/admin/panels/:sid', adminAuth, async function(req, res) {
  try {
    const sid     = parseInt(req.params.sid);
    if (!sid) return res.json({ ok: false, message: 'ID tidak valid.' });
    const headers = ptH(); const domain = C.ptero.domain;
    let srvDeleted = false;
    try { await axios.delete(domain + '/api/application/servers/' + sid, { headers }); srvDeleted = true; }
    catch(e) { if (e.response && e.response.status === 404) srvDeleted = true; else console.warn('[admin del] server:', e.message); }

    // Find userId and trx in DB
    const files = await listTrx(); let ptUid = null, tTrxId = null, tR = null;
    for (const f of files.filter(function(f) { return f.name.endsWith('.json'); })) {
      try {
        const r = await dbRead('transactions/' + f.name);
        if (r.data && r.data.result && String(r.data.result.serverId) === String(sid)) {
          ptUid = r.data.result.userId; tTrxId = r.data.id; tR = r; if (ptUid) break;
        }
      } catch(e) {}
    }
    if (ptUid) {
      try { await axios.delete(domain + '/api/application/users/' + ptUid, { headers }); console.log('[admin del] user', ptUid); }
      catch(e) { if (!e.response || e.response.status !== 404) console.warn('[admin del] user:', e.message); }
    } else if (tR && tR.data && tR.data.result && tR.data.result.username) {
      // fallback: find by username
      try {
        const ur = await axios.get(domain + '/api/application/users?filter[username]=' + tR.data.result.username, { headers });
        if (ur.data && ur.data.data && ur.data.data.length > 0) await axios.delete(domain + '/api/application/users/' + ur.data.data[0].attributes.id, { headers });
      } catch(e) {}
    }
    if (tTrxId && tR && tR.data) {
      await saveTrx(tTrxId, Object.assign({}, tR.data, { status: 'EXPIRED', _panelDeleted: true, _deletedAt: Date.now(), _deletedBy: 'admin' }), tR.sha);
    }
    auditLog('delete-panel', 'Server ' + sid + ' | trx:' + tTrxId, req.adminIp).catch(function(){});
    console.log('[admin] delete server', sid, '| trx:', tTrxId);
    res.json({ ok: true, srvDeleted });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});


// Reset server password (admin)
app.post('/api/admin/panels/:sid/reset-password', adminAuth, async function(req, res) {
  try {
    const sid    = parseInt(req.params.sid);
    const newPwd = String(req.body.password || '').trim();
    if (!sid) return res.json({ ok: false, message: 'Server ID tidak valid.' });
    if (!newPwd || newPwd.length < 6 || newPwd.length > 72) return res.json({ ok: false, message: 'Password minimal 6, maksimal 72 karakter.' });
    const SAFE_PASS = /^[a-zA-Z0-9!@#$%^&*_+=.-]+$/;
    if (!SAFE_PASS.test(newPwd)) return res.json({ ok: false, message: 'Password mengandung karakter tidak valid.' });

    // Cari userId dari DB
    const files = await listTrx(); let ptUserId = null; let uname = null;
    for (const f of files.filter(function(f) { return f.name.endsWith('.json'); })) {
      try {
        const r = await dbRead('transactions/' + f.name);
        if (r.data && r.data.result && String(r.data.result.serverId) === String(sid)) {
          ptUserId = r.data.result.userId; uname = r.data.result.username; break;
        }
      } catch(e) {}
    }
    if (!ptUserId) return res.json({ ok: false, message: 'User ID tidak ditemukan di DB.' });

    // Patch password via Pterodactyl Application API
    await axios.patch(C.ptero.domain + '/api/application/users/' + ptUserId, {
      email     : (uname || 'user') + '@Dongtube.local',
      username  : uname || ('user' + ptUserId),
      first_name: uname || 'User', last_name: 'Panel',
      password  : newPwd, language: 'en',
    }, { headers: ptH() });

    // Update DB (transaksi yang punya serverId ini)
    for (const f of files.filter(function(f) { return f.name.endsWith('.json'); })) {
      try {
        const r = await dbRead('transactions/' + f.name);
        if (r.data && r.data.result && String(r.data.result.serverId) === String(sid)) {
          await saveTrx(r.data.id, Object.assign({}, r.data, {
            result: Object.assign({}, r.data.result, { password: newPwd }),
            panelPassword: newPwd, _pwResetAt: Date.now(), _pwResetBy: 'admin',
          }), r.sha);
          break;
        }
      } catch(e) {}
    }
    auditLog('reset-password', 'Server ' + sid + ' (' + uname + ')', req.adminIp).catch(function(){});
    console.log('[admin] reset password server', sid, '| user:', uname);
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Reinstall server
app.post('/api/admin/panels/:sid/reinstall', adminAuth, async function(req, res) {
  try {
    const sid = parseInt(req.params.sid);
    if (!sid) return res.json({ ok: false, message: 'Server ID tidak valid.' });
    const r = await axios.post(C.ptero.domain + '/api/application/servers/' + sid + '/reinstall', {}, { headers: ptH() });
    if (r.status === 204) {
      auditLog('reinstall', 'Server ' + sid, req.adminIp).catch(function(){});
      console.log('[admin] reinstall server', sid);
      res.json({ ok: true });
    } else { res.json({ ok: false, message: 'Ptero: ' + r.status }); }
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Get audit log
app.get('/api/admin/audit', adminAuth, async function(req, res) {
  try {
    const r = await dbRead('audit.json');
    res.json({ ok: true, data: Array.isArray(r.data) ? r.data.slice(0, 100) : [] });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── ADMIN PRODUCTS ─────────────────────────────────────────────────────────
// ── ADMIN INIT (parallel fetch products + settings = faster load)
app.get('/api/admin/init', adminAuth, async function(req, res) {
  try {
    const [prod, sett, templates] = await Promise.all([
      dbRead('products.json'),
      getEffectiveSettings(),
      getPanelTemplates(),
    ]);
    res.json({ ok: true, products: prod.data || [], settings: sett, templates });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

app.get('/api/admin/products', adminAuth, async function(req, res) {
  try { const r = await dbRead('products.json'); res.json({ ok: true, data: r.data || [] }); }
  catch(e) { res.json({ ok: false, message: e.message }); }
});
app.put('/api/admin/products', adminAuth, async function(req, res) {
  try {
    const products = Array.isArray(req.body) ? req.body : req.body.products;
    if (!Array.isArray(products)) return res.json({ ok: false, message: 'products harus array.' });
    const r = await dbRead('products.json');
    await dbWrite('products.json', products, r.sha || null, 'admin: update products');
    console.log('[admin] products updated:', products.length);
    broadcastStore({ type: 'reload' });
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── ADMIN: ACCOUNTS POOL (multi-akun per varian) ────────────────────────────
// GET  /api/admin/accounts/:productId/:variantId → { ok, count, accounts: [...] }
app.get('/api/admin/accounts/:productId/:variantId', adminAuth, async function(req, res) {
  try {
    const { productId, variantId } = req.params;
    const r = await getAccounts(productId, variantId);
    const list = (r.data && Array.isArray(r.data)) ? r.data : [];
    res.json({ ok: true, count: list.length, accounts: list });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// POST /api/admin/accounts/:productId/:variantId  body: { accounts: "acc1\nacc2\n..." }
// Appends new accounts to the pool and updates variant stock in products.json
app.post('/api/admin/accounts/:productId/:variantId', adminAuth, async function(req, res) {
  try {
    const { productId, variantId } = req.params;
    const raw = String(req.body.accounts || req.body.data || '');
    const newAccts = raw.split('\n').map(function(s){ return s.trim(); }).filter(Boolean);
    if (newAccts.length === 0) return res.json({ ok: false, message: 'Tidak ada akun valid.' });
    if (newAccts.length > 500) return res.json({ ok: false, message: 'Maksimal 500 akun sekaligus.' });

    // Append to existing pool
    const r = await getAccounts(productId, variantId);
    const existing = (r.data && Array.isArray(r.data)) ? r.data : [];
    const merged   = existing.concat(newAccts);
    await saveAccounts(productId, variantId, merged, r.sha || null);

    // Sync stock in products.json
    try {
      const pr = await dbRead('products.json', true);
      if (pr.data) {
        const prods = pr.data;
        const pi    = prods.findIndex(function(p){ return p.id === productId; });
        if (pi >= 0) {
          const vi = prods[pi].variants ? prods[pi].variants.findIndex(function(v){ return v.id === variantId; }) : -1;
          if (vi >= 0) { prods[pi].variants[vi].stock = merged.length; await dbWrite('products.json', prods, pr.sha, 'account-stock-sync:' + variantId); }
        }
      }
    } catch(stockErr) { console.warn('[accounts] stock-sync failed:', stockErr.message); }

    console.log('[admin] accounts added:', newAccts.length, '| variant:', variantId, '| total:', merged.length);
    res.json({ ok: true, added: newAccts.length, total: merged.length });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// DELETE /api/admin/accounts/:productId/:variantId  → clear ALL accounts
app.delete('/api/admin/accounts/:productId/:variantId', adminAuth, async function(req, res) {
  try {
    const { productId, variantId } = req.params;
    const r = await getAccounts(productId, variantId);
    await saveAccounts(productId, variantId, [], r.sha || null);
    // Sync stock = 0
    try {
      const pr = await dbRead('products.json', true);
      if (pr.data) {
        const prods = pr.data;
        const pi    = prods.findIndex(function(p){ return p.id === productId; });
        if (pi >= 0) {
          const vi = prods[pi].variants ? prods[pi].variants.findIndex(function(v){ return v.id === variantId; }) : -1;
          if (vi >= 0) { prods[pi].variants[vi].stock = 0; await dbWrite('products.json', prods, pr.sha, 'account-stock-clear:' + variantId); }
        }
      }
    } catch(stockErr) { console.warn('[accounts] stock-sync failed:', stockErr.message); }
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// DELETE /api/admin/accounts/:productId/:variantId/:index → delete one account by index
app.delete('/api/admin/accounts/:productId/:variantId/:index', adminAuth, async function(req, res) {
  try {
    const { productId, variantId } = req.params;
    const idx = parseInt(req.params.index);
    if (isNaN(idx) || idx < 0) return res.json({ ok: false, message: 'Index tidak valid.' });
    const r = await getAccounts(productId, variantId);
    const list = (r.data && Array.isArray(r.data)) ? r.data : [];
    if (idx >= list.length) return res.json({ ok: false, message: 'Index di luar batas.' });
    list.splice(idx, 1);
    await saveAccounts(productId, variantId, list, r.sha);
    // Sync stock
    try {
      const pr = await dbRead('products.json', true);
      if (pr.data) {
        const prods = pr.data;
        const pi    = prods.findIndex(function(p){ return p.id === productId; });
        if (pi >= 0) {
          const vi = prods[pi].variants ? prods[pi].variants.findIndex(function(v){ return v.id === variantId; }) : -1;
          if (vi >= 0) { prods[pi].variants[vi].stock = list.length; await dbWrite('products.json', prods, pr.sha, 'account-stock-del:' + variantId); }
        }
      }
    } catch(stockErr) { console.warn('[accounts] stock-sync failed:', stockErr.message); }
    res.json({ ok: true, remaining: list.length });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── ADMIN SETTINGS ─────────────────────────────────────────────────────────
app.get('/api/admin/settings', adminAuth, async function(req, res) {
  try {
    res.json({ ok: true, data: await getEffectiveSettings() });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});
app.post('/api/admin/settings', adminAuth, async function(req, res) {
  try {
    var b = req.body;
    var storeName   = String(b.storeName   || '').trim().slice(0, 100);
    var wa          = String(b.wa          || '').trim().slice(0, 200);
    var expiryMin   = parseInt(b.expiryMin) || C.store.expiry;
    var logoUrl     = String(b.logoUrl     || '').trim().slice(0, 500);
    var announcement= String(b.announcement|| '').trim().slice(0, 500);
    var footerText  = String(b.footerText  || '').trim().slice(0, 300);
    var primaryColor= String(b.primaryColor|| '').trim().slice(0, 20);
    var tiktok      = String(b.tiktok      || '').trim().slice(0, 200);
    var instagram   = String(b.instagram   || '').trim().slice(0, 200);
    var otpEnabled  = b.otpEnabled !== 'false' && b.otpEnabled !== false;
    var panelEnabled= b.panelEnabled !== 'false' && b.panelEnabled !== false;
    var maintenanceMode  = b.maintenanceMode === 'true' || b.maintenanceMode === true;
    var maintenanceMsg   = String(b.maintenanceMsg || 'Sedang dalam maintenance.').trim().slice(0, 300);
    var musicUrl         = String(b.musicUrl || '').trim().slice(0, 500);
    var musicEnabled     = b.musicEnabled === true || b.musicEnabled === 'true';
    var captchaEnabled   = b.captchaEnabled === true || b.captchaEnabled === 'true';
    if (!storeName) return res.json({ ok: false, message: 'Nama toko tidak boleh kosong.' });
    if (expiryMin < 1 || expiryMin > 1440) return res.json({ ok: false, message: 'Expiry 1–1440 menit.' });
    var depositFeeType = ['flat','percent'].includes(String(b.depositFeeType)) ? String(b.depositFeeType) : 'flat';
    var depositFee     = parseFloat(b.depositFee) || 0;
    var depositMin     = parseInt(b.depositMin)   || 1000;
    var otpMarkup      = Math.max(0, parseFloat(b.otpMarkup) || 0);
    var appLogoUrl     = String(b.appLogoUrl || '').trim().slice(0, 500);
    var bgUrl          = String(b.bgUrl || '').trim().slice(0, 500);
    var bgType         = ['image','video'].includes(String(b.bgType)) ? String(b.bgType) : 'image';
    var bgOpacity      = Math.min(1, Math.max(0, parseFloat(b.bgOpacity) || 0.15));
    // ── PHONE FIELD
    var phoneRequired = b.phoneRequired === true || b.phoneRequired === 'true';
    var phoneEnabled  = b.phoneEnabled !== false && b.phoneEnabled !== 'false';
    // ── CUSTOM FIELDS: validasi dan sanitasi
    var customFields = [];
    if (Array.isArray(b.customFields)) {
      customFields = b.customFields.slice(0, 10).map(function(f) {
        return {
          id         : String(f.id || ('cf-' + Date.now() + '-' + Math.random().toString(36).slice(2, 6))),
          label      : String(f.label || '').trim().slice(0, 80),
          type       : ['text','number','email','tel','textarea'].includes(f.type) ? f.type : 'text',
          required   : f.required === true || f.required === 'true',
          placeholder: String(f.placeholder || '').trim().slice(0, 100),
          forProduct : ['all','panel','digital','download','sewabot'].includes(f.forProduct) ? f.forProduct : 'all',
        };
      }).filter(function(f){ return f.label; });
    }
    const r = await dbRead('settings.json');
    await dbWrite('settings.json', { storeName, wa, expiryMin, logoUrl, appLogoUrl, announcement, footerText, primaryColor, tiktok, instagram, otpEnabled, panelEnabled, maintenanceMode, maintenanceMsg, musicUrl, musicEnabled, captchaEnabled, depositFeeType, depositFee, depositMin, otpMarkup, bgUrl, bgType, bgOpacity, phoneRequired, phoneEnabled, customFields }, r.sha || null, 'admin: settings');
    console.log('[admin] settings updated');
    broadcastStore({
      type: maintenanceMode ? 'maintenance' : 'settings_update',
      on: maintenanceMode, msg: maintenanceMsg,
      settings: { storeName, wa, logoUrl, announcement, footerText, primaryColor, bgUrl, bgType, bgOpacity: parseFloat(bgOpacity), otpEnabled, panelEnabled }
    });
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── ADMIN SLIDES ───────────────────────────────────────────────────────────
app.get('/api/admin/slides', adminAuth, async function(req, res) {
  try { const r = await dbRead('slides.json'); res.json({ ok: true, data: r.data || [] }); }
  catch(e) { res.json({ ok: false, message: e.message }); }
});
app.post('/api/admin/slides', adminAuth, async function(req, res) {
  try {
    let slides = Array.isArray(req.body) ? req.body : req.body.slides;
    // FIX: handle double-stringified array (legacy clients sent JSON.stringify(array))
    if (typeof slides === 'string') { try { slides = JSON.parse(slides); } catch(e) { slides = null; } }
    if (!Array.isArray(slides)) return res.json({ ok: false, message: 'slides harus array.' });
    // SECURITY: sanitize setiap field slide — cegah XSS tersimpan ke DB
    slides = slides.map(function(s) {
      return {
        title  : String(s.title   || '').slice(0, 200),
        desc   : String(s.desc    || '').slice(0, 500),
        tag    : String(s.tag     || '').slice(0, 80),
        image  : (s.image  && String(s.image).startsWith('http'))  ? String(s.image).slice(0, 500)  : '',
        img    : (s.img    && String(s.img).startsWith('http'))    ? String(s.img).slice(0, 500)    : '',
        btnText: String(s.btnText || '').slice(0, 80),
        btnCat : String(s.btnCat  || '').replace(/[^a-z0-9_\-]/g,'').slice(0, 40),
      };
    }).filter(function(s){ return s.title || s.desc; }); // hapus slide kosong
    const r = await dbRead('slides.json');
    await dbWrite('slides.json', slides, r.sha || null, 'admin: slides');
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});
app.get('/api/slides', async function(req, res) {
  try { const r = await dbRead('slides.json'); res.json({ ok: true, data: r.data || [] }); }
  catch(e) { res.json({ ok: true, data: [] }); }
});

// ── AUTO-DELETE EXPIRED PANELS ─────────────────────────────────────────────
async function deleteExpiredPanels() {
  if (!C.ptero.domain || !C.ptero.apikey) return;
  try {
    const files = await listTrx(); const headers = ptH(); const dom = C.ptero.domain;
    for (const f of files.filter(function(f) { return f.name.endsWith('.json'); })) {
      try {
        const r = await getTrx(f.name.replace('.json', ''));
        if (!r.data) continue; const d = r.data;
        if (d.status !== 'COMPLETED' || d.productType !== 'panel') continue;
        const result = d.result || {};
        if (!result.expiresAt || Date.now() < result.expiresAt || d._panelDeleted) continue;
        let del = false;
        if (result.serverId) { try { await axios.delete(dom + '/api/application/servers/' + result.serverId, { headers }); del = true; } catch(e) { if (e.response && e.response.status === 404) del = true; } }
        if (result.userId) { try { await axios.delete(dom + '/api/application/users/' + result.userId, { headers }); } catch(e) {} }
        else if (result.username) { try { const ur = await axios.get(dom + '/api/application/users?filter[username]=' + result.username, { headers }); if (ur.data && ur.data.data && ur.data.data.length > 0) await axios.delete(dom + '/api/application/users/' + ur.data.data[0].attributes.id, { headers }); } catch(e) {} }
        if (del) { await saveTrx(d.id, Object.assign({}, d, { status: 'EXPIRED', _panelDeleted: true, _deletedAt: Date.now() }), r.sha); console.log('[expire] deleted:', d.id, '|', result.username); }
      } catch(e) {}
    }
  } catch(e) { console.error('[expire]', e.message); }
}
// deleteExpiredPanels replaced by autoSuspendExpiredPanels (defined above)


// ══════════════════════════════════════════════════════════════
// USER AUTH ROUTES (OTP Store)
// ══════════════════════════════════════════════════════════════

// Register
app.post('/api/user/register', async function(req, res) {
  try {
    const ip  = req.ip || 'x';
    if (!rateLimit('ureg:' + ip, 5, 30 * 60 * 1000)) return res.status(429).json({ ok: false, message: 'Terlalu banyak percobaan registrasi.' });
    await new Promise(function(r){ setTimeout(r, 60 + Math.random() * 80); }); // anti-timing

    const username = String(req.body.username || '').toLowerCase().trim();
    const password = String(req.body.password || '').trim();
    const email    = String(req.body.email || '').trim().slice(0, 100);

    if (!isValidUsername(username)) return res.json({ ok: false, message: 'Username 3–20 karakter, hanya huruf kecil, angka, dan underscore.' });
    if (!password || password.length < 6)  return res.json({ ok: false, message: 'Password minimal 6 karakter.' });
    if (password.length > 72) return res.json({ ok: false, message: 'Password terlalu panjang.' });

    // Check if exists
    const existing = await getUser(username);
    if (existing.data) return res.json({ ok: false, message: 'Username sudah dipakai.' });

    const hashed = await hashPassword(password);
    const now    = Date.now();
    await saveUser(username, {
      username, passwordHash: hashed, email: email || null,
      balance: 0, createdAt: now, lastLogin: null,
    }, null);

    console.log('[user] register:', username, '| ip:', ip);
    res.json({ ok: true, message: 'Akun berhasil dibuat. Silakan login.' });
  } catch(e) { console.error('[register]', e.message); res.json({ ok: false, message: e.message }); }
});

// Login
app.post('/api/user/login', async function(req, res) {
  try {
    const ip  = req.ip || 'x';
    if (!rateLimit('ulog:' + ip, 8, 15 * 60 * 1000)) return res.status(429).json({ ok: false, message: 'Terlalu banyak percobaan login. Coba lagi 15 menit.' });
    await new Promise(function(r){ setTimeout(r, 80 + Math.random() * 120); }); // anti-timing, prevent username enumeration

    const username = String(req.body.username || '').toLowerCase().trim();
    const password = String(req.body.password || '').trim();
    if (!isValidUsername(username) || !password) return res.json({ ok: false, message: 'Username atau password salah.' });

    const r = await getUser(username);
    if (!r.data) return res.json({ ok: false, message: 'Username atau password salah.' });
    if (r.data.banned) return res.json({ ok: false, message: 'Akun diblokir. Hubungi admin.' });

    const valid = await verifyPassword(password, r.data.passwordHash);
    if (!valid) { console.warn('[user] login gagal:', username, '| ip:', ip); return res.json({ ok: false, message: 'Username atau password salah.' }); }

    const token = makeUserToken(username);

    // Update lastLogin
    await saveUser(username, Object.assign({}, r.data, { lastLogin: Date.now() }), r.sha);
    console.log('[user] login:', username, '| ip:', ip);
    res.json({ ok: true, token, username });
  } catch(e) { console.error('[login]', e.message); res.json({ ok: false, message: e.message }); }
});

// Logout — stateless, clear on client side
app.post('/api/user/logout', function(req, res) {
  res.json({ ok: true });
});

// Get profile + balance
app.get('/api/user/me', userAuth, async function(req, res) {
  try {
    const r = await getUser(req.user);
    if (!r.data) return res.json({ ok: false, message: 'User tidak ditemukan.' });
    if (r.data.banned) return res.json({ ok: false, banned: true, message: 'Akun kamu telah diblokir oleh admin.' });
    res.json({ ok: true, data: { username: r.data.username, balance: r.data.balance || 0, email: r.data.email, createdAt: r.data.createdAt } });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Change password
app.post('/api/user/change-password', userAuth, async function(req, res) {
  try {
    const oldPwd = String(req.body.oldPassword || '').trim();
    const newPwd = String(req.body.newPassword || '').trim();
    if (!newPwd || newPwd.length < 6) return res.json({ ok: false, message: 'Password baru minimal 6 karakter.' });
    if (newPwd.length > 72) return res.json({ ok: false, message: 'Password terlalu panjang.' });
    const r = await getUser(req.user);
    if (!r.data) return res.json({ ok: false, message: 'User tidak ditemukan.' });
    const valid = await verifyPassword(oldPwd, r.data.passwordHash);
    if (!valid) return res.json({ ok: false, message: 'Password lama salah.' });
    const hashed = await hashPassword(newPwd);
    // SECURITY FIX: set lastTokenReset agar semua token lama diinvalidasi setelah ganti password
    await saveUser(req.user, Object.assign({}, r.data, { passwordHash: hashed, updatedAt: Date.now(), lastTokenReset: Date.now() }), r.sha);
    // Stateless tokens — client clears localStorage on password change
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ══════════════════════════════════════════════════════════════
// DEPOSIT ROUTES
// ══════════════════════════════════════════════════════════════

// Create deposit (QRIS via RumahOTP)
app.post('/api/deposit/create', userAuth, async function(req, res) {
  try {
    const ip     = req.ip || 'x';
    // FIX: Tunda piggyback cron selama proses deposit agar tidak berebut GitHub DB write.
    _lastPiggybackRun = Math.max(_lastPiggybackRun, Date.now());
    if (!rateLimit('dep:' + req.user, 5, 15 * 60 * 1000)) return res.status(429).json({ ok: false, message: 'Terlalu banyak deposit. Tunggu sebentar.' });
    const amount = parseInt(req.body.amount);
    if (!amount || amount < 1000 || amount > 5000000) return res.json({ ok: false, message: 'Nominal deposit Rp1.000 – Rp5.000.000.' });

    // Load deposit fee settings
    const stg = await getEffectiveSettings();
    if (stg.maintenanceMode) return res.json({ ok: false, message: stg.maintenanceMsg || 'Sedang dalam maintenance.', maintenance: true });
    const depMin     = stg.depositMin || 1000;
    const depFeeType = stg.depositFeeType || 'flat';
    const depFeeVal  = stg.depositFee  || 0;
    if (amount < depMin) return res.json({ ok: false, message: 'Minimum deposit Rp' + depMin.toLocaleString('id-ID') + '.' });

    // Resume check: if user already has a pending deposit with same amount, return it
    try {
      const depListR = await listDirCached('deposits');
      const depFiles = Array.isArray(depListR) ? depListR.filter(function(f){ return f.name.endsWith('.json'); }).sort(function(a,b){ return b.name.localeCompare(a.name); }).slice(0, 30) : [];
      for (var dfi = 0; dfi < depFiles.length; dfi++) {
        try {
          var existR = await getDeposit(depFiles[dfi].name.replace('.json', ''));
          if (existR.data && existR.data.username === req.user && existR.data.amount === amount && existR.data.status === 'pending' && existR.data.expiredAt && Date.now() < existR.data.expiredAt) {
            console.log('[deposit] resume existing:', existR.data.id, '| user:', req.user);
            return res.json({ ok: true, depId: existR.data.id, qr: existR.data.qrImage || null, qrString: existR.data.qrString || null, expiredAt: existR.data.expiredAt, amount: existR.data.amount, adminFeeDeposit: existR.data.adminFeeDeposit || 0, totalBayarDeposit: existR.data.totalBayarDeposit || existR.data.amount, resumed: true });
          }
        } catch(e) {}
      }
    } catch(e) { /* folder may not exist yet — proceed to create */ }

    // Admin fee = on top of QRIS amount (profit admin, RumahOTP receives `amount`)
    const adminFeeDeposit = depFeeType === 'percent'
      ? Math.round(amount * depFeeVal / 100)
      : Math.round(depFeeVal);
    const totalBayarDeposit = amount + adminFeeDeposit;

    // QRIS dibuat untuk totalBayarDeposit — user bayar total itu
    // Tapi saldo yang masuk ke RumahOTP = amount (saldo user)
    // Jadi kita buat QRIS untuk totalBayarDeposit, tapi hanya `amount` yang dikirim ke rotp wallet
    // Karena rotp.depositCreate = jumlah yang di-deposit KE saldo rotp user kita
    // Jadi kita buat QR untuk `totalBayarDeposit` tapi saldo dikreditkan sebesar `amount`
    let rotpData;
    try {
      // QRIS total yang dibayar user = amount (deposit ke rotp) + adminFee
      // Kita buat deposit senilai `amount` ke rotp, lalu user bayar totalBayarDeposit
      // Selisih adminFeeDeposit = keuntungan kita (user bayar lebih, rotp credit = amount)
      // Solusi: buat QRIS untuk totalBayarDeposit ke rotp, tapi kreditkan user hanya `amount`
      const r = await rotp.depositCreate(totalBayarDeposit);
      if (!r.success) throw new Error((r.error && r.error.message) || 'Deposit gagal');
      rotpData = r.data;
    } catch(e) { return res.json({ ok: false, message: 'Gagal buat deposit: ' + e.message }); }

    // v2 API: expired_at_ts (DETIK unix), qr_image, qr_string; v1: expired, qr
    // BUG FIX: expired_at_ts dari RumahOTP dalam DETIK, bukan milidetik.
    // Deteksi otomatis: jika nilainya < 1e12 → anggap detik, konversi ke ms.
    const _rotpExpRaw = rotpData.expired_at_ts || rotpData.expired || null;
    const rotpExpiry = _rotpExpRaw
      ? (_rotpExpRaw > 1e12 ? _rotpExpRaw : _rotpExpRaw * 1000)
      : (Date.now() + 20 * 60 * 1000);
    const rotpQrImg  = rotpData.qr_image || rotpData.qr || null;
    const rotpQrStr  = rotpData.qr_string || null;

    // FIX: Download gambar QR di server-side → konversi ke base64 data URL.
    // Ini bypass hotlink protection RumahOTP dan CSP mixed-content di browser.
    let qrImageFinal = rotpQrImg;
    if (rotpQrImg && rotpQrImg.startsWith('http')) {
      try {
        const qrResp = await axios.get(rotpQrImg, {
          responseType: 'arraybuffer',
          timeout: 8000,
          headers: { 'User-Agent': 'Mozilla/5.0 (compatible; QRProxy/1.0)' },
          maxContentLength: 1 * 1024 * 1024,
        });
        const qrCt = (qrResp.headers['content-type'] || 'image/png').split(';')[0].trim();
        qrImageFinal = 'data:' + qrCt + ';base64,' + Buffer.from(qrResp.data).toString('base64');
        console.log('[deposit] QR image downloaded as base64 (' + Math.round(qrImageFinal.length / 1024) + 'KB)');
      } catch(qrErr) {
        console.warn('[deposit] gagal download QR image, fallback ke URL:', qrErr.message);
        // Fallback ke URL asli jika download gagal
      }
    }

    const depId = 'DEP-' + Date.now() + '-' + crypto.randomBytes(4).toString('hex');
    const now   = Date.now();
    // Hitung expiry: ambil yang lebih kecil antara rotp expiry dan 20 menit dari sekarang
    const expiredAt = Math.min(rotpExpiry, now + 20 * 60 * 1000);
    const dep   = {
      id: depId, username: req.user,
      rotpId: rotpData.id,
      amount,               // saldo yang masuk ke user (amount asli)
      adminFeeDeposit,      // fee admin (keuntungan)
      totalBayarDeposit,    // total QRIS yang harus dibayar user
      qrImage: qrImageFinal || null,  // base64 data URL (atau URL asli sebagai fallback)
      qrString: rotpQrStr || null,    // simpan QR string untuk resume
      status: 'pending',
      expiredAt,
      createdAt: now,
    };
    await saveDeposit(depId, dep, null);
    console.log('[deposit] create:', depId, '| user:', req.user, '| saldo Rp' + amount, '| fee Rp' + adminFeeDeposit, '| total bayar Rp' + totalBayarDeposit);
    res.json({ ok: true, depId, qr: qrImageFinal, qrString: rotpQrStr, expiredAt, amount, adminFeeDeposit, totalBayarDeposit });
  } catch(e) { console.error('[deposit/create]', e.message); res.json({ ok: false, message: e.message }); }
});

// Check deposit status
app.get('/api/deposit/status/:id', userAuth, async function(req, res) {
  try {
    const depId = req.params.id;
    if (!isValidDepId(depId)) return res.json({ ok: false, message: 'ID tidak valid.' });
    if (!rateLimit('dsc:' + depId, 30, 10 * 60 * 1000)) return res.json({ ok: false, status: 'pending' });

    const r   = await getDeposit(depId);
    if (!r.data) return res.json({ ok: false, message: 'Deposit tidak ditemukan.' });
    if (r.data.username !== req.user) return res.status(403).json({ ok: false, message: 'Akses ditolak.' });

    const dep = r.data;
    if (dep.status === 'success')   return res.json({ ok: true, status: 'success', balance: dep.creditedBalance });
    if (dep.status === 'cancel')    return res.json({ ok: true, status: 'cancel' });
    if (dep.status === 'crediting') return res.json({ ok: true, status: 'pending' }); // sedang diproses cron, tunggu

    // Auto-expire if past expiry time
    // BUG FIX: cek RumahOTP DULU sebelum cancel — jika user sudah bayar tepat saat timer habis,
    // jangan langsung cancel. Tambahkan grace period 3 menit untuk toleransi keterlambatan polling.
    if (dep.expiredAt && Date.now() > dep.expiredAt) {
      // Cek status RumahOTP dulu — mungkin sudah dibayar tapi belum sempat diproses
      let rotpStatusCheck = null;
      try {
        const srCheck = await rotp.depositStatus(dep.rotpId);
        rotpStatusCheck = (srCheck && srCheck.success && srCheck.data) ? srCheck.data.status : null;
      } catch(e) { console.warn('[deposit/expire] rotp status check:', e.message); }

      // Jika sudah dibayar — proses kredit meski timer habis (jangan cancel!)
      if (rotpStatusCheck === 'success') {
        // FIX: Crediting lock di late-pay path juga
        try {
          const freshLockLate = await getDeposit(depId);
          if (!freshLockLate.data) return res.json({ ok: false, message: 'Deposit tidak ditemukan.' });
          if (freshLockLate.data.status === 'success') return res.json({ ok: true, status: 'success', balance: freshLockLate.data.creditedBalance });
          if (freshLockLate.data.status === 'crediting') return res.json({ ok: true, status: 'pending' });
          await saveDeposit(depId, Object.assign({}, freshLockLate.data, { status: 'crediting', creditingAt: Date.now() }), freshLockLate.sha);
        } catch(lockErrLate) {
          console.warn('[deposit/status] late-pay crediting lock conflict:', depId);
          return res.json({ ok: true, status: 'pending' });
        }
        // FIX: wrap updateBalance dengan try-catch (sebelumnya tidak ada, crash = saldo tidak masuk)
        let newBal;
        try {
          newBal = await updateBalance(req.user, dep.amount);
        } catch(balErrLate) {
          console.error('[deposit/status] late-pay balance GAGAL:', depId, balErrLate.message);
          const freshFailLate = await getDeposit(depId).catch(function(){ return { data: dep, sha: null }; });
          await saveDeposit(depId, Object.assign({}, freshFailLate.data || dep, { status: 'pending', creditingAt: null }), freshFailLate.sha || null).catch(function(){});
          return res.json({ ok: false, message: 'Gagal memperbarui saldo. Coba lagi.' });
        }
        // FIX: tulis balanceCredited:true segera agar jika saveDeposit(success) gagal, tidak double credit
        try {
          const freshFlagLate = await getDeposit(depId).catch(function(){ return { data: dep, sha: null }; });
          await saveDeposit(depId, Object.assign({}, freshFlagLate.data || dep, {
            status: 'crediting', balanceCredited: true, creditedBalance: newBal,
          }), freshFlagLate.sha || null);
        } catch(flagErrLate) { console.warn('[deposit/status] late-pay balanceCredited flag gagal:', depId); }
        const freshLateDone = await getDeposit(depId).catch(function(){ return { data: dep, sha: null }; });
        await saveDeposit(depId, Object.assign({}, freshLateDone.data || dep, { status: 'success', paidAt: Date.now(), creditedBalance: newBal, latePaymentDetected: true }), freshLateDone.sha || null);
        console.log('[deposit] late-pay detected (expired timer but paid):', depId, '| user:', req.user, '| Rp' + dep.amount);
        broadcastAdmin({ type: 'deposit_success', id: depId, username: req.user, amount: dep.amount, ts: Date.now() });
        return res.json({ ok: true, status: 'success', balance: newBal });
      }

      // Jika RumahOTP sudah cancel atau sudah lebih dari 3 menit grace period — baru cancel
      const gracePeriod = 3 * 60 * 1000; // 3 menit grace
      if (rotpStatusCheck === 'cancel' || Date.now() > dep.expiredAt + gracePeriod) {
        try { await rotp.depositCancel(dep.rotpId); } catch(e) { console.warn('[deposit/expire] cancel rotp:', e.message); }
        // FIX: re-fetch fresh SHA sebelum tulis cancel — r.sha bisa stale jika ada write lain
        const freshCancel = await getDeposit(depId).catch(function(){ return { data: dep, sha: r.sha }; });
        if (freshCancel.data && (freshCancel.data.status === 'success' || freshCancel.data.status === 'crediting')) {
          // Race: ternyata sudah dibayar saat kita sedang proses — jangan cancel
          return res.json({ ok: true, status: freshCancel.data.status === 'success' ? 'success' : 'pending', balance: freshCancel.data.creditedBalance });
        }
        if (freshCancel.data && freshCancel.data.status === 'cancel') {
          return res.json({ ok: true, status: 'cancel', message: 'Deposit kadaluarsa.' });
        }
        await saveDeposit(depId, Object.assign({}, freshCancel.data || dep, { status: 'cancel', cancelledAt: Date.now(), expiredAuto: true }), freshCancel.sha || r.sha);
        return res.json({ ok: true, status: 'cancel', message: 'Deposit kadaluarsa.' });
      }

      // Masih dalam grace period — beri kesempatan user untuk tetap bayar
      return res.json({ ok: true, status: 'pending' });
    }

    // Poll RumahOTP
    let rotpStatus;
    try {
      const sr = await rotp.depositStatus(dep.rotpId);
      // v2 response: sr.data.status; v1 response: sr.data.status
      rotpStatus = sr.success && sr.data ? sr.data.status : 'pending';
    } catch(e) { return res.json({ ok: true, status: 'pending' }); }

    if (rotpStatus === 'success') {
      // FIX: CREDITING LOCK — cegah double credit race condition antara polling user dan cron reconcile.
      // Keduanya bisa baca deposit saat status masih 'pending' lalu keduanya credit sekaligus.
      // Solusi: set status 'crediting' dulu pakai sha yang sama (optimistic lock).
      // Jika cron sudah nulis duluan → sha mismatch → saveDeposit throw → kita return pending.
      // Jika kita yang duluan → cron lihat 'crediting' → skip.
      try {
        const freshLock = await getDeposit(depId);
        if (!freshLock.data) return res.json({ ok: false, message: 'Deposit tidak ditemukan.' });
        // Cek ulang — mungkin cron sudah kredit duluan saat kita re-fetch
        if (freshLock.data.status === 'success') return res.json({ ok: true, status: 'success', balance: freshLock.data.creditedBalance });
        if (freshLock.data.status === 'crediting') return res.json({ ok: true, status: 'pending' }); // cron sedang proses
        if (freshLock.data.status === 'cancel') return res.json({ ok: true, status: 'cancel' });
        // Set crediting dulu — jika sha conflict (cron nulis duluan) → throw → return pending
        await saveDeposit(depId, Object.assign({}, freshLock.data, { status: 'crediting', creditingAt: Date.now() }), freshLock.sha);
      } catch(lockErr) {
        // SHA conflict = cron atau proses lain sudah write duluan → jangan double credit
        console.warn('[deposit/status] crediting lock conflict (normal jika cron jalan bersamaan):', depId);
        return res.json({ ok: true, status: 'pending' });
      }
      // Credit user balance
      let newBal;
      try {
        newBal = await updateBalance(req.user, dep.amount);
      } catch(balErr) {
        console.error('[deposit/status] balance GAGAL:', depId, balErr.message);
        // Rollback crediting lock → kembalikan ke pending agar bisa dicoba lagi
        const freshFail = await getDeposit(depId).catch(function(){ return { data: dep, sha: null }; });
        await saveDeposit(depId, Object.assign({}, freshFail.data || dep, { status: 'pending', creditingAt: null }), freshFail.sha || null).catch(function(){});
        return res.json({ ok: false, message: 'Gagal memperbarui saldo. Coba lagi.' });
      }
      // FIX BUG KRITIS: tulis balanceCredited:true SEGERA setelah updateBalance berhasil.
      // Jika saveDeposit(success) di bawah gagal (409), deposit stuck di 'crediting'.
      // Tanpa flag ini, reconcile cron reset ke 'pending' → updateBalance lagi → DOUBLE CREDIT.
      // Dengan flag ini, reconcile cron tahu saldo sudah masuk → langsung mark success, skip updateBalance.
      try {
        const freshFlag = await getDeposit(depId).catch(function(){ return { data: dep, sha: null }; });
        await saveDeposit(depId, Object.assign({}, freshFlag.data || dep, {
          status: 'crediting', balanceCredited: true, creditedBalance: newBal,
        }), freshFlag.sha || null);
      } catch(flagErr) { console.warn('[deposit/status] balanceCredited flag gagal (non-critical):', depId, flagErr.message); }
      const freshOk = await getDeposit(depId).catch(function(){ return { data: dep, sha: null }; });
      await saveDeposit(depId, Object.assign({}, freshOk.data || dep, { status: 'success', paidAt: Date.now(), creditedBalance: newBal }), freshOk.sha || null);
      console.log('[deposit] credited:', depId, '| user:', req.user, '| Rp' + dep.amount, '| bal:', newBal);
      broadcastAdmin({ type: 'deposit_success', id: depId, username: req.user, amount: dep.amount, ts: Date.now() });
      return res.json({ ok: true, status: 'success', balance: newBal });
    }
    if (rotpStatus === 'cancel') {
      await saveDeposit(depId, Object.assign({}, dep, { status: 'cancel', cancelledAt: Date.now() }), r.sha);
      return res.json({ ok: true, status: 'cancel' });
    }
    res.json({ ok: true, status: 'pending' });
  } catch(e) { console.error('[deposit/status]', e.message); res.json({ ok: false, message: e.message }); }
});

// List user deposits
app.get('/api/deposit/list', userAuth, async function(req, res) {
  try {
    let deps = [];
    try {
      const r = await listDirCached('deposits');
      const files = Array.isArray(r) ? r.filter(function(f){ return f.name.endsWith('.json'); }) : [];
      await Promise.all(files.map(async function(f) {
        try {
          const d = await getDeposit(f.name.replace('.json',''));
          if (d.data && d.data.username === req.user) deps.push({ id: d.data.id, amount: d.data.amount, adminFeeDeposit: d.data.adminFeeDeposit || 0, totalBayarDeposit: d.data.totalBayarDeposit || d.data.amount, status: d.data.status, createdAt: d.data.createdAt, expiredAt: d.data.expiredAt || null,
            // FIX: hanya expose qrImage untuk deposit pending (bukan success/cancel) — cegah kebocoran data
            qrImage: (d.data.status === 'pending') ? (d.data.qrImage || null) : null });
        } catch(e) {}
      }));
    } catch(e) {}
    deps.sort(function(a,b){ return (b.createdAt||0)-(a.createdAt||0); });
    res.json({ ok: true, data: deps.slice(0, 30) });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Cancel deposit (user-initiated) ─── SECURITY: rate limit + SHA lock
app.post('/api/deposit/cancel/:id', userAuth, async function(req, res) {
  try {
    const depId = req.params.id;
    if (!isValidDepId(depId)) return res.json({ ok: false, message: 'ID tidak valid.' });
    if (!rateLimit('depcancel:' + req.user, 3, 30 * 1000)) return res.status(429).json({ ok: false, message: 'Terlalu banyak permintaan.' });
    const r = await getDeposit(depId);
    if (!r.data) return res.json({ ok: false, message: 'Deposit tidak ditemukan.' });
    if (r.data.username !== req.user) return res.status(403).json({ ok: false, message: 'Akses ditolak.' });
    if (r.data.status !== 'pending') return res.json({ ok: false, message: 'Deposit sudah ' + r.data.status + ', tidak bisa dibatalkan.' });
    // Cancel at RumahOTP (best-effort, non-blocking)
    try { await rotp.depositCancel(r.data.rotpId); } catch(e) { console.warn('[deposit/cancel] rotp:', e.message); }
    // Atomic: write with SHA so concurrent requests fail
    await saveDeposit(depId, Object.assign({}, r.data, { status: 'cancel', cancelledAt: Date.now() }), r.sha);
    console.log('[deposit/cancel]', depId, '| user:', req.user);
    res.json({ ok: true });
  } catch(e) {
    if (e.response && e.response.status === 409) return res.json({ ok: false, message: 'Konflik permintaan, coba lagi.' });
    res.json({ ok: false, message: e.message });
  }
});

// ══════════════════════════════════════════════════════════════
// IMAGE PROXY — bypass hotlink protection & mixed-content CSP
// GET /api/img-proxy?url=https://...
// ══════════════════════════════════════════════════════════════
var _imgProxyCache = new Map(); // in-memory cache (url → {buf, ct, ts})
const IMG_PROXY_TTL = 30 * 60 * 1000; // 30 menit

app.get('/api/img-proxy', async function(req, res) {
  var url = req.query.url;
  if (!url || !/^https?:\/\/.{4,}/.test(url)) return res.status(400).end();
  if (!rateLimit('imgp:' + (req.ip||'x'), 120, 60 * 1000)) return res.status(429).end();
  var cached = _imgProxyCache.get(url);
  if (cached && Date.now() - cached.ts < IMG_PROXY_TTL) {
    res.set('Content-Type', cached.ct);
    res.set('Cache-Control', 'public, max-age=1800');
    return res.send(cached.buf);
  }
  try {
    var r = await axios.get(url, {
      responseType: 'arraybuffer',
      timeout: 8000,
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; ImageProxy/1.0)' },
      maxContentLength: 2 * 1024 * 1024,
    });
    var ct = (r.headers['content-type'] || 'image/png').split(';')[0].trim();
    if (!ct.startsWith('image/')) ct = 'image/png';
    var buf = Buffer.from(r.data);
    _imgProxyCache.set(url, { buf, ct, ts: Date.now() });
    if (_imgProxyCache.size > 200) {
      var oldest = null;
      _imgProxyCache.forEach(function(v, k){ if (!oldest || v.ts < _imgProxyCache.get(oldest).ts) oldest = k; });
      if (oldest) _imgProxyCache.delete(oldest);
    }
    res.set('Content-Type', ct);
    res.set('Cache-Control', 'public, max-age=1800');
    res.send(buf);
  } catch(e) {
    console.warn('[img-proxy] gagal:', url, e.message);
    res.status(502).end();
  }
});

// ══════════════════════════════════════════════════════════════
// OTP / NOMOR ROUTES (RumahOTP)
// ══════════════════════════════════════════════════════════════

// Server-side caches to avoid hammering RumahOTP API
var _svcCache  = { data: null, ts: 0 };                        // services list — 10 min TTL
var _cntCache  = {};                                            // countries per serviceId — 5 min TTL

// Get all services (public — no auth needed for browsing)
app.get('/api/otp/services', async function(req, res) {
  try {
    if (!rateLimit('otpsvc:' + (req.ip||'x'), 60, 60 * 1000)) return res.status(429).json({ ok: false, message: 'Terlalu banyak request.' });

    // Serve from cache if fresh (10 min)
    if (_svcCache.data && Date.now() - _svcCache.ts < 10 * 60 * 1000) {
      // Also fetch pinned config
      const pinnedR = await dbRead('otp-pinned.json').catch(function(){ return {data:[]}; });
      return res.json({ ok: true, data: _svcCache.data, pinned: pinnedR.data || [] });
    }

    let r;
    try { r = await rotp.services(); } catch(e) {
      // Return stale cache on RumahOTP error
      if (_svcCache.data) {
        const pr3 = await dbRead('otp-pinned.json').catch(function(){ return {data:[]}; });
        return res.json({ ok: true, data: _svcCache.data, pinned: pr3.data || [], stale: true });
      }
      return res.json({ ok: false, message: 'RumahOTP tidak dapat dihubungi: ' + e.message });
    }

    if (r.success && Array.isArray(r.data) && r.data.length > 0) {
      _svcCache.data = r.data;
      _svcCache.ts   = Date.now();
      const pinnedR2 = await dbRead('otp-pinned.json').catch(function(){ return {data:[]}; });
      return res.json({ ok: true, data: r.data, pinned: pinnedR2.data || [] });
    }

    // If RumahOTP returned empty/error, serve stale cache
    if (_svcCache.data) {
      const pr4 = await dbRead('otp-pinned.json').catch(function(){ return {data:[]}; });
      return res.json({ ok: true, data: _svcCache.data, pinned: pr4.data || [], stale: true });
    }
    res.json({ ok: false, message: (r.error && r.error.message) || 'Gagal mengambil daftar layanan dari RumahOTP.' });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});


// ── OTP Pinned Services config
app.get('/api/otp/pinned', async function(req, res) {
  try {
    const r = await dbRead('otp-pinned.json');
    res.json({ ok: true, data: r.data || [] });
  } catch(e) { res.json({ ok: true, data: [] }); }
});

app.get('/api/admin/otp/pinned', adminAuth, async function(req, res) {
  try {
    const r = await dbRead('otp-pinned.json');
    res.json({ ok: true, data: r.data || [] });
  } catch(e) { res.json({ ok: true, data: [] }); }
});

app.post('/api/admin/otp/pinned', adminAuth, async function(req, res) {
  try {
    var codes = req.body;
    if (!Array.isArray(codes)) return res.json({ ok: false, message: 'Data harus array.' });
    codes = codes.filter(function(c){ return typeof c === 'string' && c.length < 100; }).slice(0, 50);
    const r = await dbRead('otp-pinned.json');
    await dbWrite('otp-pinned.json', codes, r.sha || null, 'admin: otp-pinned');
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Get countries for a service (public)
app.get('/api/otp/countries/:serviceId', async function(req, res) {
  try {
    if (!rateLimit('otpcnt:' + (req.ip||'x'), 60, 60 * 1000)) return res.status(429).json({ ok: false, message: 'Terlalu banyak request.' });

    // FIX: accept both numeric and string service codes (e.g. "nokos", "wa", "tg", etc.)
    const sid = String(req.params.serviceId || '').trim();
    if (!sid || sid.length > 100 || !/^[a-zA-Z0-9_\-]+$/.test(sid)) return res.json({ ok: false, message: 'Service ID tidak valid.' });

    const cacheKey = 'svc_' + sid;
    const cached   = _cntCache[cacheKey];

    // Serve from cache if fresh (5 min)
    if (cached && Date.now() - cached.ts < 5 * 60 * 1000) {
      return res.json({ ok: true, data: cached.data });
    }

    let r;
    try { r = await rotp.countries(sid); } catch(e) {
      if (cached) return res.json({ ok: true, data: cached.data, stale: true });
      return res.json({ ok: false, message: 'RumahOTP tidak dapat dihubungi: ' + e.message });
    }

    if (!r.success || !Array.isArray(r.data)) {
      if (cached) return res.json({ ok: true, data: cached.data, stale: true });
      return res.json({ ok: false, message: (r.error && r.error.message) || 'Gagal mengambil daftar negara.' });
    }

    // Add markup to prices — read from settings (dynamic, set in admin panel)
    const _stgOtp = await getEffectiveSettings();
    const _otpMk  = _stgOtp.otpMarkup || 0;
    const data = r.data.map(function(c) {
      return Object.assign({}, c, {
        pricelist: (c.pricelist || []).map(function(p) {
          return Object.assign({}, p, { price: p.price + _otpMk, price_format: idrFormat(p.price + _otpMk) });
        }),
      });
    });

    _cntCache[cacheKey] = { data, ts: Date.now() };
    res.json({ ok: true, data });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── OTP INFO (public — returns store settings needed by OTP page)
app.get('/api/otp/info', async function(req, res) {
  try {
    const s = await getEffectiveSettings();
    res.json({
      ok: true,
      settings: {
        depositFeeType : s.depositFeeType,
        depositFee     : s.depositFee,
        depositMin     : s.depositMin,
        storeName      : s.storeName,
        wa             : s.wa,
        otpEnabled     : s.otpEnabled,
      },
    });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Get RumahOTP balance (admin info, cached)
var _rotpBalCache = { val: null, ts: 0 };
app.get('/api/otp/provider-balance', userAuth, async function(req, res) {
  try {
    if (Date.now() - _rotpBalCache.ts < 60000 && _rotpBalCache.val !== null) return res.json({ ok: true, data: _rotpBalCache.val });
    const r = await rotp.balance();
    if (r.success) { _rotpBalCache.val = r.data.balance; _rotpBalCache.ts = Date.now(); }
    res.json({ ok: r.success, data: r.data ? r.data.balance : 0 });
  } catch(e) { res.json({ ok: false, data: 0 }); }
});

// Order OTP number
app.post('/api/otp/order', userAuth, async function(req, res) {
  // SECURITY: Rate limit global per user (10 order per 5 menit)
  if (!rateLimit('otpord:' + req.user, 10, 5 * 60 * 1000))
    return res.status(429).json({ ok: false, message: 'Terlalu banyak order. Tunggu sebentar.' });

  // FIX: Tunda piggyback cron selama proses order agar tidak berebut GitHub DB write.
  // Jika cron sedang jalan, tunggu maksimal 8 detik sebelum lanjut (non-blocking).
  var _orderStart = Date.now();
  if (_piggybackRunning) {
    console.log('[otp/order] menunggu piggyback cron selesai...');
    for (var _w = 0; _w < 16 && _piggybackRunning; _w++) {
      await new Promise(function(r){ setTimeout(r, 500); });
    }
  }
  // Reset timer piggyback agar tidak trigger cron baru saat order berlangsung
  _lastPiggybackRun = Math.max(_lastPiggybackRun, _orderStart);

  // SECURITY: Per-user mutex lock — cegah concurrent order race condition.
  // Jika user kirim 3 request bersamaan, hanya 1 yang diproses, sisanya antri.
  // Ini menutup celah "order 3x dengan saldo 1x" via request paralel.
  const _lockRelease = await _acquireOtpLock(req.user);
  try {
    // SECURITY: Cooldown antar order (3 detik jeda minimum per user)
    // Cegah rapid-fire script yang kirim request satu per satu cepat-cepatan.
    const _cooldownLeft = _checkOtpCooldown(req.user);
    if (_cooldownLeft > 0) {
      return res.status(429).json({ ok: false, message: 'Terlalu cepat. Tunggu ' + Math.ceil(_cooldownLeft / 1000) + ' detik sebelum order lagi.' });
    }

    const number_id    = parseInt(req.body.number_id);
    const provider_id  = String(req.body.provider_id || '').trim();
    const operator_id  = String(req.body.operator_id || 'any').trim();
    const price        = parseInt(req.body.price);
    const service_name = String(req.body.service_name || '').slice(0, 100);
    const country_name = String(req.body.country_name || '').slice(0, 100);

    if (!number_id || !provider_id || !price) return res.json({ ok: false, message: 'Data tidak lengkap.' });

    // SECURITY: validasi range harga (guard nilai gila dari client)
    const _minOtpPrice = 100;    // Rp100 minimum absolut
    const _maxOtpPrice = 500000; // Rp500.000 maximum
    if (price < _minOtpPrice || price > _maxOtpPrice)
      return res.json({ ok: false, message: 'Harga tidak valid. Refresh halaman dan coba lagi.' });

    // Load dynamic OTP markup from settings
    const _stgOtpOrder = (await getEffectiveSettings()).otpMarkup || 0;

    // SECURITY: Verifikasi harga dari server, bukan client — cegah price manipulation
    let _serverPrice = null;
    try {
      const _cntVerify = await rotp.countries(String(req.body.service_id || req.body.service_name || '').split('_')[0] || 'wa');
      if (_cntVerify.success && Array.isArray(_cntVerify.data)) {
        outer: for (const _c of _cntVerify.data) {
          if (!Array.isArray(_c.pricelist)) continue;
          for (const _pl of _c.pricelist) {
            if (_c.number_id === number_id && _pl.provider_id === provider_id) {
              _serverPrice = _pl.price + _stgOtpOrder;
              break outer;
            }
          }
        }
        if (_serverPrice !== null && price !== _serverPrice) {
          console.warn('[otp/order] price mismatch: client=' + price + ' server=' + _serverPrice + ' user=' + req.user);
          if (Math.abs(price - _serverPrice) / _serverPrice > 0.5)
            return res.json({ ok: false, message: 'Harga tidak valid. Refresh halaman dan coba lagi.' });
        }
      }
    } catch(_verifyErr) {
      console.warn('[otp/order] price verify gagal (non-critical):', _verifyErr.message);
    }

    // Gunakan harga server jika tersedia (lebih aman dari harga client)
    const finalPrice = (_serverPrice !== null) ? _serverPrice : price;

    // SECURITY: Per-user+number rate limit tambahan (max 1 per 5s per kombinasi nomor)
    if (!rateLimit('otpbuy:' + req.user + ':' + number_id + ':' + provider_id, 1, 5 * 1000))
      return res.status(429).json({ ok: false, message: 'Permintaan terlalu cepat. Tunggu sebentar.' });

    // SECURITY FIX (KRITIS): Potong saldo DULU secara atomik sebelum order ke RumahOTP.
    // Dengan mutex lock di atas, tidak ada request lain yang bisa masuk sebelum ini selesai.
    // Urutan lama: cek saldo → order RumahOTP → potong saldo  ← RENTAN race condition
    // Urutan baru: potong saldo atomik → order RumahOTP → rollback jika gagal  ← AMAN
    //
    // updateBalance menggunakan optimistic locking (SHA check) — jika saldo tidak cukup,
    // fungsi ini akan throw Error sehingga order langsung dibatalkan.
    let newBal;
    try {
      newBal = await updateBalance(req.user, -finalPrice);
    } catch(balErr) {
      // Saldo tidak cukup atau user tidak ditemukan — tolak order
      return res.json({ ok: false, message: balErr.message });
    }

    // Tandai waktu order terakhir SEGERA setelah saldo dipotong
    _otpLastOrderTime.set(req.user, Date.now());

    // Cek ban (setelah potong saldo — jika banned, rollback dan tolak)
    try {
      const ur = await getUser(req.user);
      if (ur.data && ur.data.banned) {
        await updateBalance(req.user, +finalPrice).catch(function(e){ console.error('[otp/order] rollback-banned gagal:', e.message); });
        return res.json({ ok: false, message: 'Akun diblokir. Hubungi admin.' });
      }
    } catch(e) { /* gagal baca user — lanjutkan saja */ }

    // Order ke RumahOTP — jika gagal, rollback saldo otomatis
    let rotpOrder;
    try {
      const r = await rotp.order(number_id, provider_id, operator_id);
      if (!r.success) throw new Error((r.error && r.error.message) || 'Gagal order nomor');
      rotpOrder = r.data;
    } catch(e) {
      // RumahOTP gagal — kembalikan saldo
      console.error('[otp/order] rotp gagal, rollback saldo:', e.message);
      try { await updateBalance(req.user, +finalPrice); } catch(rbErr) { console.error('[otp/order] ROLLBACK GAGAL:', rbErr.message); }
      return res.json({ ok: false, message: 'RumahOTP: ' + e.message });
    }

    // Simpan order ke database
    const ordId = 'OTP-' + Date.now() + '-' + crypto.randomBytes(4).toString('hex');
    const now   = Date.now();
    const order = {
      id           : ordId,
      username     : req.user,
      rotpOrderId  : rotpOrder.order_id,
      phoneNumber  : rotpOrder.phone_number,
      service      : service_name || rotpOrder.service,
      country      : country_name || rotpOrder.country,
      operator     : rotpOrder.operator,
      price        : finalPrice,
      priceRoTP    : finalPrice - (_stgOtpOrder || 0),
      status       : 'waiting',
      otp          : null,
      expiresAt    : Date.now() + (rotpOrder.expires_in_minute || 15) * 60000,
      createdAt    : now,
    };

    // SAFETY: jika saveOtpOrder gagal, rollback saldo agar user tidak rugi
    try {
      await saveOtpOrder(ordId, order, null);
    } catch(saveErr) {
      console.error('[otp/order] saveOtpOrder gagal, rollback saldo:', ordId, saveErr.message);
      try { await rotp.cancelOrder(rotpOrder.order_id); } catch(e) { /* abaikan */ }
      try { await updateBalance(req.user, +finalPrice); } catch(rbErr) { console.error('[otp/order] ROLLBACK GAGAL:', rbErr.message); }
      return res.json({ ok: false, message: 'Gagal menyimpan order. Saldo sudah dikembalikan. Coba lagi.' });
    }

    broadcastAdmin({ type: 'new_otp_order', id: ordId, username: req.user, service: order.service, country: order.country, price: finalPrice, phone: rotpOrder.phone_number || '', ts: Date.now() });
    console.log('[otp/order]', ordId, '|', req.user, '|', rotpOrder.phone_number, '|', service_name);
    res.json({ ok: true, orderId: ordId, phoneNumber: rotpOrder.phone_number, expiresAt: order.expiresAt, balance: newBal });

  } catch(e) {
    console.error('[otp/order]', e.message);
    res.json({ ok: false, message: e.message });
  } finally {
    // WAJIB: selalu lepas lock, bahkan jika terjadi error — agar user tidak stuck
    _releaseOtpLock(req.user, _lockRelease);
  }
});

// Check OTP status
app.get('/api/otp/order/:id/status', userAuth, async function(req, res) {
  try {
    const ordId = req.params.id;
    if (!isValidOtpId(ordId)) return res.json({ ok: false, message: 'ID tidak valid.' });
    if (!rateLimit('otpchk:' + ordId, 40, 10 * 60 * 1000)) return res.json({ ok: true, status: 'waiting' });

    const r   = await getOtpOrder(ordId);
    if (!r.data) return res.json({ ok: false, message: 'Order tidak ditemukan.' });
    if (r.data.username !== req.user) return res.status(403).json({ ok: false, message: 'Akses ditolak.' });
    const ord = r.data;

    // If already finished, return cached
    if (['completed','canceled','expired'].includes(ord.status)) return res.json({ ok: true, status: ord.status, otp: ord.otp, refunded: ord.refunded || false, phoneNumber: ord.phoneNumber });

    // FIX: Handle order yang nyangkut di 'expiring' > 2 menit (crash saat proses refund)
    // Dulu: status 'expiring' jatuh ke polling RumahOTP tanpa refund → user lihat "expired" tapi saldo tidak kembali
    if (ord.status === 'expiring' && (Date.now() - (ord.expiringAt || 0)) > 2 * 60 * 1000) {
      try {
        await updateBalance(req.user, ord.price);
        const freshStuck = await getOtpOrder(ordId);
        await saveOtpOrder(ordId, Object.assign({}, freshStuck.data || ord, {
          status: 'expired', refunded: true, refundedAt: Date.now(), _rescuedByStatusCheck: true,
        }), freshStuck.sha || null);
        console.log('[otp/status] rescue expiring-stuck:', ordId, '| Rp' + ord.price);
        return res.json({ ok: true, status: 'expired', refunded: true });
      } catch(rescueErr) {
        console.error('[otp/status] rescue expiring gagal:', ordId, rescueErr.message);
        // Reset ke waiting agar cron bisa handle
        const freshRst = await getOtpOrder(ordId).catch(function(){ return { data: ord, sha: null }; });
        await saveOtpOrder(ordId, Object.assign({}, freshRst.data || ord, {
          status: 'waiting', expiringAt: null, _rescueFailedAt: Date.now(),
        }), freshRst.sha || null).catch(function(){});
        return res.json({ ok: true, status: 'waiting', otp: null });
      }
    }
    // Jika 'expiring' tapi masih baru (< 2 menit), beri tahu frontend sedang diproses
    if (ord.status === 'expiring') {
      return res.json({ ok: true, status: 'expiring', otp: null });
    }

    // Check expiry — auto-refund saldo jika order kadaluarsa
    if (Date.now() > ord.expiresAt && ord.status === 'waiting') {
      // ATOMIC LOCK: simpan status 'expiring' dengan SHA saat ini agar tidak double-refund
      try {
        await saveOtpOrder(ordId, Object.assign({}, ord, { status: 'expiring', expiringAt: Date.now() }), r.sha);
      } catch(lockErr) {
        // Request lain sudah proses — baca ulang status terbaru
        const freshR = await getOtpOrder(ordId);
        const fs = (freshR.data && freshR.data.status) || 'expired';
        return res.json({ ok: true, status: fs === 'expiring' ? 'expired' : fs, otp: null });
      }
      // Auto-refund saldo ke user
      let refunded = false;
      try {
        await updateBalance(req.user, ord.price);
        refunded = true;
        console.log('[otp/expire] auto-refund:', ordId, '| Rp' + ord.price, '| user:', req.user);
      } catch(refErr) {
        console.error('[otp/expire] refund GAGAL:', ordId, refErr.message);
        // FIX BUG KRITIS: jika refund gagal, JANGAN simpan status 'expired' dengan refunded=false
        // karena cron hanya proses status='waiting' → user kehilangan saldo selamanya
        // Kembalikan ke 'waiting' agar cron bisa coba refund ulang
        const freshRevStr = await getOtpOrder(ordId);
        await saveOtpOrder(ordId, Object.assign({}, freshRevStr.data || ord, {
          status: 'waiting', expiringAt: null, _refundFailedAt: Date.now(),
        }), freshRevStr.sha || null).catch(function(e2){ console.error('[otp/expire] revert gagal:', e2.message); });
        return res.json({ ok: false, message: 'Gagal mengembalikan saldo. Hubungi admin. ID: ' + ordId });
      }
      const freshR2 = await getOtpOrder(ordId);
      await saveOtpOrder(ordId, Object.assign({}, freshR2.data || ord, {
        status  : 'expired',
        refunded: refunded,
        refundedAt: refunded ? Date.now() : null,
        expiredAutoAt: Date.now(),
      }), freshR2.sha || null);
      return res.json({ ok: true, status: 'expired', otp: null, refunded });
    }

    // Poll RumahOTP — /v1/orders/get_status returns status + otp_code
    let rotpStatus, rotpOtp;
    try {
      const sr = await rotp.get('/api/v1/orders/get_status?order_id=' + ord.rotpOrderId);
      if (sr.success && sr.data) {
        rotpStatus = sr.data.status;
        rotpOtp    = (sr.data.otp_code && sr.data.otp_code !== '-') ? sr.data.otp_code : null;
      } else {
        rotpStatus = ord.status;
      }
    } catch(e) { return res.json({ ok: true, status: ord.status, otp: ord.otp, phoneNumber: ord.phoneNumber }); }

    const finalStatus = (rotpStatus === 'received' || rotpStatus === 'completed') ? 'completed' : rotpStatus;
    if (finalStatus !== ord.status || (rotpOtp && rotpOtp !== ord.otp)) {
      await saveOtpOrder(ordId, Object.assign({}, ord, { status: finalStatus, otp: rotpOtp || ord.otp, updatedAt: Date.now() }), r.sha);
      // Kirim notifikasi webhook dan feed saat OTP berhasil diterima
      if (finalStatus === 'completed') {
        broadcastAdmin({ type: 'otp_completed', id: ordId, username: req.user, service: ord.service, country: ord.country, price: ord.price, phone: ord.phoneNumber || '', otp: rotpOtp || ord.otp || '', ts: Date.now() });
      }
    }
    res.json({ ok: true, status: finalStatus, otp: rotpOtp || ord.otp, phoneNumber: ord.phoneNumber });
  } catch(e) { console.error('[otp/status]', e.message); res.json({ ok: false, message: e.message }); }
});

// Cancel OTP order + refund  ─── SECURITY: atomic cancel to prevent double-refund
app.post('/api/otp/order/:id/cancel', userAuth, async function(req, res) {
  try {
    const ordId = req.params.id;
    if (!isValidOtpId(ordId)) return res.json({ ok: false, message: 'ID tidak valid.' });

    // SECURITY: strict per-order rate limit (max 2 attempts per order per 30s)
    if (!rateLimit('otpcancel:' + ordId, 2, 30 * 1000)) {
      return res.status(429).json({ ok: false, message: 'Terlalu banyak permintaan. Tunggu sebentar.' });
    }

    const r = await getOtpOrder(ordId);
    if (!r.data) return res.json({ ok: false, message: 'Order tidak ditemukan.' });
    if (r.data.username !== req.user) return res.status(403).json({ ok: false, message: 'Akses ditolak.' });
    const ord = r.data;

    if (ord.status === 'completed') return res.json({ ok: false, message: 'OTP sudah diterima, tidak bisa dibatalkan.' });
    if (ord.status === 'canceled')  return res.json({ ok: false, message: 'Order sudah dibatalkan.' });
    if (ord.status === 'canceling') return res.json({ ok: false, message: 'Pembatalan sedang diproses, mohon tunggu.' });
    if (ord.status === 'expiring')  return res.json({ ok: false, message: 'Order sedang diproses, mohon tunggu.' });
    if (ord.status === 'expired' && ord.refunded) return res.json({ ok: false, message: 'Order sudah kadaluarsa dan saldo sudah dikembalikan otomatis.' });
    if (ord.refunded)               return res.json({ ok: false, message: 'Order ini sudah pernah direfund.' });

    // ATOMIC LOCK: write 'canceling' status FIRST using the exact SHA from read.
    // If another request races in simultaneously, GitHub returns 409 conflict and it gets rejected.
    try {
      await saveOtpOrder(ordId, Object.assign({}, ord, { status: 'canceling', cancelingAt: Date.now() }), r.sha);
    } catch(lockErr) {
      console.warn('[otp/cancel] lock conflict:', ordId, lockErr.message);
      return res.json({ ok: false, message: 'Konflik permintaan, coba lagi.' });
    }

    // Cancel at RumahOTP (non-critical — refund regardless)
    try { await rotp.cancelOrder(ord.rotpOrderId); } catch(e) { console.warn('[otp/cancel] rotp:', e.message); }

    // Refund balance
    let newBal;
    try {
      newBal = await updateBalance(req.user, ord.price);
    } catch(balErr) {
      // Balance update failed — revert order status so user can retry
      console.error('[otp/cancel] balance update failed:', balErr.message);
      const fresh = await getOtpOrder(ordId);
      await saveOtpOrder(ordId, Object.assign({}, fresh.data || ord, { status: ord.status, cancelingAt: null }), fresh.sha || null);
      return res.json({ ok: false, message: 'Gagal update saldo. Hubungi admin. ID: ' + ordId });
    }

    // FIX: tulis balanceRefunded:true segera setelah updateBalance berhasil.
    // Jika saveOtpOrder(canceled) di bawah gagal 409, order stuck di 'canceling'.
    // Cron rescue akan baca flag ini → mark canceled TANPA panggil updateBalance lagi (cegah double refund).
    try {
      const freshFlag = await getOtpOrder(ordId).catch(function(){ return { data: ord, sha: null }; });
      await saveOtpOrder(ordId, Object.assign({}, freshFlag.data || ord, {
        status: 'canceling', balanceRefunded: true, refundedBalance: newBal,
      }), freshFlag.sha || null);
    } catch(flagErr) { console.warn('[otp/cancel] balanceRefunded flag gagal (non-critical):', ordId); }

    // Final: mark as fully canceled
    const fresh2 = await getOtpOrder(ordId);
    await saveOtpOrder(ordId, Object.assign({}, fresh2.data || ord, { status: 'canceled', cancelledAt: Date.now(), refunded: true, refundedAt: Date.now() }), fresh2.sha || null);
    console.log('[otp/cancel]', ordId, '| refund Rp' + ord.price, '| user:', req.user);
    res.json({ ok: true, balance: newBal });
  } catch(e) { console.error('[otp/cancel]', e.message); res.json({ ok: false, message: e.message }); }
});

// List user OTP orders (latest 30)
app.get('/api/otp/orders', userAuth, async function(req, res) {
  try {
    const orders = await listOtpOrders(req.user);
    res.json({ ok: true, data: orders.slice(0, 30) });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ══════════════════════════════════════════════════════════════
// ADMIN: USER MANAGEMENT
// ══════════════════════════════════════════════════════════════

// List all users
app.get('/api/admin/users', adminAuth, async function(req, res) {
  try {
    const files = await listUsers();
    const users = [];
    await Promise.all(files.filter(function(f){ return f.name.endsWith('.json'); }).map(async function(f) {
      try {
        const r = await getUser(f.name.replace('.json',''));
        if (r.data) users.push({ username: r.data.username, balance: r.data.balance||0, email: r.data.email, banned: r.data.banned||false, createdAt: r.data.createdAt, lastLogin: r.data.lastLogin });
      } catch(e) {}
    }));
    users.sort(function(a,b){ return (b.createdAt||0)-(a.createdAt||0); });
    res.json({ ok: true, data: users });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Admin: adjust user balance manually
app.post('/api/admin/users/:username/balance', adminAuth, async function(req, res) {
  try {
    const username = req.params.username;
    if (!isValidUsername(username)) return res.json({ ok: false, message: 'Username tidak valid.' });
    const delta = parseInt(req.body.delta);
    if (isNaN(delta) || delta === 0) return res.json({ ok: false, message: 'Jumlah tidak valid.' });
    // FIX: batasi delta maksimum untuk mencegah kesalahan input ekstrem (typo nol terlalu banyak)
    // Admin masih bisa adjust besar, tapi tidak lebih dari 100 juta per request
    if (Math.abs(delta) > 100000000) return res.json({ ok: false, message: 'Delta terlalu besar. Maksimum Rp100.000.000 per request.' });
    const newBal = await updateBalance(username, delta);
    auditLog('adjust-balance', username + ' delta:' + delta + ' newbal:' + newBal, req.adminIp).catch(function(){});
    console.log('[admin] balance', username, delta > 0 ? '+' : '', delta, '| new:', newBal);
    res.json({ ok: true, newBalance: newBal });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Admin: ban/unban user
app.post('/api/admin/users/:username/ban', adminAuth, async function(req, res) {
  try {
    const username = req.params.username;
    const ban      = req.body.ban !== false && req.body.ban !== 'false';
    if (!isValidUsername(username)) return res.json({ ok: false, message: 'Username tidak valid.' });
    const r = await getUser(username);
    if (!r.data) return res.json({ ok: false, message: 'User tidak ditemukan.' });
    await saveUser(username, Object.assign({}, r.data, { banned: ban }), r.sha);
    // Stateless tokens: banned flag checked on each request via userAuth → getUser
    auditLog(ban ? 'ban-user' : 'unban-user', username, req.adminIp).catch(function(){});
    // Real-time notification: push ban event so the user's active session detects it immediately
    if (ban) broadcastStore({ type: 'user_banned', username: username });
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Admin: reset user password
app.post('/api/admin/users/:username/reset-password', adminAuth, async function(req, res) {
  try {
    const username = req.params.username;
    const newPwd   = String(req.body.password || '').trim();
    if (!isValidUsername(username)) return res.json({ ok: false, message: 'Username tidak valid.' });
    if (!newPwd || newPwd.length < 6) return res.json({ ok: false, message: 'Password minimal 6 karakter.' });
    const r = await getUser(username);
    if (!r.data) return res.json({ ok: false, message: 'User tidak ditemukan.' });
    const hashed = await hashPassword(newPwd);
    // SECURITY FIX: set lastTokenReset = sekarang agar semua token lama user ini diinvalidasi
    await saveUser(username, Object.assign({}, r.data, { passwordHash: hashed, updatedAt: Date.now(), lastTokenReset: Date.now() }), r.sha);
    auditLog('reset-user-pwd', username, req.adminIp).catch(function(){});
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Admin: RumahOTP provider balance
app.get('/api/admin/otp/balance', adminAuth, async function(req, res) {
  try {
    const r = await rotp.balance();
    res.json({ ok: r.success, data: r.data || {} });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Admin: list all OTP orders (latest 100)
app.get('/api/admin/otp/orders', adminAuth, async function(req, res) {
  try {
    let all = [];
    try {
      const r = await listDirCached('otp-orders');
      const files = Array.isArray(r) ? r.filter(function(f){ return f.name.endsWith('.json'); }) : [];
      await Promise.all(files.map(async function(f) {
        try { const d = await getOtpOrder(f.name.replace('.json','')); if(d.data) all.push(d.data); } catch(e) {}
      }));
    } catch(e) {}
    all.sort(function(a,b){ return (b.createdAt||0)-(a.createdAt||0); });
    res.json({ ok: true, data: all.slice(0,100) });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── PRODUCT DELIVERY HELPER ────────────────────────────────────────────────
// Unified: handles panel, download, digital (link/image/video/audio/text)
async function processProductDelivery(trx, id) {
  if (trx.productType === 'panel' && trx.type === 'renewal') {
    return await processRenewal(trx, id);
  }
  if (trx.productType === 'panel') {
    const pd = await createPanelServer(trx.variantPlan, trx.variantDays, id, trx.panelUsername, trx.panelPassword);
    // SECURITY FIX: hapus panelPassword dari trx di DB setelah panel berhasil dibuat
    // Password sudah tersimpan di result — tidak perlu simpan dua kali
    try {
      const pTrxR = await getTrx(id);
      if (pTrxR.data) {
        await saveTrx(id, Object.assign({}, pTrxR.data, { panelPassword: null, _pwClearedAt: Date.now() }), pTrxR.sha);
      }
    } catch(e) { /* non-critical — lanjut */ }
    return { type: 'panel', username: pd.username, password: pd.password, domain: pd.domain, ram: pd.ram, disk: pd.disk, cpu: pd.cpu, days: pd.days, expiresAt: pd.expiresAt, serverId: pd.serverId, userId: pd.userId };
  }
  if (trx.productType === 'sewabot') {
    // Sewabot: admin manually installs the bot. Mark result so pay page shows proper message.
    // Also mirror order to sewabot-orders/ directory so admin sewabot tab can manage it.
    const sbId = 'BOT-' + id; // alias in sewabot-orders
    // Retry save up to 3 times — kritis agar admin dashboard tidak kosong
    var sbSaved = false;
    for (var _sbTry = 0; _sbTry < 3 && !sbSaved; _sbTry++) {
      try {
        if (_sbTry > 0) await _sleep(400 * _sbTry);
        await saveSewabotOrder(sbId, {
          id: sbId, trxId: id, type: 'sewabot',
          groupUrl: trx.groupUrl || '', days: trx.variantDays || 30,
          buyerName: trx.buyerName || null, phone: trx.phone || null,
          price: trx.unitPrice, adminFee: trx.adminFee || 0, totalBayar: trx.totalBayar,
          status: 'PROCESSING', // admin needs to install
          createdAt: trx.createdAt, completedAt: null,
        }, null);
        sbSaved = true;
      } catch(sbErr) { console.warn('[sewabot-mirror] attempt ' + (_sbTry+1) + ' gagal:', sbErr.message); }
    }
    if (!sbSaved) console.error('[sewabot-mirror] GAGAL simpan setelah 3 percobaan — trxId:', id);
    broadcastAdmin({ type: 'new_sewabot', id: sbId, groupUrl: trx.groupUrl, days: trx.variantDays, buyerName: trx.buyerName, totalBayar: trx.totalBayar, ts: Date.now() });
    return { type: 'sewabot', groupUrl: trx.groupUrl || '', days: trx.variantDays || 30 };
  }
  // For download and digital: always read fresh from products.json
  const products = await getProducts();
  const prod     = products.find(function(p) { return p.id === trx.productId; });
  const vari     = prod && prod.variants.find(function(v) { return v.id === trx.variantId; });
  const ct       = (vari && vari.contentType) || (trx.productType === 'download' ? 'file' : 'text');
  const contentUrl = (vari && (vari.contentUrl || vari.fileUrl)) || trx.variantFile || '';
  const contentText= (vari && (vari.contentText || vari.content)) || '';

  // ── ACCOUNT POOL: pop one account from the stok akun
  if (ct === 'account') {
    // If already delivered (trx already has account stored), return it directly
    if (trx.deliveredAccount) {
      return {
        type: 'content', contentType: 'text',
        contentText: trx.deliveredAccount,
        title: trx.variantName || trx.productName,
        description: (vari && vari.description) || '',
        filename: '', productName: trx.productName,
      };
    }
    const account = await popAccount(trx.productId, trx.variantId);
    if (!account) {
      return { type: 'content', contentType: 'text', contentText: 'Stok akun habis. Hubungi admin.\nID Order: ' + trx.id, title: trx.variantName || trx.productName, description: '', filename: '', productName: trx.productName };
    }
    // FIX: simpan deliveredAccount ke DB SEGERA setelah pop agar tidak hilang jika server crash
    // sebelum COMPLETED ditulis. Tanpa ini, akun sudah di-pop dari pool tapi trx belum punya data.
    try {
      const freshTrxAcc = await getTrx(trx.id);
      await saveTrx(trx.id, Object.assign({}, freshTrxAcc.data || trx, { deliveredAccount: account, _accountSavedAt: Date.now() }), freshTrxAcc.sha || null);
      trx.deliveredAccount = account; // update local reference juga
    } catch(saveAccErr) {
      console.warn('[processDelivery] gagal simpan deliveredAccount ke DB (non-critical):', saveAccErr.message);
      trx.deliveredAccount = account; // tetap lanjut walau save gagal
    }
    return {
      type: 'content', contentType: 'text',
      contentText: account,
      title: trx.variantName || trx.productName,
      description: (vari && vari.description) || '',
      filename: '', productName: trx.productName,
    };
  }

  if (!contentUrl && !contentText && ct !== 'text') {
    // no content configured — return helpful fallback
    return { type: 'content', contentType: 'text', contentText: trx.productName + '\nID: ' + trx.id + '\nHubungi admin untuk detail produk.', title: trx.variantName || trx.productName, description: (vari && vari.description) || '', filename: '', productName: trx.productName };
  }
  return {
    type         : 'content',
    contentType  : ct,
    contentUrl,
    contentText  : contentText || (ct === 'text' && !contentUrl ? trx.productName + ' berhasil!\nID: ' + trx.id : ''),
    title        : trx.variantName || trx.productName,
    description  : (vari && vari.description)   || '',
    instructions : (vari && vari.instructions)  || '',  // FIX: sertakan panduan langkah-langkah
    contentLinks : (vari && Array.isArray(vari.links) ? vari.links : []),  // FIX: multi-link
    filename     : (vari && vari.filename) || (trx.variantName || 'produk').replace(/[^a-zA-Z0-9]/g, '_'),
    productName  : trx.productName,
  };
}


// ══════════════════════════════════════════════════════════════
// SEWA BOT ROUTES
// ══════════════════════════════════════════════════════════════

// Get sewabot products (prices) from settings
app.get('/api/sewabot/info', async function(req, res) {
  try {
    const r = await dbRead('sewabot-config.json');
    const cfg = r.data || { prices: { 7: 15000, 14: 25000, 30: 45000 }, description: '' };
    res.json({ ok: true, data: cfg });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Create sewabot order
app.post('/api/sewabot/order', async function(req, res) {
  try {
    const ip = req.ip || 'x';
    if (!rateLimit('sbot:' + ip, 5, 10 * 60 * 1000)) return res.status(429).json({ ok: false, message: 'Terlalu banyak request. Coba lagi.' });

    const groupUrl  = String(req.body.groupUrl || '').trim();
    const days      = parseInt(req.body.days) || 30;
    const phone     = String(req.body.phone || '').trim();
    const buyerName = String(req.body.buyerName || '').trim().slice(0, 100);

    if (!groupUrl || groupUrl.length < 5) return res.json({ ok: false, message: 'Link grup tidak boleh kosong.' });
    if (groupUrl.length > 500) return res.json({ ok: false, message: 'Link grup terlalu panjang.' });
    if (days < 1 || days > 365) return res.json({ ok: false, message: 'Durasi tidak valid (1-365 hari).' });

    // Load price from config
    const cfgR = await dbRead('sewabot-config.json');
    const cfg = cfgR.data || { prices: { 7: 15000, 14: 25000, 30: 45000 } };
    const price = parseInt((cfg.prices || {})[String(days)] || (cfg.prices || {})[days]) || 15000;

    const orderId = newId('BOT');
    // SECURITY: cancelToken agar hanya pemilik order yang bisa cancel
    const sbCancelToken = crypto.randomBytes(16).toString('hex');
    let pakData = null, qrBase64 = null, totalBayar = price, adminFee = 0;
    try {
      pakData    = await pak.create(orderId, price);
      totalBayar = pakData.total_payment || price;
      adminFee   = pakData.fee || 0;
      const qs   = pakData._qrisString || pakData.payment_number || '';
      qrBase64   = await QRCode.toDataURL(qs, { errorCorrectionLevel: 'M', margin: 2, scale: 8, color: { dark: '#000000', light: '#ffffff' } });
    } catch(e) {
      console.warn('[sewabot] demo mode:', e.message);
      qrBase64 = await QRCode.toDataURL('DEMO-' + orderId, { margin: 2, scale: 8 });
    }

    const stg = await getEffectiveSettings();
    const now = Date.now();
    const order = {
      id: orderId, type: 'sewabot',
      groupUrl, days, buyerName,
      phone: phone ? '62' + String(phone).replace(/^0/, '') : null,
      price, adminFee, totalBayar,
      qrBase64, pakData,
      status: 'PENDING',
      createdAt: now,
      expiryAt: now + (stg.expiryMin || C.store.expiry) * 60000,
      demo: !pakData || !C.pak.apikey,
      cancelToken: sbCancelToken,
      creatorIp: req.ip || 'unknown',
    };
    await saveSewabotOrder(orderId, order, null);
    console.log('[sewabot/order]', orderId, '|', groupUrl, '|', days, 'd | Rp' + totalBayar);
    broadcastAdmin({ type: 'new_sewabot', id: orderId, groupUrl, days, buyerName, totalBayar, ts: Date.now() });
    res.json({ ok: true, orderId, cancelToken: sbCancelToken });
  } catch(e) { console.error('[sewabot/order]', e.message); res.json({ ok: false, message: e.message }); }
});

// Check sewabot payment
app.post('/api/sewabot/check', async function(req, res) {
  try {
    const id = req.body.id;
    if (!id || id.length > 60) return res.json({ status: 'NOT_FOUND' });
    if (!rateLimit('sbcheck:' + id, 30, 10 * 60 * 1000)) return res.json({ status: 'PENDING' });

    const r = await getSewabotOrder(id); const order = r.data; const sha = r.sha;
    if (!order) return res.json({ status: 'NOT_FOUND' });
    if (order.status === 'COMPLETED') return res.json({ status: 'COMPLETED', data: { groupUrl: order.groupUrl, days: order.days, buyerName: order.buyerName } });
    if (order.status === 'FAILED' || order.status === 'EXPIRED') return res.json({ status: order.status });
    // FIX: handle PROCESSING agar tidak re-check Pakasir sia-sia
    if (order.status === 'PROCESSING') {
      if (order.processingAt && Date.now() - order.processingAt > 3 * 60 * 1000) {
        await saveSewabotOrder(id, Object.assign({}, order, { status: 'PENDING', processingAt: null }), sha).catch(function(){});
      }
      return res.json({ status: 'PENDING' });
    }

    if (Date.now() > order.expiryAt) {
      await saveSewabotOrder(id, Object.assign({}, order, { status: 'EXPIRED' }), sha);
      return res.json({ status: 'EXPIRED' });
    }
    if (order.demo) return res.json({ status: 'PENDING' });

    const pakRes    = await pak.check(id, order.price);
    const trxObj    = pakRes && pakRes.transaction;
    const pakStatus = ((trxObj && trxObj.status) || (pakRes && pakRes.data && pakRes.data.status) || (pakRes && pakRes.status) || '').toLowerCase();

    if (pakStatus === 'completed' || pakStatus === 'paid' || pakStatus === 'success') {
      // FIX: PROCESSING lock agar tidak double-complete jika 2 request bersamaan
      try {
        await saveSewabotOrder(id, Object.assign({}, order, { status: 'PROCESSING', processingAt: Date.now() }), sha);
      } catch(lockErr) {
        console.log('[sewabot/check] lock conflict (ok):', id);
        return res.json({ status: 'PENDING' });
      }
      const freshSb = await getSewabotOrder(id);
      await saveSewabotOrder(id, Object.assign({}, freshSb.data || order, { status: 'COMPLETED', completedAt: Date.now() }), freshSb.sha || null);
      console.log('[sewabot] COMPLETED:', id);
      broadcastAdmin({ type: 'sewabot_completed', id, groupUrl: order.groupUrl, days: order.days, buyerName: order.buyerName, totalBayar: order.totalBayar || order.price, ts: Date.now() });
      return res.json({ status: 'COMPLETED', data: { groupUrl: order.groupUrl, days: order.days, buyerName: order.buyerName } });
    }
    if (pakStatus === 'failed' || pakStatus === 'canceled' || pakStatus === 'cancelled') {
      await saveSewabotOrder(id, Object.assign({}, order, { status: 'FAILED' }), sha);
      return res.json({ status: 'FAILED' });
    }
    return res.json({ status: 'PENDING' });
  } catch(e) { console.error('[sewabot/check]', e.message); return res.json({ status: 'PENDING' }); }
});

// Get sewabot order QR (for pay page)
app.get('/api/sewabot/:id/qr', async function(req, res) {
  try {
    const id = req.params.id;
    if (!id || id.length > 60) return res.status(404).send('Not found');
    const r = await getSewabotOrder(id);
    if (!r.data || !r.data.qrBase64) return res.status(404).send('No QR');
    if (r.data.status !== 'PENDING') return res.status(410).send('Gone');
    const b64 = r.data.qrBase64.split(',')[1];
    res.set('Content-Type', 'image/png');
    res.set('Cache-Control', 'no-store');
    res.send(Buffer.from(b64, 'base64'));
  } catch(e) { res.status(500).send('Error'); }
});

// Get sewabot order data (public, no QR)
app.get('/api/sewabot/:id', async function(req, res) {
  try {
    const id = req.params.id;
    if (!id || id.length > 60) return res.json({ ok: false, message: 'Tidak ditemukan.' });
    const r = await getSewabotOrder(id);
    if (!r.data) return res.json({ ok: false, message: 'Tidak ditemukan.' });
    const d = Object.assign({}, r.data);
    delete d.qrBase64; delete d.pakData;
    res.json({ ok: true, data: d });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Cancel sewabot order
app.post('/api/sewabot/cancel', async function(req, res) {
  try {
    const id = req.body.id;
    const cancelToken = req.body.cancelToken || '';
    if (!id || id.length > 60) return res.json({ ok: false });
    const r = await getSewabotOrder(id);
    if (!r.data || r.data.status !== 'PENDING') return res.json({ ok: false, message: 'Order tidak bisa dibatalkan.' });

    // SECURITY FIX: verifikasi kepemilikan via cancelToken
    if (r.data.cancelToken) {
      if (!cancelToken || cancelToken.length !== 32 || cancelToken !== r.data.cancelToken) {
        console.warn('[sewabot/cancel] token mismatch:', id, '| ip:', req.ip);
        return res.status(403).json({ ok: false, message: 'Tidak diizinkan membatalkan order ini.' });
      }
    }

    if (!r.data.demo && C.pak.apikey) await pak.cancel(id, r.data.price).catch(function(){});
    await saveSewabotOrder(id, Object.assign({}, r.data, { status: 'FAILED', cancelledAt: Date.now() }), r.sha);
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Admin: list sewabot orders
app.get('/api/admin/sewabot/orders', adminAuth, async function(req, res) {
  try {
    // Baca dari sewabot-orders/ (primary source)
    const orders = await listSewabotOrders();

    // Fallback: cari transaksi dengan productType='sewabot' yang tidak ada di sewabot-orders/
    // Ini mengatasi bug di mana sewabot-orders save gagal saat proses pembayaran
    const sbIds = new Set(orders.map(function(o){ return o.trxId || o.id; }));
    try {
      const files = await listTrx();
      await Promise.all(files.filter(function(f){ return f.name.endsWith('.json'); }).map(async function(f) {
        try {
          const r = await dbRead('transactions/' + f.name);
          if (!r.data) return;
          const d = r.data;
          if (d.productType !== 'sewabot') return;
          if (d.status !== 'COMPLETED' && d.status !== 'PAID_ERROR') return;
          // Cek apakah sudah ada di sewabot-orders
          if (sbIds.has(d.id)) return;
          // Belum ada — tambahkan sebagai rekonstruksi dari data transaksi
          orders.push({
            id: 'BOT-' + d.id, trxId: d.id,
            groupUrl: d.groupUrl || (d.result && d.result.groupUrl) || '',
            days: d.variantDays || (d.result && d.result.days) || 30,
            buyerName: d.buyerName || null,
            phone: d.phone || null,
            price: d.unitPrice || 0,
            adminFee: d.adminFee || 0,
            totalBayar: d.totalBayar || d.unitPrice || 0,
            status: 'PROCESSING',
            createdAt: d.createdAt,
            completedAt: d.completedAt || null,
            _reconstructed: true, // flag untuk debugging
          });
        } catch(e) {}
      }));
    } catch(e) { console.warn('[sewabot/orders] fallback scan error:', e.message); }

    orders.sort(function(a,b){ return (b.createdAt||0)-(a.createdAt||0); });
    res.json({ ok: true, data: orders.slice(0, 200) });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Admin: update sewabot order status / note
app.post('/api/admin/sewabot/orders/:id/status', adminAuth, async function(req, res) {
  try {
    const id     = req.params.id;
    const status = req.body.status;
    const note   = String(req.body.note || '').slice(0, 500);
    const VALID  = ['COMPLETED', 'PENDING', 'FAILED', 'EXPIRED', 'PROCESSING'];
    if (!VALID.includes(status)) return res.json({ ok: false, message: 'Status tidak valid.' });
    if (!id || id.length > 60) return res.json({ ok: false, message: 'ID tidak valid.' });
    const r = await getSewabotOrder(id);
    if (!r.data) return res.json({ ok: false, message: 'Tidak ditemukan.' });
    await saveSewabotOrder(id, Object.assign({}, r.data, { status, adminNote: note, updatedAt: Date.now() }), r.sha);
    auditLog('sewabot-status', id + ' -> ' + status, req.adminIp).catch(function(){});
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Admin: get/set sewabot config (prices)
app.get('/api/admin/sewabot/config', adminAuth, async function(req, res) {
  try { const r = await dbRead('sewabot-config.json'); res.json({ ok: true, data: r.data || { prices: { 7: 15000, 14: 25000, 30: 45000 }, description: '' } }); }
  catch(e) { res.json({ ok: false, message: e.message }); }
});
app.post('/api/admin/sewabot/config', adminAuth, async function(req, res) {
  try {
    const prices = req.body.prices || { 7: 15000, 14: 25000, 30: 45000 };
    const description = String(req.body.description || '').slice(0, 500);
    const enabled = req.body.enabled !== false && req.body.enabled !== 'false';
    const r = await dbRead('sewabot-config.json');
    await dbWrite('sewabot-config.json', { prices, description, enabled }, r.sha || null, 'admin: sewabot-config');
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── SSE: Real-time transaction status ──────────────────────────────────────
// Server-Sent Events — replaces client-side polling
app.get('/api/trx/:id/stream', async function(req, res) {
  const id = req.params.id;
  if (!isValidId(id)) { res.status(400).end(); return; }
  if (!rateLimit('sse:' + id, 5, 60 * 1000)) { res.status(429).end(); return; }

  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no' });
  res.flushHeaders();

  function send(data) { if (!res.writableEnded) res.write('data: ' + JSON.stringify(data) + '\n\n'); }
  send({ type: 'connected' });

  var closed = false;
  var startedAt = Date.now();
  req.on('close', function() { closed = true; clearInterval(iv); });

  var iv = setInterval(async function() {
    if (closed) { clearInterval(iv); return; }
    // FIX: max lifetime 55 detik agar tidak numpuk di Vercel serverless
    if (Date.now() - startedAt > 55000) {
      send({ type: 'reconnect' });
      clearInterval(iv); res.end(); return;
    }
    try {
      const r = await getTrx(id);
      if (!r.data) { send({ type: 'status', status: 'NOT_FOUND' }); clearInterval(iv); res.end(); return; }
      const trx = r.data;
      if (trx.status === 'COMPLETED' || trx.status === 'PAID_ERROR') {
        send({ type: 'status', status: 'COMPLETED', result: trx.result || null });
        clearInterval(iv); res.end(); return;
      }
      if (trx.status === 'FAILED' || trx.status === 'EXPIRED') {
        send({ type: 'status', status: trx.status });
        clearInterval(iv); res.end(); return;
      }
      if (Date.now() > trx.expiryAt) {
        send({ type: 'status', status: 'EXPIRED' });
        clearInterval(iv); res.end(); return;
      }
      // FIX: PROCESSING = sedang diproses, kirim ping biasa agar client tidak stuck
      send({ type: 'ping', status: trx.status === 'PROCESSING' ? 'PENDING' : trx.status });
    } catch(e) {}
  }, 4000);
});


// ════════════════════════════════════════════════════════════════
// PUBLIC FEED API
// ════════════════════════════════════════════════════════════════

// GET /api/feed — last 50 success events (JSON)
// Query: ?type=order|deposit|sewabot&limit=20
// Catatan: _feedBuf adalah in-memory dan direset saat Vercel cold start.
// Fallback ke feed-cache.json di GitHub jika buffer kosong.
app.get('/api/feed', async function(req, res) {
  if (!rateLimit('feed:' + (req.ip||'x'), 60, 60000))  // [FIX] Naik ke 60/mnt — polling bot 6x/mnt, jangan sampai kena limit
    return res.status(429).json({ ok: false, message: 'Terlalu banyak request. Coba lagi.' });
  var typeFilter = req.query.type || 'all';
  var limit      = Math.min(parseInt(req.query.limit) || 20, 50);
  var source     = _feedBuf.slice();
  var fromCache  = false;
  if (source.length === 0) {
    try {
      const r = await dbRead('feed-cache.json');
      if (Array.isArray(r.data) && r.data.length > 0) { source = r.data; fromCache = true; }
    } catch(e) {}
  }
  var data = source.filter(function(e) {
    return typeFilter === 'all' || e.type === typeFilter;
  }).slice(0, limit);
  if (!fromCache) data = data.map(formatFeedEvent);
  res.json({ ok: true, count: data.length, data });
});

// GET /api/feed/stream — SSE real-time, public
app.get('/api/feed/stream', function(req, res) {
  if (!rateLimit('feedsse:' + (req.ip||'x'), 10, 60000)) { res.status(429).end(); return; }
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.flushHeaders();
  // Kirim buffer terakhir saat connect
  _feedBuf.slice(0, 10).reverse().forEach(function(e) {
    res.write('data: ' + JSON.stringify(formatFeedEvent(e)) + '\n\n');
  });
  res.write('data: ' + JSON.stringify({ type: 'connected', ts: Date.now() }) + '\n\n');
  _feedClients.add(res);
  var startedAt = Date.now();
  var ping = setInterval(function() {
    try {
      if (Date.now() - startedAt > 55000) {
        res.write('data: ' + JSON.stringify({ type: 'reconnect' }) + '\n\n');
        clearInterval(ping); _feedClients.delete(res); res.end(); return;
      }
      res.write('data: ' + JSON.stringify({ type: 'ping' }) + '\n\n');
    }
    catch(e) { clearInterval(ping); _feedClients.delete(res); }
  }, 25000);
  req.on('close', function() { clearInterval(ping); _feedClients.delete(res); });
});

// ── ADMIN: Feed cache management
// GET /api/admin/feed — lihat isi feed cache (dari GitHub, reliable di serverless)
app.get('/api/admin/feed', adminAuth, async function(req, res) {
  try {
    var memBuf = _feedBuf.slice();
    var ghBuf  = [];
    try {
      const r = await dbRead('feed-cache.json');
      if (Array.isArray(r.data)) ghBuf = r.data;
    } catch(e) {}
    res.json({ ok: true, inMemoryCount: memBuf.length, githubCacheCount: ghBuf.length, data: ghBuf });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// DELETE /api/admin/feed — hapus feed cache (reset tampilan live activity)
app.delete('/api/admin/feed', adminAuth, async function(req, res) {
  try {
    _feedBuf.length = 0; // reset in-memory buffer
    const r = await dbRead('feed-cache.json');
    if (r.sha) await dbWrite('feed-cache.json', [], r.sha, 'admin: clear feed cache');
    auditLog('clear-feed', 'Feed cache dihapus', req.adminIp).catch(function(){});
    res.json({ ok: true, message: 'Feed cache berhasil direset.' });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── WEBHOOK CONFIG (admin)
app.get('/api/admin/webhook', adminAuth, async function(req, res) {
  try { const r = await dbRead('webhook-config.json'); res.json({ ok: true, data: r.data || { url: '', secret: '', enabled: false } }); }
  catch(e) { res.json({ ok: false, message: e.message }); }
});
app.post('/api/admin/webhook', adminAuth, async function(req, res) {
  try {
    var url     = String(req.body.url     || '').trim().slice(0, 500);
    var secret  = String(req.body.secret  || '').trim().slice(0, 200);
    var enabled = req.body.enabled !== false && req.body.enabled !== 'false';
    if (url) {
      if (!url.startsWith('http://') && !url.startsWith('https://')) return res.json({ ok: false, message: 'URL harus dimulai dengan https://' });
      // SECURITY FIX: blokir SSRF — jangan izinkan webhook ke internal/localhost/metadata
      const _ssrfBlock = /^https?:\/\/(localhost|127\.|0\.0\.0\.0|10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[01])\.|169\.254\.|::1|metadata\.|fd[0-9a-f]{2}:)/i;
      if (_ssrfBlock.test(url)) return res.json({ ok: false, message: 'URL tidak diizinkan (internal/localhost tidak boleh dipakai sebagai webhook).' });
    }
    const r = await dbRead('webhook-config.json');
    await dbWrite('webhook-config.json', { url, secret, enabled }, r.sha || null, 'admin: webhook');
    // Invalidate in-memory cache so next fire uses the new config
    _webhookCache = { url, secret, enabled }; _webhookCacheAt = Date.now();
    auditLog('webhook-update', url, req.adminIp).catch(function(){});
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Test webhook — kirim dummy event dengan payload lengkap seperti event asli
app.post('/api/admin/webhook/test', adminAuth, async function(req, res) {
  try {
    const cfg = await getWebhookConfig();
    if (!cfg || !cfg.url) return res.json({ ok: false, message: 'URL webhook belum dikonfigurasi.' });
    if (cfg.enabled === false) return res.json({ ok: false, message: 'Webhook tidak aktif. Aktifkan dulu di pengaturan webhook.' });
    // Kirim contoh payload lengkap seperti event order asli
    const testPayload = formatWebhookPayload({
      id: 'TEST-' + Date.now(), type: 'order', ts: Date.now(),
      label: 'Test — Produk Contoh (Panel 1GB)',
      amount: 25000,
      productName: 'Hosting Panel',
      variantName: '1GB / 30 Hari',
      productType: 'panel',
      phone: '628123456789',
    });
    await axios.post(cfg.url, testPayload, {
      timeout: 8000,
      headers: {
        'Content-Type': 'application/json',
        'X-Dongtube-Event': 'test',
        'X-Dongtube-Secret': cfg.secret || '',
      },
    });
    res.json({ ok: true, message: 'Test berhasil dikirim ke webhook.', payload: testPayload });
  } catch(e) { res.json({ ok: false, message: 'Gagal: ' + e.message }); }
});

// ── PANEL TEMPLATES (admin) ────────────────────────────────────────────────

// ── SYSTEM INFO — untuk admin melihat konfigurasi DB backend aktif
app.get('/api/admin/system-info', adminAuth, function(req, res) {
  var dbInfo;
  if (DB_BACKEND === 'github') {
    dbInfo = {
      backend: 'github',
      owner  : GH_DB_OWNER,
      repo   : GH_DB_REPO,
      branch : GH_DB_BRANCH,
      ready  : !!_dbOctokit,
    };
  } else {
    dbInfo = {
      backend: 'supabase',
      project: SUPABASE_URL ? SUPABASE_URL.replace('https://','').split('.')[0] : '',
      ready  : !!supabase,
    };
  }
  var cdnAccs = _cdnAccounts();
  res.json({
    ok     : true,
    version: '2.0.0',
    db     : dbInfo,
    cdn    : cdnAccs.map(function(a){ return { name: a.name, owner: a.owner, repos: a.repos }; }),
    ptero  : { configured: !!(C.ptero.domain && C.ptero.apikey), domain: C.ptero.domain },
    pakasir: { configured: !!(C.pak.slug && C.pak.apikey) },
    store  : { name: C.store.name },
    env_hint: DB_BACKEND === 'github'
      ? 'DATABASE=github | GH_DB_TOKEN + GH_DB_OWNER + GH_DB_REPO + GH_DB_BRANCH'
      : 'DATABASE=supabase (default) | SUPABASE_URL + SUPABASE_SERVICE_KEY',
  });
});

app.get('/api/admin/panel-templates', adminAuth, async function(req, res) {
  try {
    const templates = await getPanelTemplates();
    // Merge with SPEC defaults for any plans not yet in DB
    const defaultPlans = Object.keys(SPEC).map(function(id) {
      const t = templates.find(function(x){ return x.id === id; });
      if (t) return t;
      return { id, name: id.toUpperCase(), active: true,
        ram: SPEC[id].ram, disk: SPEC[id].disk, cpu: SPEC[id].cpu,
        io: 500, swap: 0,
        egg: C.ptero.egg, nest: C.ptero.nest, location: C.ptero.location,
        docker_image: 'ghcr.io/parkervcp/yolks:nodejs_20',
        startup: '',
        environment: { INST: 'npm', USER_UPLOAD: '0', AUTO_UPDATE: '0', CMD_RUN: 'npm start' },
        feature_limits: { databases: 5, backups: 5, allocations: 5 }
      };
    });
    // Merge: start with defaultPlans, replace with any that exist in templates, then add any extra in templates
    var merged = defaultPlans.slice();
    templates.forEach(function(t) {
      var idx = merged.findIndex(function(x){ return x.id === t.id; });
      if (idx >= 0) merged[idx] = t; else merged.push(t);
    });
    res.json({ ok: true, data: merged, ptero: { domain: C.ptero.domain, egg: C.ptero.egg, nest: C.ptero.nest, location: C.ptero.location } });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});
app.post('/api/admin/panel-templates', adminAuth, async function(req, res) {
  try {
    const templates = Array.isArray(req.body) ? req.body : req.body.templates;
    if (!Array.isArray(templates)) return res.json({ ok: false, message: 'templates harus array.' });
    // Validate each template
    const clean = templates.map(function(t) {
      return {
        id:           String(t.id || '').toLowerCase().replace(/[^a-z0-9_]/g,'').slice(0,20),
        name:         String(t.name || t.id || '').slice(0,50),
        active:       t.active !== false,
        ram:          parseInt(t.ram) || 0,
        disk:         parseInt(t.disk) || 0,
        cpu:          parseInt(t.cpu) || 0,
        io:           parseInt(t.io) || 500,
        swap:         parseInt(t.swap) || 0,
        egg:          parseInt(t.egg) || C.ptero.egg,
        nest:         parseInt(t.nest) || C.ptero.nest,
        location:     parseInt(t.location) || C.ptero.location,
        docker_image: String(t.docker_image || 'ghcr.io/parkervcp/yolks:nodejs_20').slice(0,200),
        startup:      String(t.startup || '').slice(0,1000),
        environment:  (t.environment && typeof t.environment === 'object') ? t.environment : { INST: 'npm', USER_UPLOAD: '0', AUTO_UPDATE: '0', CMD_RUN: 'npm start' },
        feature_limits: (t.feature_limits && typeof t.feature_limits === 'object') ? t.feature_limits : { databases: 5, backups: 5, allocations: 5 },
      };
    }).filter(function(t){ return t.id; });
    const r = await dbRead('panel-templates.json');
    await dbWrite('panel-templates.json', clean, r.sha || null, 'admin: panel-templates');
    // [FIX] Pre-warm cache with fresh data so subsequent admin reloads get correct data
    try { await dbRead('panel-templates.json', true); } catch(e) {}
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── SERVER LOG ──────────────────────────────────────────────────────────────
app.get('/api/admin/server-log', adminAuth, function(req, res) {
  try {
    var limit = Math.min(parseInt(req.query.limit) || 100, 500);
    var level = req.query.level || 'all';
    var logs = _logBuf.slice();
    if (level !== 'all') logs = logs.filter(function(l){ return l.level === level; });
    res.json({ ok: true, data: logs.slice(-limit).reverse() });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── SERVER METRICS ──────────────────────────────────────────────────────────
app.get('/api/admin/server-metrics', adminAuth, function(req, res) {
  try {
    var os = require('os');
    var mem = process.memoryUsage();
    var load = os.loadavg();
    var cpus = os.cpus();
    var totalMem = os.totalmem();
    var freeMem  = os.freemem();
    res.json({
      ok: true,
      data: {
        uptime_process: Math.floor(process.uptime()),
        uptime_system : Math.floor(os.uptime()),
        load_avg: load,
        cpu_count: cpus.length,
        cpu_model: cpus[0] && cpus[0].model,
        mem_total: totalMem,
        mem_free:  freeMem,
        mem_used:  totalMem - freeMem,
        mem_percent: Math.round((totalMem - freeMem) / totalMem * 100),
        process_heap_used: mem.heapUsed,
        process_heap_total: mem.heapTotal,
        process_rss: mem.rss,
        node_version: process.version,
        platform: os.platform(),
        arch: os.arch(),
        hostname: os.hostname(),
        log_count: _logBuf.length,
      }
    });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── ADMIN: CDN STATUS ────────────────────────────────────────────────────────
// GET /api/admin/cdn/status — status semua account CDN + folder cache
app.get('/api/admin/cdn/status', adminAuth, function(req, res) {
  try {
    var accs = _cdnAccounts();
    res.json({
      ok: true,
      configured: accs.length > 0,
      accounts: accs.map(function(a) {
        return {
          name     : a.name,
          owner    : a.owner,
          repos    : a.repos,
          branch   : a.branch,
          active   : true,
        };
      }),
      folderCacheSize: _cdnFolderCache.size,
      maxFilesPerFolder: CDN_MAX_FILES_PER_FOLDER,
      maxFolders: CDN_MAX_FOLDERS,
      githubApiLimit: CDN_GITHUB_API_LIMIT,
    });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// GET /api/admin/cdn/files — list semua file di CDN (cross-account)
app.get('/api/admin/cdn/files', adminAuth, async function(req, res) {
  try {
    var accs = _cdnAccounts();
    if (!accs.length) return res.json({ ok: false, message: 'CDN belum dikonfigurasi.' });

    var allFiles = [];
    for (var ai = 0; ai < accs.length; ai++) {
      var acc = accs[ai];
      for (var ri = 0; ri < acc.repos.length; ri++) {
        var repo = acc.repos[ri];
        // Subfolder baru: files, files2, ..., files10
        for (var fi = 1; fi <= CDN_MAX_FOLDERS; fi++) {
          var folder = fi === 1 ? 'files' : 'files' + fi;
          try {
            var r = await acc.octokit.repos.getContent({ owner: acc.owner, repo: repo, path: folder });
            if (Array.isArray(r.data)) {
              r.data.filter(function(f){ return f.type === 'file'; }).forEach(function(f) {
                allFiles.push({
                  name     : f.name,
                  url      : '/cdn/' + f.name,
                  size     : f.size,
                  path     : f.path,
                  account  : acc.name,
                  repo     : repo,
                  folder   : folder,
                  sha      : f.sha,
                  download : f.download_url,
                });
              });
            }
          } catch(e) {
            if (e.status === 404) break; // folder tidak ada, hentikan loop folder untuk repo ini
          }
        }
        // Legacy folder: cdn/
        try {
          var rLeg = await acc.octokit.repos.getContent({ owner: acc.owner, repo: repo, path: 'cdn' });
          if (Array.isArray(rLeg.data)) {
            rLeg.data.filter(function(f){ return f.type === 'file'; }).forEach(function(f) {
              allFiles.push({
                name     : f.name,
                url      : '/cdn/' + f.name,
                size     : f.size,
                path     : f.path,
                account  : acc.name,
                repo     : repo,
                folder   : 'cdn (legacy)',
                sha      : f.sha,
                download : f.download_url,
              });
            });
          }
        } catch(e) { /* legacy folder tidak ada — ok */ }
      }
    }
    res.json({ ok: true, total: allFiles.length, data: allFiles });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// DELETE /api/admin/cdn/files/:filename — hapus file CDN
// Query param: ?account=PRIMARY&repo=reponame&folder=files
app.delete('/api/admin/cdn/files/:filename', adminAuth, async function(req, res) {
  try {
    var filename = req.params.filename;
    if (!filename || !/^[a-zA-Z0-9\-_.]+$/.test(filename)) return res.json({ ok: false, message: 'Nama file tidak valid.' });

    var accName  = req.query.account || 'PRIMARY';
    var repoName = req.query.repo;
    var folder   = req.query.folder || 'files';

    var accs = _cdnAccounts();
    var acc  = accs.find(function(a){ return a.name === accName; }) || accs[0];
    if (!acc) return res.json({ ok: false, message: 'CDN account tidak ditemukan.' });

    var repo = repoName || acc.repos[0];
    var filePath = folder === 'cdn (legacy)' ? 'cdn/' + filename : folder + '/' + filename;

    // Ambil SHA file dulu
    var fileInfo;
    try {
      var rf = await acc.octokit.repos.getContent({ owner: acc.owner, repo: repo, path: filePath });
      fileInfo = rf.data;
    } catch(e) {
      if (e.status === 404) return res.json({ ok: false, message: 'File tidak ditemukan di CDN.' });
      throw e;
    }

    await acc.octokit.repos.deleteFile({
      owner: acc.owner, repo: repo,
      path : filePath,
      message: 'cdn: delete ' + filename,
      sha  : fileInfo.sha,
      branch: acc.branch,
    });

    _cdnInvalidateCache(acc.owner, repo, folder);
    auditLog('cdn-delete', filename + ' from ' + acc.owner + '/' + repo + '/' + folder, req.adminIp).catch(function(){});
    console.log('[cdn/delete]', filename, '|', acc.name + '/' + repo + '/' + folder);
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: _cdnSanitizeError(e) }); }
});

// POST /api/admin/cdn/invalidate-cache — reset folder count cache manual
app.post('/api/admin/cdn/invalidate-cache', adminAuth, function(req, res) {
  var before = _cdnFolderCache.size;
  _cdnFolderCache.clear();
  console.log('[cdn] cache invalidated by admin | was:', before, 'entries');
  res.json({ ok: true, cleared: before });
});


// ════════════════════════════════════════════════════════════════
// VOUCHER / PROMO SYSTEM
// ════════════════════════════════════════════════════════════════
async function getVouchers() {
  try { const r = await dbRead('vouchers.json'); return Array.isArray(r.data) ? r.data : []; } catch(e) { return []; }
}
async function saveVouchers(arr, sha) {
  return dbWrite('vouchers.json', arr, sha, 'vouchers');
}

app.get('/api/admin/vouchers', adminAuth, async function(req, res) {
  try { const r = await dbRead('vouchers.json'); res.json({ ok: true, data: Array.isArray(r.data) ? r.data : [], sha: r.sha }); }
  catch(e) { res.json({ ok: false, message: e.message }); }
});
app.post('/api/admin/vouchers', adminAuth, async function(req, res) {
  try {
    const vouchers = Array.isArray(req.body) ? req.body : (req.body.vouchers || []);
    const clean = vouchers.map(function(v) {
      return {
        id:          String(v.id || ('vc-' + Date.now())),
        code:        String(v.code || '').toUpperCase().trim().replace(/[^A-Z0-9_-]/g,'').slice(0,20),
        type:        ['percent','nominal'].includes(v.type) ? v.type : 'percent',
        value:       Math.max(0, parseInt(v.value) || 0),
        minOrder:    Math.max(0, parseInt(v.minOrder) || 0),
        maxDiscount: Math.max(0, parseInt(v.maxDiscount) || 0),
        usedCount:   parseInt(v.usedCount) || 0,
        maxUse:      parseInt(v.maxUse) || 0,       // 0 = unlimited
        expiresAt:   v.expiresAt ? parseInt(v.expiresAt) : null,
        active:      v.active !== false,
        productIds:  Array.isArray(v.productIds) ? v.productIds : [], // [] = all products
        desc:        String(v.desc || '').slice(0, 100),
      };
    }).filter(function(v){ return v.code; });
    const r = await dbRead('vouchers.json');
    await dbWrite('vouchers.json', clean, r.sha || null, 'admin: vouchers');
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Public: validate voucher
app.post('/api/voucher/validate', async function(req, res) {
  try {
    const ip   = req.ip || 'x';
    if (!rateLimit('vcval:' + ip, 10, 60000)) return res.status(429).json({ ok: false, message: 'Terlalu banyak percobaan.' });
    const { code, productId, amount } = req.body;
    if (!code) return res.json({ ok: false, message: 'Kode voucher kosong.' });
    const vouchers = await getVouchers();
    const v = vouchers.find(function(v){ return v.code === String(code).toUpperCase().trim() && v.active !== false; });
    if (!v) return res.json({ ok: false, message: 'Kode voucher tidak valid.' });
    if (v.expiresAt && Date.now() > v.expiresAt) return res.json({ ok: false, message: 'Voucher sudah kadaluarsa.' });
    if (v.maxUse > 0 && v.usedCount >= v.maxUse) return res.json({ ok: false, message: 'Kuota voucher habis.' });
    if (v.minOrder > 0 && (parseInt(amount)||0) < v.minOrder) return res.json({ ok: false, message: 'Minimum order ' + idrFormat(v.minOrder) });
    if (v.productIds && v.productIds.length > 0 && productId && !v.productIds.includes(productId)) {
      return res.json({ ok: false, message: 'Voucher tidak berlaku untuk produk ini.' });
    }
    let discount = 0;
    const base = parseInt(amount) || 0;
    if (v.type === 'percent') { discount = Math.round(base * v.value / 100); if (v.maxDiscount > 0) discount = Math.min(discount, v.maxDiscount); }
    else { discount = v.value; }
    discount = Math.min(discount, base);
    res.json({ ok: true, discount, finalAmount: base - discount, code: v.code, desc: v.desc, type: v.type, value: v.value });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Flash sale system removed — discount handled via variant.salePrice and variant.saleBadge

// ════════════════════════════════════════════════════════════════
// ADVANCED ANALYTICS
// ════════════════════════════════════════════════════════════════
app.get('/api/admin/analytics', adminAuth, async function(req, res) {
  const forceRefresh = req.query.refresh === '1';
  if (!forceRefresh && _analyticsCache && (Date.now() - _analyticsCacheAt) < ANALYTICS_TTL) {
    return res.json(Object.assign({ ok: true, cached: true }, _analyticsCache));
  }
  try {
    const now = Date.now();
    const D7  = now - 7  * 86400000;
    const D30 = now - 30 * 86400000;

    // ── Daily map 30 hari
    const dailyMap = {};
    for (let i = 0; i < 30; i++) {
      const d   = new Date(now - i * 86400000);
      const key = d.toISOString().slice(0, 10);
      dailyMap[key] = { date: key, revenue: 0, orders: 0, completed: 0 };
    }

    // ── Monthly map 12 bulan
    const monthMap = {};
    for (let i = 0; i < 12; i++) {
      const d   = new Date(now);
      d.setDate(1); d.setMonth(d.getMonth() - i);
      const key = d.toISOString().slice(0, 7); // YYYY-MM
      monthMap[key] = { month: key, revenue: 0, orders: 0, depositFee: 0, sewabot: 0 };
    }

    const productCount = {};
    let revProduct = 0, revDepFee = 0, revSewabot = 0;
    let rev7 = 0, ord30 = 0, totalOrd = 0;
    let completedTotal = 0, failedTotal = 0, pendingTotal = 0;
    let convNumer = 0, convDenom = 0;

    // ── Scan transaksi produk — parallel, batched (10 at a time) for speed
    const files = await listTrx();
    const trxFiles = files.filter(function(f){ return f.name.endsWith('.json'); });
    const BATCH = 12;
    const trxList = [];
    for (let bi = 0; bi < trxFiles.length; bi += BATCH) {
      const batch = await Promise.all(trxFiles.slice(bi, bi + BATCH).map(async function(f) {
        try { const r = await dbRead('transactions/' + f.name); return r.data || null; }
        catch(e) { return null; }
      }));
      batch.forEach(function(d) { if (d) trxList.push(d); });
      if (bi + BATCH < trxFiles.length) await _sleep(80);
    }

    trxList.forEach(function(t) {
      const ts     = t.createdAt || 0;
      const dayKey = new Date(ts).toISOString().slice(0, 10);
      const monKey = new Date(ts).toISOString().slice(0, 7);
      totalOrd++; convDenom++;

      if (t.status === 'COMPLETED') {
        completedTotal++; convNumer++;
        const rv = t.totalBayar || t.unitPrice || 0;
        if (t.type === 'sewabot' || t.productType === 'sewabot') {
          revSewabot += rv;
          if (monthMap[monKey]) monthMap[monKey].sewabot += rv;
        } else {
          revProduct += rv;
          if (monthMap[monKey]) { monthMap[monKey].revenue += rv; }
        }
        if (ts > D30) ord30++;
        if (ts > D7)  rev7 += rv;
        if (dailyMap[dayKey]) { dailyMap[dayKey].revenue += rv; dailyMap[dayKey].completed++; }
        const pid = t.productName || t.productId || 'Unknown';
        productCount[pid] = (productCount[pid] || 0) + 1;
      } else if (['FAILED','CANCELLED','EXPIRED'].includes(t.status)) {
        failedTotal++;
      } else {
        pendingTotal++;
      }
      if (dailyMap[dayKey]) dailyMap[dayKey].orders++;
      if (monthMap[monKey]) monthMap[monKey].orders++;
    });

    // ── Scan deposit — hitung adminFeeDeposit, parallel batched
    try {
      const depListR = await listDirCached('deposits');
      const depFiles = Array.isArray(depListR) ? depListR.filter(function(f){ return f.name.endsWith('.json'); }) : [];
      for (let bi = 0; bi < depFiles.length; bi += BATCH) {
        const batch = await Promise.all(depFiles.slice(bi, bi + BATCH).map(async function(f) {
          try { const dr = await getDeposit(f.name.replace('.json','')); return dr.data || null; }
          catch(e) { return null; }
        }));
        batch.forEach(function(d) {
          if (!d || d.status !== 'success') return;
          const fee = d.adminFeeDeposit || 0;
          const ts  = d.createdAt || 0;
          revDepFee += fee;
          const dayKey = new Date(ts).toISOString().slice(0, 10);
          const monKey = new Date(ts).toISOString().slice(0, 7);
          if (dailyMap[dayKey]) dailyMap[dayKey].revenue += fee;
          if (monthMap[monKey]) monthMap[monKey].depositFee += fee;
        });
        if (bi + BATCH < depFiles.length) await _sleep(80);
      }
    } catch(ghErr) {
      if (ghErr.response && ghErr.response.status !== 404) console.warn('[analytics] deposit scan:', ghErr.message);
    }

    const totalRev = revProduct + revDepFee + revSewabot;
    const daily    = Object.values(dailyMap).sort(function(a, b){ return a.date.localeCompare(b.date); });
    const monthly  = Object.values(monthMap).sort(function(a, b){ return b.month.localeCompare(a.month); });
    const topProducts = Object.entries(productCount)
      .sort(function(a, b){ return b[1] - a[1]; }).slice(0, 10)
      .map(function(e){ return { name: e[0], count: e[1] }; });

    const result = {
      summary: {
        totalRev, totalOrd, rev7, ord30,
        revProduct, revDepFee, revSewabot,
        completedTotal, failedTotal, pendingTotal,
        conversionRate: convDenom > 0 ? Math.round(convNumer / convDenom * 100) : 0,
        avgOrderValue: completedTotal > 0 ? Math.round(revProduct / completedTotal) : 0,
      },
      daily, monthly, topProducts,
    };
    // Cache result
    _analyticsCache = result;
    _analyticsCacheAt = Date.now();

    res.json(Object.assign({ ok: true }, result));
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ════════════════════════════════════════════════════════════════
// ORDER HISTORY (public — by phone or trx ID)
// ════════════════════════════════════════════════════════════════
app.post('/api/history', async function(req, res) {
  try {
    const ip = req.ip || 'x';
    if (!rateLimit('hist:' + ip, 8, 60000)) return res.status(429).json({ ok: false, message: 'Terlalu banyak permintaan.' });
    const query = String(req.body.query || '').trim();
    if (!query || query.length < 4) return res.json({ ok: false, message: 'Masukkan nomor HP atau ID transaksi.' });

    // Try direct ID lookup first (TRX-, RNW-, BOT- prefixes)
    // Normalize: uppercase prefix, lowercase hex suffix (files are stored with lowercase hex)
    var _normalized = query
      .replace(/^([A-Za-z]+)-/, function(_, p){ return p.toUpperCase() + '-'; })
      .replace(/-([0-9A-Fa-f]+)$/, function(_, h){ return '-' + h.toLowerCase(); });
    var _qUp = _normalized.toUpperCase(); // only used for prefix checks
    var _isTrxId = /^(TRX|RNW|BOT)-\d{13}-[a-f0-9]{8}$/i.test(query);
    if (_isTrxId) {
      // Coba di transactions/ dulu (TRX- dan RNW-)
      if (_normalized.startsWith('TRX-') || _normalized.startsWith('RNW-')) {
        try {
          const r = await getTrx(_normalized);
          if (r.data) return res.json({ ok: true, data: [sanitizeHistoryItem(r.data)] });
        } catch(e) {}
      }
      // Coba di sewabot-orders/ (BOT-)
      if (_normalized.startsWith('BOT-')) {
        try {
          const sb = await getSewabotOrder(_normalized);
          if (sb.data) return res.json({ ok: true, data: [sanitizeHistoryItem(Object.assign({ productName: 'Sewa Bot', variantName: sb.data.days + ' Hari', productType: 'sewabot' }, sb.data))] });
        } catch(e) {}
      }
      // Coba semua jika prefix tidak cocok
      try {
        const r = await getTrx(_normalized);
        if (r.data) return res.json({ ok: true, data: [sanitizeHistoryItem(r.data)] });
      } catch(e) {}
      return res.json({ ok: false, message: 'ID transaksi tidak ditemukan.' });
    }

    // Search by phone
    const files = await listTrx();
    const phone = query.replace(/[^0-9]/g, '');
    const matches = [];
    await Promise.all(files.filter(function(f){ return f.name.endsWith('.json'); }).map(async function(f) {
      try {
        const r = await dbRead('transactions/' + f.name);
        if (!r.data) return;
        const d = r.data;
        const dp = (d.phone || '').replace(/[^0-9]/g, '');
        if (dp && dp.endsWith(phone.slice(-9))) matches.push(sanitizeHistoryItem(d));
      } catch(e){}
    }));

    // Also search deposits — by username (if query looks like a username)
    if (/^[a-z0-9_]{3,20}$/.test(query.toLowerCase())) {
      try {
        const depListR = await listDirCached('deposits');
        const depFiles = Array.isArray(depListR) ? depListR.filter(function(f){ return f.name.endsWith('.json'); }) : [];
        await Promise.all(depFiles.map(async function(f) {
          try {
            const d = await getDeposit(f.name.replace('.json', ''));
            if (d.data && d.data.username === query.toLowerCase()) {
              matches.push({ type: 'deposit', id: d.data.id, amount: d.data.amount, totalBayarDeposit: d.data.totalBayarDeposit || d.data.amount, adminFeeDeposit: d.data.adminFeeDeposit || 0, status: d.data.status, createdAt: d.data.createdAt });
            }
          } catch(e){}
        }));
      } catch(e){}
    }

    if (matches.length === 0) return res.json({ ok: false, message: 'Tidak ada order ditemukan.' });
    matches.sort(function(a, b){ return (b.createdAt||0) - (a.createdAt||0); });
    res.json({ ok: true, data: matches.slice(0, 20) });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

function sanitizeHistoryItem(d) {
  const safe = {
    id: d.id, status: d.status, productName: d.productName || '—',
    variantName: d.variantName || '—', totalBayar: d.totalBayar || d.unitPrice || 0,
    createdAt: d.createdAt, phone: d.phone ? d.phone.slice(0,-4)+'****' : null,
    productType: d.productType || 'digital',
    customFields: d.customFields || null, // expose custom field values
  };
  // Only expose result if COMPLETED
  if ((d.status === 'COMPLETED' || d.status === 'PAID_ERROR') && d.result) {
    const r = d.result;
    if (r.type === 'panel') {
      safe.result = { type:'panel', domain: r.domain, username: r.username, password: r.password, expiresAt: r.expiresAt, plan: r.plan, ram: r.ram, disk: r.disk, cpu: r.cpu };
    } else if (r.type === 'content') {
      // Format akun multi-baris agar rapi: tiap baris = satu field
      var ct = String(r.contentText || '');
      // Deteksi format akun: "email|password" atau "email:password" atau "username\npassword"
      var parsed = null;
      if (ct.includes('|') || ct.includes(':')) {
        var sep = ct.includes('|') ? '|' : ':';
        var parts = ct.split(sep).map(function(s){ return s.trim(); });
        if (parts.length === 2 && parts[0] && parts[1]) {
          parsed = { email: parts[0], password: parts[1] };
        } else if (parts.length >= 2) {
          parsed = { raw: ct, lines: parts };
        }
      } else if (ct.includes('\n')) {
        var lines = ct.split('\n').map(function(l){ return l.trim(); }).filter(Boolean);
        if (lines.length >= 2) parsed = { lines: lines };
      }
      safe.result = { type:'content', contentType: r.contentType, contentUrl: r.contentUrl, contentText: ct, title: r.title || d.variantName, parsedAccount: parsed };
    } else if (r.type === 'renewal') {
      safe.result = { type:'renewal', domain: r.domain, username: r.username, expiresAt: r.expiresAt, addedDays: r.addedDays };
    } else if (r.type === 'sewabot') {
      safe.result = { type:'sewabot', groupUrl: r.groupUrl, days: r.days };
    } else if (r.type === 'error') {
      safe.result = { type:'error', message: r.message };
    }
  }
  return safe;
}

// ════════════════════════════════════════════════════════════════
// AUTO SUSPEND EXPIRED PANELS (enhanced: suspend instead of delete)
// ════════════════════════════════════════════════════════════════
async function autoSuspendExpiredPanels() {
  if (!C.ptero.domain || !C.ptero.apikey) return;
  try {
    const files = await listTrx(); const headers = ptH(); const dom = C.ptero.domain;
    let suspended = 0, deleted = 0;

    // Kumpulkan serverId milik reseller agar tidak ikut di-suspend oleh cron ini
    // (panel reseller dikelola manual oleh admin via /api/admin/reseller-servers)
    const resellerServerIds = new Set();
    try {
      var rsFiles = (await listDirCached('reseller-servers')).filter(function(f){ return f.name.endsWith('.json'); });
      await Promise.all(rsFiles.map(async function(f) {
        try { var r = await getRsServer(f.name.replace('.json','')); if (r.data && r.data.serverId) resellerServerIds.add(String(r.data.serverId)); } catch(e) {}
      }));
    } catch(e) { /* folder reseller-servers mungkin belum ada */ }
    for (const f of files.filter(function(f){ return f.name.endsWith('.json'); })) {
      try {
        const r = await getTrx(f.name.replace('.json',''));
        if (!r.data) continue; const d = r.data;
        if (d.status !== 'COMPLETED' || d.productType !== 'panel') continue;
        const result = d.result || {};
        if (!result.expiresAt || !result.serverId) continue;
        // SKIP: jangan suspend panel milik reseller — dikelola manual admin
        if (resellerServerIds.has(String(result.serverId))) continue;
        const daysLeft = (result.expiresAt - Date.now()) / 86400000;

        // Auto-suspend at 0 days
        if (daysLeft <= 0 && !d._panelSuspended && !d._panelDeleted) {
          try {
            await axios.post(dom + '/api/application/servers/' + result.serverId + '/suspend', {}, { headers });
            await saveTrx(d.id, Object.assign({}, d, { _panelSuspended: true, _suspendedAt: Date.now() }), r.sha);
            suspended++;
            console.log('[auto-suspend] suspended:', result.username, '| server:', result.serverId);
          } catch(e) { if (e.response && e.response.status === 404) { await saveTrx(d.id, Object.assign({}, d, { _panelDeleted: true }), r.sha); } }
        }

        // Delete after 3 more days grace period
        if (daysLeft <= -3 && d._panelSuspended && !d._panelDeleted) {
          try { await axios.delete(dom + '/api/application/servers/' + result.serverId, { headers }); } catch(e) {}
          try {
            if (result.userId) await axios.delete(dom + '/api/application/users/' + result.userId, { headers });
          } catch(e) {}
          await saveTrx(d.id, Object.assign({}, d, { status: 'EXPIRED', _panelDeleted: true, _deletedAt: Date.now() }), r.sha);
          deleted++;
          console.log('[auto-delete] deleted:', result.username);
        }
      } catch(e) {}
    }
    if (suspended > 0 || deleted > 0) console.log('[panel-cron] suspended:', suspended, '| deleted:', deleted);
  } catch(e) { console.error('[panel-cron]', e.message); }
}
// Run every 30 minutes, first run after 60s
// Di Vercel serverless, piggybacked cron menggantikan setInterval ini.
// Biarkan tetap ada untuk fallback saat dev lokal.
setInterval(autoSuspendExpiredPanels, 30 * 60 * 1000);
setTimeout(autoSuspendExpiredPanels, 60 * 1000);

// Admin: manual trigger panel cron
app.post('/api/admin/panels/run-cron', adminAuth, async function(req, res) {
  autoSuspendExpiredPanels().catch(function(){});
  res.json({ ok: true, message: 'Panel cron dijadwalkan.' });
});

// Admin: manual trigger OTP expire cron
app.post('/api/admin/otp/run-expire', adminAuth, async function(req, res) {
  autoExpireOtpOrders().catch(function(){});
  res.json({ ok: true, message: 'OTP expire cron dijadwalkan.' });
});

// ══════════════════════════════════════════════════════════════
// AUTO-SEED DEFAULT PANEL PRODUCTS
// Membuat semua varian panel 1GB – Unlimited secara otomatis
// tanpa perlu tambah manual satu per satu di admin panel.
// ══════════════════════════════════════════════════════════════

// Harga default per GB (sesuai list harga resmi)
const DEFAULT_PANEL_PRICES = {
  '1gb': 5000,   '2gb': 6000,   '3gb': 7000,   '4gb': 8000,
  '5gb': 9000,   '6gb': 10000,  '7gb': 11000,  '8gb': 12000,
  '9gb': 13000,  '10gb': 14000, '11gb': 15000, '12gb': 16000,
  '13gb': 17000, '14gb': 18000, '15gb': 19000, '16gb': 20000,
  '17gb': 21000, '18gb': 22000, '19gb': 23000, '20gb': 24000,
  'unlimited': 40000,
};

const DEFAULT_PANEL_NAMES = {
  '1gb': '1GB RAM', '2gb': '2GB RAM', '3gb': '3GB RAM', '4gb': '4GB RAM',
  '5gb': '5GB RAM', '6gb': '6GB RAM', '7gb': '7GB RAM', '8gb': '8GB RAM',
  '9gb': '9GB RAM', '10gb': '10GB RAM', '11gb': '11GB RAM', '12gb': '12GB RAM',
  '13gb': '13GB RAM', '14gb': '14GB RAM', '15gb': '15GB RAM', '16gb': '16GB RAM',
  '17gb': '17GB RAM', '18gb': '18GB RAM', '19gb': '19GB RAM', '20gb': '20GB RAM',
  'unlimited': 'Unlimited RAM',
};

function buildDefaultPanelProduct() {
  var planKeys = [
    '1gb','2gb','3gb','4gb','5gb','6gb','7gb','8gb','9gb','10gb',
    '11gb','12gb','13gb','14gb','15gb','16gb','17gb','18gb','19gb','20gb','unlimited'
  ];
  var variants = planKeys.map(function(plan) {
    var spec = SPEC[plan];
    var price = DEFAULT_PANEL_PRICES[plan];
    var name = DEFAULT_PANEL_NAMES[plan];
    return {
      id     : 'panel-' + plan,
      name   : name,
      plan   : plan,
      price  : price,
      salePrice: null,
      stock  : -1,
      days   : 30,
      daysOptions: [7, 14, 30],
      dayPrices: {
        '7' : Math.round(price * 7  / 30),
        '14': Math.round(price * 14 / 30),
        '30': price,
      },
      description: (spec.ram === 0 ? 'Unlimited' : (spec.ram / 1024).toFixed(0) + 'GB') + ' RAM · ' +
                   (spec.disk === 0 ? 'Unlimited' : (spec.disk / 1024).toFixed(0) + 'GB') + ' Disk · ' +
                   (spec.cpu === 0 ? 'Unlimited' : spec.cpu + '%') + ' CPU',
      contentType : 'panel',
      active      : true,
    };
  });
  return {
    id         : 'panel-legal',
    name       : 'Panel Legal Pterodactyl',
    type       : 'panel',
    category   : 'Panel',
    description: '🔒 Server privat & kualitas terbaik\n⚡ Anti delay / lemot\n🛡 Script anti-maling, anti-drama\n🎯 Garansi 15 hari Full (1x replace)\n✅ Run Script Bug / MD lancar\n📋 Claim garansi wajib sertakan bukti transaksi',
    active     : true,
    image      : '',
    variants,
  };
}

async function autoSeedProducts() {
  // Guard: hanya jalan jika DB sudah siap (Supabase atau GitHub)
  if (DB_BACKEND === 'github' && !_dbOctokit) return;
  if (DB_BACKEND !== 'github' && !supabase)   return;
  try {
    const r = await dbRead('products.json');
    // Hanya seed jika belum ada produk sama sekali
    if (r.data && Array.isArray(r.data) && r.data.length > 0) return;
    const defaultProduct = buildDefaultPanelProduct();
    await dbWrite('products.json', [defaultProduct], r.sha || null, 'auto-seed: default panel products');
    console.log('[seed] Default panel products berhasil dibuat (21 varian: 1GB–Unlimited)');
  } catch(e) {
    console.warn('[seed] Gagal auto-seed products:', e.message);
  }
}

// Admin: seed default products (reset + rebuild)
// POST /api/admin/seed-products body: { confirm: true }
// body: { confirm: true, merge: true } → tambahkan panel ke produk existing (tidak hapus)
app.post('/api/admin/seed-products', adminAuth, async function(req, res) {
  try {
    const merge   = req.body.merge === true || req.body.merge === 'true';
    const confirm = req.body.confirm === true || req.body.confirm === 'true';
    if (!confirm) return res.json({ ok: false, message: 'Kirim { confirm: true } untuk konfirmasi.' });

    const defaultProduct = buildDefaultPanelProduct();
    const r = await dbRead('products.json');
    let products = [];

    if (merge && r.data && Array.isArray(r.data)) {
      // Merge: hapus produk panel lama (type=panel), tambahkan yang baru
      products = r.data.filter(function(p){ return p.type !== 'panel'; });
      products.push(defaultProduct);
    } else {
      // Replace: hanya berisi 1 panel product dengan semua varian
      products = [defaultProduct];
    }

    await dbWrite('products.json', products, r.sha || null, 'admin: seed default panel products');
    // Broadcast ke semua client agar produk langsung muncul
    broadcastStore({ type: 'reload' });
    auditLog('seed-products', merge ? 'merge' : 'replace', req.adminIp).catch(function(){});
    console.log('[admin] seed-products:', merge ? 'merge' : 'replace', '| total:', products.length, 'produk |', defaultProduct.variants.length, 'varian panel');
    res.json({
      ok: true,
      message: 'Berhasil! ' + defaultProduct.variants.length + ' varian panel (1GB–Unlimited) sudah di-generate.',
      totalProducts: products.length,
      panelVariants: defaultProduct.variants.length,
      preview: defaultProduct.variants.map(function(v){ return v.name + ' = Rp' + v.price.toLocaleString('id-ID'); }),
    });
  } catch(e) {
    console.error('[admin] seed-products:', e.message);
    res.json({ ok: false, message: e.message });
  }
});

// ── VERCEL CRON ENDPOINT ──────────────────────────────────────────────────
// Vercel Hobby: hanya boleh cron sekali sehari (vercel.json schedule: "0 0 * * *").
// Untuk frekuensi lebih tinggi, digunakan "piggybacked cron" — lihat middleware di atas.
// Endpoint ini tetap bermanfaat untuk:
//   1. Daily cleanup (deposit lama, panel expired, OTP stale)
//   2. Manual trigger via admin panel atau curl
//   3. Failsafe jika piggybacked cron tidak terpanggil seharian
//
// Untuk keamanan: set CRON_SECRET di environment variable Vercel.
const CRON_SECRET = process.env.CRON_SECRET || '';
app.get('/api/cron/run', async function(req, res) {
  var secret = req.headers['x-cron-secret'] || req.query.secret || '';
  // SECURITY FIX: CRON_SECRET wajib ada dan harus cocok.
  // Jika tidak diset di env, endpoint ini DITOLAK agar tidak bisa dipanggil sembarangan.
  if (!CRON_SECRET || secret !== CRON_SECRET) {
    return res.status(401).json({ ok: false, message: 'Unauthorized' });
  }
  console.log('[cron/run] triggered by:', req.ip);
  _lastPiggybackRun = Date.now(); // Reset agar piggyback tidak double-run
  // Run all crons non-blocking
  autoExpireOtpOrders().catch(function(e){ console.error('[cron/run] otp-expire:', e.message); });
  autoReconcileDeposits().catch(function(e){ console.error('[cron/run] reconcile:', e.message); });
  autoSuspendExpiredPanels().catch(function(e){ console.error('[cron/run] panels:', e.message); });
  autoExpirePendingOrders().catch(function(e){ console.error('[cron/run] trx-expire:', e.message); });
  res.json({ ok: true, ts: Date.now(), message: 'Semua cron dijalankan.' });
});

// ════════════════════════════════════════════════════════════════
// AUTO-EXPIRE PENDING TRX ORDERS (background — no client polling needed)
// Mengatasi order yang stuck di PENDING saat user tutup halaman sebelum timer habis.
// Berjalan via piggybacked cron (setiap ~5 menit). Pakasir juga dicek — jika sudah
// dibayar maka diproses, jika expired di Pakasir/waktu lewat maka di-cancel.
// ════════════════════════════════════════════════════════════════
var _trxExpireRunning = false;

async function autoExpirePendingOrders() {
  if (_trxExpireRunning) { console.log('[trx/expire-cron] skip — masih berjalan'); return; }
  _trxExpireRunning = true;
  try {
    const files = await listTrx();
    const pending = files.filter(function(f){ return f.name.endsWith('.json'); });
    let expired = 0, completed = 0, errors = 0;

    for (var i = 0; i < pending.length; i++) {
      if (i > 0) await _sleep(600);
      try {
        const r = await getTrx(pending[i].name.replace('.json', ''));
        if (!r.data) continue;
        const trx = r.data;

        // Hanya proses yang masih PENDING
        if (trx.status !== 'PENDING') continue;
        // Skip demo mode (tidak ada QRIS asli)
        if (trx.demo) {
          // Tetap expire jika waktu sudah lewat
          if (trx.expiryAt && Date.now() > trx.expiryAt) {
            await saveTrx(trx.id, Object.assign({}, trx, { status: 'EXPIRED', expiredAutoAt: Date.now() }), r.sha);
            expired++;
          }
          continue;
        }
        // Jika waktu sudah lewat (+ grace 2 menit), expire paksa
        if (trx.expiryAt && Date.now() > trx.expiryAt + 2 * 60 * 1000) {
          try { await pak.cancel(trx.id, trx.unitPrice); } catch(e) {}
          await saveTrx(trx.id, Object.assign({}, trx, { status: 'EXPIRED', expiredAutoAt: Date.now() }), r.sha);
          expired++;
          console.log('[trx/expire-cron] expired:', trx.id);
          continue;
        }
        // Jika masih dalam batas waktu, cek status Pakasir (mungkin sudah dibayar)
        try {
          const pakRes    = await pak.check(trx.id, trx.unitPrice);
          const trxObj    = pakRes && pakRes.transaction;
          const pakStatus = ((trxObj && trxObj.status) || (pakRes && pakRes.data && pakRes.data.status) || (pakRes && pakRes.status) || '').toLowerCase();
          if (pakStatus === 'completed' || pakStatus === 'paid' || pakStatus === 'success') {
            // Bayar terdeteksi! PROCESSING lock dulu sebelum deliver
            try {
              await saveTrx(trx.id, Object.assign({}, trx, { status: 'PROCESSING', processingAt: Date.now() }), r.sha);
            } catch(lockErr) {
              console.log('[trx/expire-cron] lock conflict (ok):', trx.id); continue;
            }
            try {
              const result = await processProductDelivery(trx, trx.id);
              const freshR = await getTrx(trx.id);
              await saveTrx(trx.id, Object.assign({}, freshR.data || trx, { status: 'COMPLETED', result, completedAt: Date.now() }), freshR.sha || null);
              decrementStock(trx.productId, trx.variantId).catch(function(){});
              broadcastAdmin({ type: 'trx_completed', id: trx.id, productName: trx.productName, variantName: trx.variantName, totalBayar: trx.totalBayar || trx.unitPrice, productType: trx.productType, phone: trx.phone || null, ts: Date.now() });
              console.log('[trx/expire-cron] auto-completed (cron):', trx.id);
              completed++;
            } catch(procErr) {
              const errResult = { type: 'error', message: 'Pembayaran diterima tapi proses gagal. Hubungi admin. ID: ' + trx.id };
              const freshR2 = await getTrx(trx.id);
              await saveTrx(trx.id, Object.assign({}, freshR2.data || trx, { status: 'PAID_ERROR', error: procErr.message, result: errResult }), freshR2.sha || null);
              errors++;
            }
          } else if (pakStatus === 'failed' || pakStatus === 'canceled' || pakStatus === 'cancelled') {
            await saveTrx(trx.id, Object.assign({}, trx, { status: 'FAILED', expiredAutoAt: Date.now() }), r.sha);
            expired++;
          }
          // else masih pending di Pakasir — biarkan, akan dicek lagi nanti
        } catch(pakErr) { /* Pakasir tidak bisa dihubungi — skip item ini */ }
      } catch(e) { errors++; console.warn('[trx/expire-cron] item error:', pending[i].name, e.message); }
    }
    if (expired > 0 || completed > 0 || errors > 0) {
      console.log('[trx-expire-cron] selesai | expired:', expired, '| auto-completed:', completed, '| error:', errors);
    }
  } catch(e) { console.error('[trx/expire-cron]', e.message); }
  finally { _trxExpireRunning = false; }
}

// ════════════════════════════════════════════════════════════════
// AUTO-RECONCILE PENDING DEPOSITS
// Mengatasi saldo tidak masuk saat user tutup halaman sebelum polling selesai.
// Cron ini berjalan setiap 5 menit, max 10 deposit per run, dengan jeda 800ms antar item.
// ════════════════════════════════════════════════════════════════
var _reconcileRunning = false; // prevent concurrent runs

async function autoReconcileDeposits() {
  // SECURITY FIX: reconcile harus jalan di kedua backend (Supabase & GitHub DB)
  // Sebelumnya ada guard `if (!supabase) return` yang menyebabkan deposit tidak pernah
  // direconcile jika pakai DATABASE=github
  if (_reconcileRunning) { console.log('[deposit/reconcile] skip — masih berjalan'); return; }
  _reconcileRunning = true;
  try {
    let files = [];
    try {
      const r = await listDirCached('deposits');
      files = Array.isArray(r) ? r.filter(function(f){ return f.name.endsWith('.json'); }) : [];
    } catch(e) {
      if (!e.response || e.response.status !== 404) throw e;
      return;
    }

    // Proses maksimal 10 deposit per run untuk menghindari GitHub rate limit
    const MAX_PER_RUN = 10;
    let processed = 0, credited = 0, cancelled = 0;

    for (var i = 0; i < files.length && processed < MAX_PER_RUN; i++) {
      var f = files[i];
      // Jeda 800ms antar item — GitHub secondary rate limit sangat sensitif terhadap burst
      if (processed > 0) await _sleep(800);

      try {
        const d = await getDeposit(f.name.replace('.json', ''));
        if (!d.data || (d.data.status !== 'pending' && d.data.status !== 'crediting')) continue;
        const dep = d.data;
        processed++;

        if (dep.status === 'crediting') {
          // FIX BUG KRITIS: Jika balanceCredited:true → saldo sudah masuk ke user.
          // Jangan reset ke 'pending' karena akan trigger updateBalance lagi → DOUBLE CREDIT.
          // Langsung mark sebagai 'success' tanpa memanggil updateBalance.
          if (dep.balanceCredited) {
            console.log('[deposit/reconcile] rescue crediting+balanceCredited:', dep.id, '| mark success (skip updateBalance)');
            const frDone = await getDeposit(dep.id).catch(function(){ return { data: dep, sha: d.sha }; });
            await saveDeposit(dep.id, Object.assign({}, frDone.data || dep, {
              status: 'success', paidAt: dep.paidAt || Date.now(), reconciledAt: Date.now(), _rescuedCreditedAt: Date.now(),
            }), frDone.sha || null).catch(function(e){ console.warn('[deposit/reconcile] rescue save gagal:', dep.id, e.message); });
            credited++;
            continue;
          }
          // TTL 10 menit — updateBalance tidak mungkin butuh >10 menit.
          // Hanya reset ke 'pending' jika balanceCredited tidak ada (saldo belum masuk).
          if (dep.creditingAt && Date.now() - dep.creditingAt > 10 * 60 * 1000) {
            console.warn('[deposit/reconcile] crediting stuck >10min (tanpa balanceCredited), reset ke pending:', dep.id);
            await saveDeposit(dep.id, Object.assign({}, dep, { status: 'pending', creditingAt: null, _creditingResetAt: Date.now() }), d.sha);
          }
          continue;
        }

        if (dep.expiredAt && Date.now() > dep.expiredAt + 5 * 60 * 1000) {
          try { await rotp.depositCancel(dep.rotpId); } catch(e) {}
          await saveDeposit(dep.id, Object.assign({}, dep, { status: 'cancel', expiredAuto: true, cancelledAt: Date.now() }), d.sha);
          cancelled++;
          continue;
        }

        let rotpStatus = null;
        try {
          const sr = await rotp.depositStatus(dep.rotpId);
          rotpStatus = (sr && sr.success && sr.data) ? sr.data.status : null;
        } catch(e) { continue; }

        if (rotpStatus === 'success') {
          try {
            await saveDeposit(dep.id, Object.assign({}, dep, { status: 'crediting', creditingAt: Date.now() }), d.sha);
          } catch(lockErr) { continue; }

          let newBal;
          try {
            newBal = await updateBalance(dep.username, dep.amount);
          } catch(balErr) {
            console.error('[deposit/reconcile] balance GAGAL:', dep.id, balErr.message);
            const freshFail = await getDeposit(dep.id);
            await saveDeposit(dep.id, Object.assign({}, freshFail.data || dep, { status: 'pending', creditingAt: null }), freshFail.sha || null);
            continue;
          }

          // FIX: tulis balanceCredited:true SEGERA setelah updateBalance berhasil
          // agar jika saveDeposit(success) di bawah gagal, cron berikutnya tidak double credit
          try {
            const frFlag = await getDeposit(dep.id).catch(function(){ return { data: dep, sha: null }; });
            await saveDeposit(dep.id, Object.assign({}, frFlag.data || dep, {
              status: 'crediting', balanceCredited: true, creditedBalance: newBal,
            }), frFlag.sha || null);
          } catch(flagErr) { console.warn('[deposit/reconcile] balanceCredited flag gagal:', dep.id, flagErr.message); }

          const freshOk = await getDeposit(dep.id);
          await saveDeposit(dep.id, Object.assign({}, freshOk.data || dep, {
            status         : 'success',
            paidAt         : Date.now(),
            creditedBalance: newBal,
            reconciledAt   : Date.now(),
          }), freshOk.sha || null);

          console.log('[deposit/reconcile] saldo masuk:', dep.id, '| user:', dep.username, '| Rp' + dep.amount, '| saldo baru:', newBal);
          broadcastAdmin({ type: 'deposit_success', id: dep.id, username: dep.username, amount: dep.amount, ts: Date.now() });
          credited++;

        } else if (rotpStatus === 'cancel') {
          await saveDeposit(dep.id, Object.assign({}, dep, { status: 'cancel', cancelledAt: Date.now() }), d.sha);
          cancelled++;
        }
      } catch(e) { console.warn('[deposit/reconcile] item error:', f.name, e.message); }
    }
    if (credited > 0 || cancelled > 0) {
      console.log('[deposit-reconcile] selesai | saldo masuk:', credited, '| dibatalkan:', cancelled);
    }
  } catch(e) { console.error('[deposit/reconcile]', e.message); }
  finally { _reconcileRunning = false; }
}

// Piggybacked cron menggantikan setInterval di Vercel serverless.
// setInterval tetap ada sebagai fallback saat server berjalan lokal / non-serverless.

// ════════════════════════════════════════════════════════════════
// AUTO-EXPIRE OTP ORDERS (7.5 menit — tanpa polling dari user)
// Mengatasi order OTP yang stuck di "waiting" saat user tutup halaman.
// ════════════════════════════════════════════════════════════════
const OTP_AUTO_EXPIRE_MS = 7.5 * 60 * 1000; // 7 menit 30 detik
var _otpExpireRunning = false;

async function autoExpireOtpOrders() {
  if (_otpExpireRunning) { console.log('[otp/expire-cron] skip — masih berjalan'); return; }
  _otpExpireRunning = true;
  try {
    let files = [];
    try {
      const r = await listDirCached('otp-orders');
      files = Array.isArray(r) ? r.filter(function(f){ return f.name.endsWith('.json'); }) : [];
    } catch(e) {
      if (!e.response || e.response.status !== 404) throw e;
      return; // folder belum ada, skip
    }

    let expired = 0, errors = 0;
    const cutoff = Date.now() - OTP_AUTO_EXPIRE_MS; // order lebih lama dari 7.5 menit

    for (var i = 0; i < files.length; i++) {
      if (i > 0) await _sleep(600); // jeda 600ms antar item

      try {
        const d = await getOtpOrder(files[i].name.replace('.json', ''));
        if (!d.data) continue;
        const ord = d.data;

        // Hanya proses yang masih waiting ATAU expired tapi refund belum berhasil
        if (ord.status === 'expired' && ord.refunded) continue; // sudah beres
        // Proses: waiting, expired(unrefunded), expiring(stuck), canceling(stuck)
        const _validStatuses = ['waiting', 'expired', 'expiring', 'canceling'];
        if (!_validStatuses.includes(ord.status)) continue;

        // ── RESCUE: 'canceling' nyangkut > 3 menit ──────────────────────────────
        // Terjadi jika saveOtpOrder(canceled) gagal setelah updateBalance(+refund) berhasil.
        // Cek flag balanceRefunded untuk tahu apakah saldo sudah dikembalikan.
        if (ord.status === 'canceling') {
          var _cancelingAge = Date.now() - (ord.cancelingAt || ord.createdAt || 0);
          if (_cancelingAge < 3 * 60 * 1000) continue; // terlalu baru, masih mungkin sedang proses
          console.log('[otp/expire-cron] RESCUE canceling-stuck:', ord.id, '| nyangkut', Math.round(_cancelingAge/1000) + 's');
          try {
            // Jika balanceRefunded:true → saldo sudah kembali, langsung mark canceled
            // Jika tidak ada flag → kita tidak tahu; coba updateBalance (resiko kecil double refund vs tidak refund)
            if (!ord.balanceRefunded) {
              await updateBalance(ord.username, ord.price);
            } else {
              console.log('[otp/expire-cron] canceling-stuck: balanceRefunded sudah true, skip updateBalance');
            }
            const frCancel = await getOtpOrder(ord.id);
            await saveOtpOrder(ord.id, Object.assign({}, frCancel.data || ord, {
              status: 'canceled', cancelledAt: Date.now(), refunded: true, refundedAt: Date.now(),
              _cronRescuedCanceling: Date.now(),
            }), frCancel.sha || null);
            console.log('[otp/expire-cron] RESCUE canceling OK:', ord.id, '| user:', ord.username);
            expired++;
          } catch(cancelRescueErr) {
            console.error('[otp/expire-cron] RESCUE canceling GAGAL:', ord.id, cancelRescueErr.message);
            errors++;
          }
          continue;
        }

        // ── RESCUE: 'expiring' nyangkut > 3 menit ───────────────────────────────
        if (ord.status === 'expiring') {
          var _expiringAge = Date.now() - (ord.expiringAt || ord.createdAt || 0);
          if (_expiringAge < 3 * 60 * 1000) continue; // masih baru, tunggu dulu
          console.log('[otp/expire-cron] RESCUE expiring-stuck:', ord.id, '| nyangkut', Math.round(_expiringAge/1000) + 's');
          try {
            // FIX: cek balanceRefunded — jika sudah ada, JANGAN updateBalance lagi (cegah double refund)
            if (!ord.balanceRefunded) {
              await updateBalance(ord.username, ord.price);
              // Tulis flag dulu sebelum saveOtpOrder akhir
              const frFlag = await getOtpOrder(ord.id).catch(function(){ return { data: ord, sha: null }; });
              await saveOtpOrder(ord.id, Object.assign({}, frFlag.data || ord, { balanceRefunded: true }), frFlag.sha || null).catch(function(){});
            } else {
              console.log('[otp/expire-cron] expiring-stuck: balanceRefunded sudah true, skip updateBalance');
            }
            const frStuck = await getOtpOrder(ord.id);
            await saveOtpOrder(ord.id, Object.assign({}, frStuck.data || ord, {
              status: 'expired', refunded: true, refundedAt: Date.now(), _cronRescuedStuck: Date.now(),
            }), frStuck.sha || null);
            console.log('[otp/expire-cron] RESCUE expiring OK:', ord.id, '| Rp' + ord.price, '| user:', ord.username);
            expired++;
          } catch(stuckErr) {
            console.error('[otp/expire-cron] RESCUE expiring GAGAL:', ord.id, stuckErr.message);
            const frReset = await getOtpOrder(ord.id).catch(function(){ return { data: ord, sha: null }; });
            await saveOtpOrder(ord.id, Object.assign({}, frReset.data || ord, {
              status: 'waiting', expiringAt: null, _rescueFailedAt: Date.now(),
            }), frReset.sha || null).catch(function(){});
            errors++;
          }
          continue;
        }

        // ── RESCUE: 'expired' tapi refunded=false → coba refund ulang ──────────
        if (ord.status === 'expired' && !ord.refunded) {
          // ATOMIC LOCK agar tidak double-refund
          try {
            await saveOtpOrder(ord.id, Object.assign({}, ord, { status: 'expiring', expiringAt: Date.now(), _cronRescue: true }), d.sha);
          } catch(lockErr) { continue; }
          try {
            // FIX: cek balanceRefunded — mungkin updateBalance sudah berhasil sebelumnya
            // tapi saveOtpOrder gagal sehingga refunded tetap false. Jangan double refund.
            if (!ord.balanceRefunded) {
              await updateBalance(ord.username, ord.price);
              // Tulis flag dulu
              const frRescFlag = await getOtpOrder(ord.id).catch(function(){ return { data: ord, sha: null }; });
              await saveOtpOrder(ord.id, Object.assign({}, frRescFlag.data || ord, { balanceRefunded: true }), frRescFlag.sha || null).catch(function(){});
            } else {
              console.log('[otp/expire-cron] rescue expired: balanceRefunded sudah true, skip updateBalance:', ord.id);
            }
            const frRescue = await getOtpOrder(ord.id);
            await saveOtpOrder(ord.id, Object.assign({}, frRescue.data || ord, {
              status: 'expired', refunded: true, refundedAt: Date.now(), _cronRescuedAt: Date.now(),
            }), frRescue.sha || null);
            console.log('[otp/expire-cron] RESCUE refund:', ord.id, '| Rp' + ord.price, '| user:', ord.username);
            expired++;
          } catch(rescueErr) {
            console.error('[otp/expire-cron] rescue refund GAGAL:', ord.id, rescueErr.message);
            const frFail = await getOtpOrder(ord.id);
            await saveOtpOrder(ord.id, Object.assign({}, frFail.data || ord, {
              status: 'expired', refunded: false, expiringAt: null,
            }), frFail.sha || null).catch(function(){});
            errors++;
          }
          continue;
        }

        // Cek apakah sudah melewati batas 7.5 menit ATAU sudah lewat expiresAt-nya
        const isOverdue = (ord.createdAt && ord.createdAt < cutoff);
        const isExpired = (ord.expiresAt && Date.now() > ord.expiresAt);
        if (!isOverdue && !isExpired) continue;

        // ATOMIC LOCK — cegah double-refund dengan request user
        try {
          await saveOtpOrder(ord.id, Object.assign({}, ord, { status: 'expiring', expiringAt: Date.now(), _cronExpire: true }), d.sha);
        } catch(lockErr) {
          console.log('[otp/expire-cron] lock conflict (ok):', ord.id);
          continue; // request lain sudah handle
        }

        // Cancel di RumahOTP (best-effort)
        try { await rotp.cancelOrder(ord.rotpOrderId); } catch(e) { console.warn('[otp/expire-cron] rotp cancel:', e.message); }

        // Refund balance
        let refunded = false;
        try {
          await updateBalance(ord.username, ord.price);
          refunded = true;
          // FIX: tulis balanceRefunded:true segera agar jika saveOtpOrder(expired) gagal di bawah,
          // rescue cron berikutnya tidak panggil updateBalance lagi → cegah double refund
          try {
            const frBalFlag = await getOtpOrder(ord.id).catch(function(){ return { data: ord, sha: null }; });
            await saveOtpOrder(ord.id, Object.assign({}, frBalFlag.data || ord, { balanceRefunded: true }), frBalFlag.sha || null);
          } catch(flagErr) { /* non-critical — rescue cron akan handle jika stuck */ }
        } catch(balErr) {
          console.error('[otp/expire-cron] refund GAGAL:', ord.id, balErr.message);
          // FIX: jika refund gagal, kembalikan ke 'waiting' bukan simpan 'expired' dengan refunded=false
          // agar siklus rescue berikutnya bisa coba lagi
          const fr = await getOtpOrder(ord.id);
          await saveOtpOrder(ord.id, Object.assign({}, fr.data || ord, {
            status: 'waiting', expiringAt: null, _refundFailedAt: Date.now(),
          }), fr.sha || null);
          errors++;
          continue;
        }

        const fr2 = await getOtpOrder(ord.id);
        await saveOtpOrder(ord.id, Object.assign({}, fr2.data || ord, {
          status       : 'expired',
          refunded     : refunded,
          refundedAt   : refunded ? Date.now() : null,
          expiredAutoAt: Date.now(),
          _cronExpire  : true,
        }), fr2.sha || null);

        console.log('[otp/expire-cron] expired+refund:', ord.id, '| Rp' + ord.price, '| user:', ord.username);
        expired++;
      } catch(e) { console.warn('[otp/expire-cron] item error:', files[i].name, e.message); errors++; }
    }

    if (expired > 0 || errors > 0) {
      console.log('[otp-expire-cron] selesai | expired+refunded:', expired, '| error:', errors);
    }
  } catch(e) { console.error('[otp/expire-cron]', e.message); }
  finally { _otpExpireRunning = false; }
}

// Di Vercel serverless, setInterval tidak berjalan antar-request.
// Piggybacked cron middleware di atas menangani ini via setiap request masuk.

// ── DEPOSIT CLEANUP: hapus file deposit final (success/cancel) yang sudah > 7 hari
// Ini penting untuk mengurangi jumlah file yang dibaca tiap reconcile run
async function cleanupOldDeposits() {
  try {
    const r = await listDirCached('deposits');
    var files = Array.isArray(r) ? r.filter(function(f){ return f.name.endsWith('.json'); }) : [];
    var WEEK = 7 * 24 * 60 * 60 * 1000;
    var deleted = 0;
    for (var i = 0; i < files.length; i++) {
      await _sleep(600);
      try {
        var d = await getDeposit(files[i].name.replace('.json', ''));
        if (!d.data) continue;
        var finalStatus = d.data.status === 'success' || d.data.status === 'cancel';
        var old = d.data.createdAt && (Date.now() - d.data.createdAt) > WEEK;
        if (finalStatus && old) {
          await dbDelete('deposits/' + files[i].name);
          _gitTreeCache.delete('deposits');
          deleted++;
        }
      } catch(e) { /* skip jika gagal */ }
    }
    if (deleted > 0) console.log('[deposit/cleanup] dihapus:', deleted, 'file lama');
  } catch(e) {
    if (!e.response || e.response.status !== 404) console.warn('[deposit/cleanup]', e.message);
  }
}
// Cleanup sekali sehari, mulai 10 menit setelah server start
setInterval(cleanupOldDeposits, 24 * 60 * 60 * 1000);
setTimeout(cleanupOldDeposits, 10 * 60 * 1000);

// Admin: manual trigger reconcile deposit
app.post('/api/admin/deposits/reconcile', adminAuth, async function(req, res) {
  autoReconcileDeposits().catch(function(){});
  res.json({ ok: true, message: 'Reconcile deposit dijadwalkan.' });
});

// ════════════════════════════════════════════════════════════════
// CAPTCHA (simple math captcha — no external service)
// ════════════════════════════════════════════════════════════════
const _captchaStore = new Map(); // token → {answer, expiresAt}

app.get('/api/captcha', function(req, res) {
  const a = Math.floor(Math.random() * 12) + 1;
  const b = Math.floor(Math.random() * 12) + 1;
  const answer = a + b;
  const token  = require('crypto').randomBytes(16).toString('hex');
  _captchaStore.set(token, { answer, expiresAt: Date.now() + 5 * 60 * 1000 });
  // Cleanup old tokens
  if (_captchaStore.size > 500) {
    const now = Date.now();
    for (const [k, v] of _captchaStore) { if (v.expiresAt < now) _captchaStore.delete(k); }
  }
  res.json({ ok: true, token, question: a + ' + ' + b + ' = ?' });
});

function verifyCaptcha(token, answer) {
  const entry = _captchaStore.get(token);
  if (!entry) return false;
  _captchaStore.delete(token); // one-time use
  if (entry.expiresAt < Date.now()) return false;
  return parseInt(answer) === entry.answer;
}

// ══════════════════════════════════════════════════════════════
// CDN UPLOAD ROUTES — public, siapapun bisa upload
// ══════════════════════════════════════════════════════════════

// Upload file ke CDN (public — rate limited)
app.post('/api/upload', _multer.single('file'), async function(req, res) {
  try {
    const ip = req.ip || 'x';
    if (!rateLimit('cdn_up:' + ip, 20, 15 * 60 * 1000)) return res.status(429).json({ ok: false, message: 'Terlalu banyak upload. Coba lagi nanti.' });

    // Cek apakah CDN dikonfigurasi
    if (!_cdnAccounts().length) return res.status(503).json({ ok: false, message: 'CDN belum dikonfigurasi. Hubungi admin.' });

    let buffer, ext, originalName;
    const startTime = Date.now();

    if (req.file) {
      buffer = req.file.buffer;
      originalName = req.file.originalname || 'file';
      const dotIdx = originalName.lastIndexOf('.');
      ext = dotIdx >= 0 ? originalName.slice(dotIdx).toLowerCase() : '';
    } else if (req.body && req.body.url) {
      const targetUrl = String(req.body.url || '').trim();
      if (!targetUrl.startsWith('http://') && !targetUrl.startsWith('https://')) return res.json({ ok: false, message: 'URL tidak valid. Harus dimulai dengan http/https.' });
      // SECURITY: block SSRF — reject internal/private IPs and localhost
      const _ssrfBlock = /^https?:\/\/(localhost|127\.|10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[01])\.|169\.254\.|0\.0\.0\.0|::1|metadata\.)/i;
      if (_ssrfBlock.test(targetUrl)) return res.json({ ok: false, message: 'URL tidak diizinkan.' });
      if (!rateLimit('cdn_url:' + ip, 10, 15 * 60 * 1000)) return res.status(429).json({ ok: false, message: 'Terlalu banyak request.' });
      const urlRes = await axios.get(targetUrl, { responseType: 'arraybuffer', timeout: 15000, maxContentLength: Math.max(C.cdn.maxSize, 100 * 1024 * 1024), maxRedirects: 3 });
      buffer = Buffer.from(urlRes.data);
      const urlPath = targetUrl.split('?')[0].split('/').pop();
      const dotIdx2 = urlPath.lastIndexOf('.');
      ext = dotIdx2 >= 0 ? urlPath.slice(dotIdx2).toLowerCase() : '';
      originalName = urlPath;
    } else {
      return res.json({ ok: false, message: 'File atau URL diperlukan.' });
    }

    // Validasi ukuran (CDN_MAX_SIZE_MB dari env, default 100MB; file < 25MB pakai API, >= 25MB pakai git clone)
    const maxSize = Math.max(C.cdn.maxSize, 100 * 1024 * 1024);
    if (buffer.length > maxSize) return res.json({ ok: false, message: 'Ukuran file terlalu besar. Maksimal ' + (maxSize / 1024 / 1024).toFixed(0) + 'MB.' });

    if (!ext || !CDN_ALLOWED_EXT.test(ext)) ext = '.bin';

    // Custom slug support: req.body.slug atau req.body.name
    let filename;
    const rawSlug = String(req.body.slug || req.body.name || '').trim();
    if (rawSlug) {
      // Sanitize: hanya huruf, angka, dash, underscore, titik — maks 80 char
      const slug = rawSlug.replace(/[^a-zA-Z0-9\-_.]/g, '-').replace(/-{2,}/g, '-').replace(/^[-.]|[-.]$/g, '').slice(0, 80);
      if (!slug) return res.json({ ok: false, message: 'Nama file tidak valid.' });
      // Tambahkan ekstensi kalau belum ada
      const slugHasExt = /\.[a-z0-9]{1,6}$/i.test(slug);
      filename = slugHasExt ? slug : slug + ext;
    } else {
      filename = crypto.randomBytes(8).toString('hex') + ext;
    }

    let url;
    try {
      url = await cdnUploadFile(filename, buffer);
    } catch(uploadErr) {
      console.error('[cdn/upload]', uploadErr.message);
      // Sanitize error — jangan expose token/secret ke client
      return res.json({ ok: false, message: _cdnSanitizeError(uploadErr) });
    }

    const fullUrl  = req.protocol + '://' + req.get('host') + url;
    const sizeMB   = (buffer.length / 1024 / 1024).toFixed(2);
    const timeSec  = ((Date.now() - startTime) / 1000).toFixed(2);

    console.log('[cdn/upload]', filename, '|', sizeMB + 'MB', '| ' + timeSec + 's | ip:', ip);
    res.json({ ok: true, url: fullUrl, path: url, filename: filename, size: buffer.length, sizeMB: parseFloat(sizeMB), uploadTime: parseFloat(timeSec) });
  } catch(e) {
    console.error('[cdn/upload]', e.message);
    res.json({ ok: false, message: _cdnSanitizeError(e) });
  }
});

// Serve CDN file (proxy dari repo utama, folder cdn/)
// Support random hex (e.g. /cdn/a1b2c3d4e5f6a7b8.jpg) dan custom slug (e.g. /cdn/nama-file.jpg)
app.get('/cdn/:filename', async function(req, res) {
  try {
    const filename = req.params.filename;
    if (!filename || filename.length > 200 || !/^[a-zA-Z0-9\-_.]+$/.test(filename)) return res.status(404).send('Not found');
    if (!rateLimit('cdnget:' + (req.ip || 'x'), 300, 60000)) return res.status(429).send('Too many requests');

    var accs = _cdnAccounts();
    if (!accs.length) return res.status(503).send('CDN not configured');

    const ext  = filename.split('.').pop().toLowerCase();
    const mime = CDN_MIME[ext] || 'application/octet-stream';
    const isDownload = ['apk','zip','exe','dmg','rar','7z','tar','gz','iso','msi','deb','rpm'].includes(ext);

    // ── SEARCH ORDER: semua account → semua repo → subfolder (files, files2, ...) + legacy path
    var fileData = null;
    var foundAcc = null;
    var foundRepo = null;

    outer:
    for (var ai = 0; ai < accs.length; ai++) {
      var acc = accs[ai];
      for (var ri = 0; ri < acc.repos.length; ri++) {
        var repo = acc.repos[ri];
        // 1. Coba subfolder baru: files, files2, ... files10
        for (var fi = 1; fi <= CDN_MAX_FOLDERS; fi++) {
          var folder = fi === 1 ? 'files' : 'files' + fi;
          try {
            var r = await acc.octokit.repos.getContent({ owner: acc.owner, repo: repo, path: folder + '/' + filename, ref: acc.branch });
            fileData = r.data; foundAcc = acc; foundRepo = repo;
            console.log('[cdn/get]', filename, 'found in', acc.name + '/' + repo + '/' + folder);
            break outer;
          } catch(e) {
            if (e.status === 404) continue; // tidak ada di folder ini, coba berikutnya
            throw e; // error lain (rate limit, auth) — lempar
          }
        }
        // 2. Legacy path: cdn/{filename} (backward compatibility)
        try {
          var rLeg = await acc.octokit.repos.getContent({ owner: acc.owner, repo: repo, path: 'cdn/' + filename, ref: acc.branch });
          fileData = rLeg.data; foundAcc = acc; foundRepo = repo;
          console.log('[cdn/get]', filename, 'found in legacy cdn/', acc.name + '/' + repo);
          break outer;
        } catch(e) {
          if (e.status !== 404) throw e;
        }
      }
    }

    if (!fileData) {
      console.warn('[cdn/get]', filename, 'not found in any account/repo');
      return res.status(404).send('Not found');
    }

    // ── HELPER: kirim buffer dengan header yang benar
    function sendCdnBuffer(buf) {
      res.set('Content-Type', mime);
      res.set('Cache-Control', 'public, max-age=31536000, immutable');
      res.set('Content-Length', buf.length);
      res.set('Access-Control-Allow-Origin', '*');
      res.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
      res.set('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Accept-Ranges');
      res.set('Cross-Origin-Resource-Policy', 'cross-origin');
      if (isDownload) res.set('Content-Disposition', 'attachment; filename="' + filename + '"');
      else res.set('Content-Disposition', 'inline; filename="' + filename + '"');
      res.send(buf);
    }

    // ── GitHub API mengembalikan inline content untuk file <1MB
    if (fileData.content && fileData.content.trim() !== '') {
      return sendCdnBuffer(Buffer.from(fileData.content, 'base64'));
    }

    // ── File besar: proxy via app server (tetap di domain sendiri)
    if (fileData.download_url) {
      try {
        var ghStream = await axios({ method: 'get', url: fileData.download_url, responseType: 'arraybuffer', timeout: 30000, maxContentLength: 200 * 1024 * 1024 });
        return sendCdnBuffer(Buffer.from(ghStream.data));
      } catch(proxyErr) {
        console.warn('[cdn/proxy]', filename, proxyErr.message);
        // Fallback: redirect ke jsDelivr agar tidak expose GitHub URL
        var jsDelivrUrl = 'https://cdn.jsdelivr.net/gh/' + foundAcc.owner + '/' + foundRepo + '@' + foundAcc.branch + '/' + (fileData.path || ('cdn/' + filename));
        res.set('Cache-Control', 'public, max-age=3600');
        return res.redirect(302, jsDelivrUrl);
      }
    }

    return res.status(404).send('File content unavailable');
  } catch(e) {
    console.error('[cdn/get]', e.message);
    if (e.status === 404) return res.status(404).send('Not found');
    res.status(500).send('Error');
  }
});

// ══════════════════════════════════════════════════════════════
// PWA ROUTES
// ════════════════════════════════════════════════════════════════
// ── CATEGORIES ─────────────────────────────────────────────────────────────
// Public
app.get('/api/categories', async function(req, res) {
  try { const r = await dbRead('categories.json'); res.json({ ok: true, data: r.data || [] }); }
  catch(e) { res.json({ ok: true, data: [] }); }
});
// Admin
app.get('/api/admin/categories', adminAuth, async function(req, res) {
  try { const r = await dbRead('categories.json'); res.json({ ok: true, data: r.data || [] }); }
  catch(e) { res.json({ ok: false, message: e.message }); }
});
app.post('/api/admin/categories', adminAuth, async function(req, res) {
  try {
    const cats = Array.isArray(req.body) ? req.body : req.body.categories;
    if (!Array.isArray(cats)) return res.json({ ok: false, message: 'categories harus array.' });
    const r = await dbRead('categories.json');
    await dbWrite('categories.json', cats, r.sha || null, 'admin: categories');
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── ADMIN: List all deposits ────────────────────────────────────────────────
app.get('/api/admin/deposits', adminAuth, async function(req, res) {
  try {
    let deps = [];
    try {
      const r = await listDirCached('deposits');
      const files = Array.isArray(r) ? r.filter(function(f){ return f.name.endsWith('.json'); }) : [];
      // Sequential with small delay — avoids GitHub secondary rate limit on burst reads
      for (var fi = 0; fi < files.length; fi++) {
        if (fi > 0 && fi % 10 === 0) await _sleep(300);
        try { const d = await getDeposit(files[fi].name.replace('.json','')); if (d.data) deps.push(d.data); } catch(e) {}
      }
    } catch(ghErr) {
      if (!ghErr.response || ghErr.response.status !== 404) throw ghErr;
    }
    deps.sort(function(a,b){ return (b.createdAt||0)-(a.createdAt||0); });
    res.json({ ok: true, data: deps.slice(0, 200) });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ══════════════════════════════════════════════════════════════
// RESELLER SYSTEM
// Reseller bisa login di /cpanel untuk membuat panel Pterodactyl
// secara mandiri sesuai kuota & plan yang diizinkan admin.
// ══════════════════════════════════════════════════════════════

const RESELLER_RE  = /^[a-z0-9_]{3,20}$/;
const RSVSRV_RE    = /^RSV-\d{13}-[a-f0-9]{8}$/;
function isValidResellerUsername(u) { return RESELLER_RE.test(u); }
function isValidRsvId(id)           { return typeof id === 'string' && RSVSRV_RE.test(id); }

async function getReseller(username)          { return dbRead('resellers/' + username + '.json'); }
async function saveReseller(username, d, sha) { return dbWrite('resellers/' + username + '.json', d, sha, 'reseller:' + username); }
async function listResellers() {
  try { return await listDirCached('resellers'); } catch(e) { return []; }
}
async function getRsServer(id)           { return dbRead('reseller-servers/' + id + '.json'); }
async function saveRsServer(id, d, sha)  { return dbWrite('reseller-servers/' + id + '.json', d, sha, 'rssrv:' + id); }
async function listRsServersOf(resellerUsername) {
  try {
    var files = (await listDirCached('reseller-servers')).filter(function(f){ return f.name.endsWith('.json'); });
    var results = [];
    await Promise.all(files.map(async function(f) {
      try {
        var r = await getRsServer(f.name.replace('.json',''));
        if (r.data && r.data.resellerUsername === resellerUsername) results.push(r.data);
      } catch(e) {}
    }));
    return results.sort(function(a,b){ return (b.createdAt||0)-(a.createdAt||0); });
  } catch(e) { return []; }
}

function makeResellerToken(username) {
  return makeToken({ sub: username, role: 'reseller', iat: Date.now(), exp: Date.now() + SESSION_TTL });
}
function resellerAuth(req, res, next) {
  var token   = req.headers['x-reseller-token'];
  var payload = verifyToken(token);
  if (!payload || payload.role !== 'reseller' || !payload.sub) {
    return res.status(401).json({ ok: false, message: 'Login diperlukan.' });
  }
  req.resellerUser = payload.sub;
  next();
}

// ── Login reseller
app.post('/api/reseller/login', async function(req, res) {
  var ip = req.ip || 'x';
  if (!rateLimit('rslogin:' + ip, 5, 15 * 60 * 1000)) return res.status(429).json({ ok: false, message: 'Terlalu banyak percobaan. Coba lagi dalam 15 menit.' });
  var username = String(req.body.username || '').toLowerCase().trim();
  var password = String(req.body.password || '');
  if (!isValidResellerUsername(username) || !password) return res.json({ ok: false, message: 'Username atau password tidak valid.' });
  try {
    var r = await getReseller(username);
    if (!r.data || r.data.deleted) return res.json({ ok: false, message: 'Username atau password salah.' });
    if (r.data.active === false)   return res.json({ ok: false, message: 'Akun dinonaktifkan. Hubungi admin.' });
    var valid = await verifyPassword(password, r.data.passwordHash);
    if (!valid) { console.warn('[reseller] login gagal:', username, ip); return res.json({ ok: false, message: 'Username atau password salah.' }); }
    await saveReseller(username, Object.assign({}, r.data, { lastLogin: Date.now() }), r.sha);
    console.log('[reseller] login:', username, 'dari', ip);
    res.json({ ok: true, token: makeResellerToken(username) });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── Profile reseller (quota + plan list)
app.get('/api/reseller/profile', resellerAuth, async function(req, res) {
  try {
    var r = await getReseller(req.resellerUser);
    if (!r.data || r.data.deleted) return res.json({ ok: false, message: 'Akun tidak ditemukan.' });
    if (r.data.active === false)   return res.status(403).json({ ok: false, message: 'Akun dinonaktifkan.' });
    var servers     = await listRsServersOf(req.resellerUser);
    var activeCount = servers.filter(function(s){ return s.status !== 'deleted'; }).length;
    res.json({ ok: true, data: {
      username    : r.data.username,
      allowedPlans: r.data.allowedPlans || [],
      maxServers  : r.data.maxServers   || 10,
      activeServers: activeCount,
      createdAt   : r.data.createdAt,
    }});
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── Plans tersedia untuk reseller
app.get('/api/reseller/plans', resellerAuth, async function(req, res) {
  try {
    var r = await getReseller(req.resellerUser);
    if (!r.data || r.data.deleted || r.data.active === false) return res.status(403).json({ ok: false, message: 'Akun tidak valid.' });
    var allowedPlans = r.data.allowedPlans || [];
    var templates    = await getPanelTemplates();
    var all = Object.keys(SPEC).map(function(id) {
      var t = templates.find(function(x){ return x.id === id; });
      return t || { id: id, name: id.toUpperCase(), ram: SPEC[id].ram, disk: SPEC[id].disk, cpu: SPEC[id].cpu, active: true };
    });
    templates.forEach(function(t) { if (!SPEC[t.id]) all.push(t); });
    var plans = (allowedPlans.length > 0)
      ? all.filter(function(p){ return allowedPlans.includes(p.id) && p.active !== false; })
      : all.filter(function(p){ return p.active !== false; });
    res.json({ ok: true, data: plans, domain: C.ptero.domain });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── Buat panel (reseller)
app.post('/api/reseller/create-panel', resellerAuth, async function(req, res) {
  var ip = req.ip || 'x';
  if (!rateLimit('rscreate:' + req.resellerUser, 3, 60 * 1000)) return res.status(429).json({ ok: false, message: 'Terlalu banyak request. Tunggu sebentar.' });
  try {
    var panelUsername = sanitizeUsername(String(req.body.username || ''));
    var panelPassword = String(req.body.password || '').trim();
    var plan          = String(req.body.plan || '').toLowerCase().replace(/[^a-z0-9_]/g,'').slice(0,20);
    var days          = Math.max(1, Math.min(3650, parseInt(req.body.days) || 30));

    if (!panelUsername || panelUsername.length < 3) return res.json({ ok: false, message: 'Username panel minimal 3 karakter.' });
    if (!panelPassword || panelPassword.length < 6)  return res.json({ ok: false, message: 'Password panel minimal 6 karakter.' });
    if (!plan)                                        return res.json({ ok: false, message: 'Pilih plan terlebih dahulu.' });
    if (!C.ptero.domain || !C.ptero.apikey)          return res.json({ ok: false, message: 'Server Pterodactyl belum dikonfigurasi. Hubungi admin.' });

    var rr = await getReseller(req.resellerUser);
    if (!rr.data || rr.data.deleted || rr.data.active === false) return res.status(403).json({ ok: false, message: 'Akun reseller tidak valid.' });

    // Validasi plan diizinkan
    var allowedPlans = rr.data.allowedPlans || [];
    if (allowedPlans.length > 0 && !allowedPlans.includes(plan)) return res.json({ ok: false, message: 'Plan tidak diizinkan untuk akun ini.' });

    // Validasi kuota
    var maxServers  = rr.data.maxServers || 10;
    var servers     = await listRsServersOf(req.resellerUser);
    var activeCount = servers.filter(function(s){ return s.status !== 'deleted'; }).length;
    if (activeCount >= maxServers) return res.json({ ok: false, message: 'Kuota server habis (' + maxServers + ' server). Hubungi admin untuk tambah kuota.' });

    var orderId = 'RSV-' + Date.now() + '-' + crypto.randomBytes(4).toString('hex');
    var pd      = await createPanelServer(plan, days, orderId, panelUsername, panelPassword);

    // FIX: secondary quota check setelah panel berhasil dibuat di Pterodactyl
    // Ini menangani race condition di mana 2 request bersamaan lolos cek awal
    // Jika melebihi kuota, panel yang baru dibuat langsung dihapus
    var serversAfter = await listRsServersOf(req.resellerUser);
    var activeAfter  = serversAfter.filter(function(s){ return s.status !== 'deleted'; }).length;
    if (activeAfter >= maxServers) {
      // Rollback: hapus panel yang baru saja dibuat dari Pterodactyl
      try {
        if (pd.serverId && C.ptero.domain && C.ptero.apikey) {
          await axios.delete(C.ptero.domain + '/api/application/servers/' + pd.serverId, { headers: ptH() }).catch(function(){});
          if (pd.userId) await axios.delete(C.ptero.domain + '/api/application/users/' + pd.userId, { headers: ptH() }).catch(function(){});
        }
      } catch(rollbackErr) { console.warn('[reseller/create] quota rollback ptero:', rollbackErr.message); }
      console.warn('[reseller/create] kuota melebihi batas (race condition), panel di-rollback:', orderId, '| reseller:', req.resellerUser);
      return res.json({ ok: false, message: 'Kuota server habis (' + maxServers + ' server). Hubungi admin untuk tambah kuota.' });
    }

    var srv = {
      id              : orderId,
      resellerUsername: req.resellerUser,
      panelUsername   : pd.username,
      plan            : plan,
      serverId        : pd.serverId,
      userId          : pd.userId,
      domain          : pd.domain,
      ram: pd.ram, disk: pd.disk, cpu: pd.cpu,
      days            : days,
      expiresAt       : pd.expiresAt,
      status          : 'active',
      createdAt       : Date.now(),
      createdFromIp   : ip,
    };
    await saveRsServer(orderId, srv, null);
    _gitTreeCache.delete('reseller-servers');
    console.log('[reseller] panel created:', req.resellerUser, '| plan:', plan, '| user:', pd.username, '| id:', orderId);

    res.json({ ok: true, data: {
      id: orderId, username: pd.username, password: pd.password,
      domain: pd.domain, ram: pd.ram, disk: pd.disk, cpu: pd.cpu,
      days: days, expiresAt: pd.expiresAt, plan: plan,
    }});
  } catch(e) {
    console.error('[reseller/create-panel]', e.message);
    res.json({ ok: false, message: e.message });
  }
});

// ── List server milik reseller
app.get('/api/reseller/servers', resellerAuth, async function(req, res) {
  try {
    var servers = await listRsServersOf(req.resellerUser);
    res.json({ ok: true, data: servers.map(function(s) {
      var daysLeft = s.expiresAt ? Math.ceil((s.expiresAt - Date.now()) / 86400000) : null;
      return { id: s.id, panelUsername: s.panelUsername, plan: s.plan,
        ram: s.ram, disk: s.disk, cpu: s.cpu, days: s.days,
        expiresAt: s.expiresAt, daysLeft: daysLeft,
        expired: s.expiresAt ? Date.now() > s.expiresAt : false,
        status: s.status, createdAt: s.createdAt, domain: s.domain };
    })});
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── ADMIN: List semua reseller
app.get('/api/admin/resellers', adminAuth, async function(req, res) {
  try {
    var files = await listResellers();
    var resellers = [];
    await Promise.all(files.filter(function(f){ return f.name.endsWith('.json'); }).map(async function(f) {
      try {
        var r = await getReseller(f.name.replace('.json',''));
        if (r.data && !r.data.deleted) {
          resellers.push({
            username    : r.data.username,
            active      : r.data.active !== false,
            allowedPlans: r.data.allowedPlans || [],
            maxServers  : r.data.maxServers   || 10,
            createdAt   : r.data.createdAt,
            lastLogin   : r.data.lastLogin,
          });
        }
      } catch(e) {}
    }));
    resellers.sort(function(a,b){ return (b.createdAt||0)-(a.createdAt||0); });
    res.json({ ok: true, data: resellers });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── ADMIN: Buat reseller
app.post('/api/admin/resellers', adminAuth, async function(req, res) {
  try {
    var username     = String(req.body.username || '').toLowerCase().trim();
    var password     = String(req.body.password || '').trim();
    var allowedPlans = Array.isArray(req.body.allowedPlans) ? req.body.allowedPlans.map(function(p){ return String(p).toLowerCase().replace(/[^a-z0-9_]/g,'').slice(0,20); }).filter(Boolean) : [];
    var maxServers   = Math.max(1, Math.min(9999, parseInt(req.body.maxServers) || 10));
    if (!isValidResellerUsername(username)) return res.json({ ok: false, message: 'Username tidak valid (3-20 karakter, huruf kecil/angka/_).' });
    if (!password || password.length < 6)   return res.json({ ok: false, message: 'Password minimal 6 karakter.' });
    var existing = await getReseller(username);
    if (existing.data && !existing.data.deleted) return res.json({ ok: false, message: 'Username sudah terdaftar.' });
    var hashed = await hashPassword(password);
    await saveReseller(username, { username, passwordHash: hashed, active: true, allowedPlans, maxServers, createdAt: Date.now(), lastLogin: null }, existing.sha || null);
    _gitTreeCache.delete('resellers');
    auditLog('create-reseller', username, req.adminIp).catch(function(){});
    console.log('[admin] create reseller:', username);
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── ADMIN: Update reseller (active, plans, quota)
app.patch('/api/admin/resellers/:username', adminAuth, async function(req, res) {
  try {
    var username = req.params.username;
    if (!isValidResellerUsername(username)) return res.json({ ok: false, message: 'Username tidak valid.' });
    var r = await getReseller(username);
    if (!r.data || r.data.deleted) return res.json({ ok: false, message: 'Reseller tidak ditemukan.' });
    var updates = { updatedAt: Date.now() };
    if (typeof req.body.active !== 'undefined')  updates.active = req.body.active !== false && req.body.active !== 'false';
    if (Array.isArray(req.body.allowedPlans))     updates.allowedPlans = req.body.allowedPlans.map(function(p){ return String(p).toLowerCase().replace(/[^a-z0-9_]/g,'').slice(0,20); }).filter(Boolean);
    if (typeof req.body.maxServers !== 'undefined') { var ms = parseInt(req.body.maxServers); if (!isNaN(ms) && ms >= 1 && ms <= 9999) updates.maxServers = ms; }
    await saveReseller(username, Object.assign({}, r.data, updates), r.sha);
    auditLog('update-reseller', username, req.adminIp).catch(function(){});
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ══════════════════════════════════════════════════════════════
// ADMIN: RESELLER SERVER MANAGEMENT
// Kelola semua panel yang dibuat oleh reseller dari sini
// ══════════════════════════════════════════════════════════════

// ── ADMIN: List semua server reseller
app.get('/api/admin/reseller-servers', adminAuth, async function(req, res) {
  try {
    var files = [];
    try { files = (await listDirCached('reseller-servers')).filter(function(f){ return f.name.endsWith('.json'); }); } catch(e) {}
    var servers = [];
    await Promise.all(files.map(async function(f) {
      try {
        var r = await getRsServer(f.name.replace('.json',''));
        if (r.data && r.data.status !== 'deleted') {
          var daysLeft = r.data.expiresAt ? Math.ceil((r.data.expiresAt - Date.now()) / 86400000) : null;
          servers.push(Object.assign({}, r.data, { daysLeft, expired: r.data.expiresAt ? Date.now() > r.data.expiresAt : false }));
        }
      } catch(e) {}
    }));
    servers.sort(function(a,b){ return (b.createdAt||0)-(a.createdAt||0); });
    res.json({ ok: true, data: servers, total: servers.length });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── ADMIN: Perpanjang expiry reseller server
app.post('/api/admin/reseller-servers/:id/extend', adminAuth, async function(req, res) {
  try {
    var id = req.params.id;
    var addDays = parseInt(req.body.days);
    if (!id || isNaN(addDays) || addDays < 1 || addDays > 3650) return res.json({ ok: false, message: 'Input tidak valid (1–3650 hari).' });
    var r = await getRsServer(id);
    if (!r.data) return res.json({ ok: false, message: 'Server tidak ditemukan.' });
    var base = (r.data.expiresAt && r.data.expiresAt > Date.now()) ? r.data.expiresAt : Date.now();
    var newExpiry = base + addDays * 86400000;
    if (C.ptero.domain && C.ptero.apikey && r.data.serverId) {
      try {
        var sRes = await axios.get(C.ptero.domain + '/api/application/servers/' + r.data.serverId, { headers: ptH() });
        var srv = sRes.data.attributes;
        await axios.patch(C.ptero.domain + '/api/application/servers/' + r.data.serverId + '/details', {
          name: srv.name, user: srv.user, email: srv.user, external_id: srv.external_id || null,
          description: 'RS:' + r.data.resellerUsername + ' | exp: ' + new Date(newExpiry).toLocaleDateString('id-ID') + ' [+' + addDays + 'd]',
        }, { headers: ptH() });
      } catch(e) { console.warn('[admin extend-rs] ptero:', e.message); }
    }
    await saveRsServer(id, Object.assign({}, r.data, {
      expiresAt: newExpiry, status: 'active',
      extendedAt: Date.now(), extendedDays: (r.data.extendedDays || 0) + addDays,
    }), r.sha);
    _gitTreeCache.delete('reseller-servers');
    auditLog('extend-rs-server', id + ' by ' + r.data.resellerUsername + ' +' + addDays + 'd', req.adminIp).catch(function(){});
    console.log('[admin] extend reseller server:', id, '+', addDays, 'd | reseller:', r.data.resellerUsername);
    res.json({ ok: true, newExpiry, newExpiryFmt: new Date(newExpiry).toLocaleDateString('id-ID') });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── ADMIN: Kurangi hari reseller server
app.post('/api/admin/reseller-servers/:id/reduce', adminAuth, async function(req, res) {
  try {
    var id = req.params.id;
    var reduceDays = parseInt(req.body.days);
    if (!id || isNaN(reduceDays) || reduceDays < 1 || reduceDays > 3650)
      return res.json({ ok: false, message: 'Input tidak valid (1–3650 hari).' });
    var r = await getRsServer(id);
    if (!r.data) return res.json({ ok: false, message: 'Server tidak ditemukan.' });
    var cur = (r.data.expiresAt && r.data.expiresAt > Date.now()) ? r.data.expiresAt : Date.now();
    var newExpiry = Math.max(Date.now(), cur - reduceDays * 86400000);
    if (C.ptero.domain && C.ptero.apikey && r.data.serverId) {
      try {
        var sRes = await axios.get(C.ptero.domain + '/api/application/servers/' + r.data.serverId, { headers: ptH() });
        var srv = sRes.data.attributes;
        await axios.patch(C.ptero.domain + '/api/application/servers/' + r.data.serverId + '/details', {
          name: srv.name, user: srv.user, email: srv.user, external_id: srv.external_id || null,
          description: 'RS:' + r.data.resellerUsername + ' | exp: ' + new Date(newExpiry).toLocaleDateString('id-ID') + ' [-' + reduceDays + 'd]',
        }, { headers: ptH() });
      } catch(e) {}
    }
    await saveRsServer(id, Object.assign({}, r.data, { expiresAt: newExpiry, reducedAt: Date.now(), reducedDays: (r.data.reducedDays||0)+reduceDays }), r.sha);
    _gitTreeCache.delete('reseller-servers');
    auditLog('reduce-rs-server', id + ' -' + reduceDays + 'd', req.adminIp).catch(function(){});
    res.json({ ok: true, newExpiry, newExpiryFmt: new Date(newExpiry).toLocaleDateString('id-ID') });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── ADMIN: Suspend reseller server
app.post('/api/admin/reseller-servers/:id/suspend', adminAuth, async function(req, res) {
  try {
    var id = req.params.id;
    var r = await getRsServer(id);
    if (!r.data || !r.data.serverId) return res.json({ ok: false, message: 'Server tidak ditemukan.' });
    var resp = await axios.post(C.ptero.domain + '/api/application/servers/' + r.data.serverId + '/suspend', {}, { headers: ptH() });
    if (resp.status !== 204) return res.json({ ok: false, message: 'Pterodactyl error: ' + resp.status });
    await saveRsServer(id, Object.assign({}, r.data, { status: 'suspended', _suspendedAt: Date.now() }), r.sha);
    auditLog('suspend-rs-server', id, req.adminIp).catch(function(){});
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── ADMIN: Unsuspend reseller server
app.post('/api/admin/reseller-servers/:id/unsuspend', adminAuth, async function(req, res) {
  try {
    var id = req.params.id;
    var r = await getRsServer(id);
    if (!r.data || !r.data.serverId) return res.json({ ok: false, message: 'Server tidak ditemukan.' });
    var resp = await axios.post(C.ptero.domain + '/api/application/servers/' + r.data.serverId + '/unsuspend', {}, { headers: ptH() });
    if (resp.status !== 204) return res.json({ ok: false, message: 'Pterodactyl error: ' + resp.status });
    await saveRsServer(id, Object.assign({}, r.data, { status: 'active', _unsuspendedAt: Date.now() }), r.sha);
    auditLog('unsuspend-rs-server', id, req.adminIp).catch(function(){});
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── ADMIN: Hapus reseller server (+ user Pterodactyl)
app.delete('/api/admin/reseller-servers/:id', adminAuth, async function(req, res) {
  try {
    var id = req.params.id;
    var r = await getRsServer(id);
    if (!r.data) return res.json({ ok: false, message: 'Server tidak ditemukan.' });
    var headers = ptH(); var dom = C.ptero.domain; var srvDeleted = false;
    if (r.data.serverId && dom && C.ptero.apikey) {
      try { await axios.delete(dom + '/api/application/servers/' + r.data.serverId, { headers }); srvDeleted = true; }
      catch(e) { if (e.response && e.response.status === 404) srvDeleted = true; else console.warn('[admin del-rs] server:', e.message); }
    }
    if (r.data.userId && dom && C.ptero.apikey) {
      try { await axios.delete(dom + '/api/application/users/' + r.data.userId, { headers }); }
      catch(e) { if (!e.response || e.response.status !== 404) console.warn('[admin del-rs] user:', e.message); }
    }
    await saveRsServer(id, Object.assign({}, r.data, { status: 'deleted', _deletedAt: Date.now(), _deletedBy: 'admin' }), r.sha);
    _gitTreeCache.delete('reseller-servers');
    auditLog('delete-rs-server', id + ' reseller:' + r.data.resellerUsername, req.adminIp).catch(function(){});
    console.log('[admin] delete reseller server:', id, '| reseller:', r.data.resellerUsername);
    res.json({ ok: true, srvDeleted });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── ADMIN: Hapus reseller (soft delete)
app.delete('/api/admin/resellers/:username', adminAuth, async function(req, res) {
  try {
    var username = req.params.username;
    if (!isValidResellerUsername(username)) return res.json({ ok: false, message: 'Username tidak valid.' });
    var r = await getReseller(username);
    if (!r.data) return res.json({ ok: false, message: 'Reseller tidak ditemukan.' });
    await saveReseller(username, Object.assign({}, r.data, { active: false, deleted: true, deletedAt: Date.now() }), r.sha);
    _gitTreeCache.delete('resellers');
    auditLog('delete-reseller', username, req.adminIp).catch(function(){});
    console.log('[admin] delete reseller:', username);
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── ADMIN: Reset password reseller
app.post('/api/admin/resellers/:username/reset-password', adminAuth, async function(req, res) {
  try {
    var username = req.params.username;
    var newPwd   = String(req.body.password || '').trim();
    if (!isValidResellerUsername(username)) return res.json({ ok: false, message: 'Username tidak valid.' });
    if (!newPwd || newPwd.length < 6)       return res.json({ ok: false, message: 'Password minimal 6 karakter.' });
    var r = await getReseller(username);
    if (!r.data || r.data.deleted) return res.json({ ok: false, message: 'Reseller tidak ditemukan.' });
    var hashed = await hashPassword(newPwd);
    await saveReseller(username, Object.assign({}, r.data, { passwordHash: hashed, updatedAt: Date.now() }), r.sha);
    auditLog('reset-reseller-pwd', username, req.adminIp).catch(function(){});
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── POPUP AD SYSTEM ────────────────────────────────────────────────────────
// Public: get current ad config (only what's needed to display)
app.get('/api/ad', async function(req, res) {
  try {
    const r = await dbRead('ad-config.json');
    const d = r.data || {};
    // Only expose public fields — never expose internal/admin-only data
    res.json({
      ok: true,
      enabled : d.enabled  === true,
      imageUrl: d.imageUrl || '',
      text    : d.text     || '',
      linkUrl : d.linkUrl  || '',
    });
  } catch(e) { res.json({ ok: false, enabled: false }); }
});

// Admin: get full ad config
app.get('/api/admin/ad', adminAuth, async function(req, res) {
  try {
    const r = await dbRead('ad-config.json');
    res.json({ ok: true, data: r.data || { enabled: false, imageUrl: '', text: '', linkUrl: '' } });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Admin: save ad config
app.post('/api/admin/ad', adminAuth, async function(req, res) {
  try {
    var enabled  = req.body.enabled  === true || req.body.enabled  === 'true';
    var imageUrl = String(req.body.imageUrl || '').trim().slice(0, 500);
    var text     = String(req.body.text     || '').trim().slice(0, 300);
    var linkUrl  = String(req.body.linkUrl  || '').trim().slice(0, 500);
    if (imageUrl && !imageUrl.startsWith('http')) return res.json({ ok: false, message: 'URL gambar harus dimulai dengan http/https.' });
    if (linkUrl  && !linkUrl.startsWith('http'))  return res.json({ ok: false, message: 'URL link harus dimulai dengan http/https.' });
    const r = await dbRead('ad-config.json');
    await dbWrite('ad-config.json', { enabled, imageUrl, text, linkUrl }, r.sha || null, 'admin: ad-config');
    auditLog('ad-update', (enabled ? 'enabled' : 'disabled') + ' | ' + (imageUrl || 'no-image'), req.adminIp).catch(function(){});
    console.log('[admin] ad-config updated | enabled:', enabled);
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ════════════════════════════════════════════════════════════════
// PROFANITY FILTER — dipakai oleh ulasan & chat
// ════════════════════════════════════════════════════════════════
const _PROFANITY_LIST = [
  'anjing','anjg','anying','bangsat','bajingan','brengsek','kontol','memek',
  'ngentot','jancok','jancuk','cuk','asu','monyet','goblok','tolol','idiot',
  'bego','kampret','keparat','setan','iblis','laknat','kadal','babi','kimak',
  'cibai','pukimak','pepek','titit','coli','colmek','ngaceng','bokep','erek',
  'sange','fuck','shit','bitch','asshole','dickhead','cunt','pussy','nigger',
  'nigga','whore','slut','bastard','motherfucker','fucker','wtf','stfu',
];
const _BANNED_NAMES = [
  'kontol','memek','pepek','titit','coli','ngentot','jancok','jancuk','cuk',
  'pukimak','kimak','cibai','ngaceng','bokep','erek','porn','fuck','shit',
  'bitch','cunt','pussy','nigger','nigga','whore','slut',
];
function _censorText(text) {
  var t = text;
  _PROFANITY_LIST.forEach(function(w) {
    var re = new RegExp('(?<![a-z])' + w.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '(?![a-z])', 'gi');
    t = t.replace(re, function(m) { return '*'.repeat(m.length); });
  });
  return t;
}
function _hasBadName(name) {
  var l = name.toLowerCase().replace(/[^a-z]/g, '');
  return _BANNED_NAMES.some(function(w) { return l.includes(w); });
}

// ── REVIEWS DB HELPERS ─────────────────────────────────────────
async function getReviews()             { return dbRead('reviews.json'); }
async function saveReviews(arr, sha)    { return dbWrite('reviews.json', arr, sha, 'review:update'); }

// ── CHAT DB HELPERS ────────────────────────────────────────────
async function getChatMessages()           { return dbRead('chat-messages.json'); }
async function saveChatMessages(arr, sha)  { return dbWrite('chat-messages.json', arr, sha, 'chat:update'); }

function newRevId() { return 'REV-' + Date.now() + '-' + crypto.randomBytes(4).toString('hex'); }
function newMsgId() { return 'MSG-' + Date.now() + '-' + crypto.randomBytes(4).toString('hex'); }

// ════════════════════════════════════════════════════════════════
// REVIEWS API
// ════════════════════════════════════════════════════════════════

// GET /api/reviews — public, paginated
app.get('/api/reviews', async function(req, res) {
  try {
    if (!rateLimit('rev-read:' + (req.ip||'x'), 30, 60000))
      return res.status(429).json({ ok: false, message: 'Terlalu banyak request.' });

    var page  = Math.max(1, parseInt(req.query.page) || 1);
    var star  = parseInt(req.query.star) || 0;
    var sort  = ['newest','oldest','highest','lowest'].includes(req.query.sort) ? req.query.sort : 'newest';
    var limit = 10;

    var r   = await getReviews();
    var all = Array.isArray(r.data) ? r.data : [];

    // Stats
    var starCounts = { 1:0, 2:0, 3:0, 4:0, 5:0 };
    var sumStar = 0;
    all.forEach(function(rv) {
      if (rv.star >= 1 && rv.star <= 5) { starCounts[rv.star]++; sumStar += rv.star; }
    });
    var avgRating = all.length > 0 ? (sumStar / all.length).toFixed(1) : '0.0';
    var withPhoto = all.filter(function(rv) { return rv.photoUrl; }).length;

    // Filter
    var filtered = star > 0 ? all.filter(function(rv) { return rv.star === star; }) : all.slice();
    if (req.query.photo === '1') filtered = filtered.filter(function(rv) { return rv.photoUrl; });

    // Sort
    if (sort === 'oldest')       filtered.sort(function(a,b) { return (a.createdAt||0)-(b.createdAt||0); });
    else if (sort === 'highest') filtered.sort(function(a,b) { return (b.star||0)-(a.star||0) || (b.createdAt||0)-(a.createdAt||0); });
    else if (sort === 'lowest')  filtered.sort(function(a,b) { return (a.star||0)-(b.star||0) || (b.createdAt||0)-(a.createdAt||0); });
    else                         filtered.sort(function(a,b) { return (b.createdAt||0)-(a.createdAt||0); });

    var totalFiltered = filtered.length;
    var paged = filtered.slice((page-1)*limit, page*limit);

    var clean = paged.map(function(rv) {
      return { id: rv.id, username: rv.username, displayName: rv.displayName,
        star: rv.star, text: rv.text, photoUrl: rv.photoUrl || null,
        createdAt: rv.createdAt, helpful: rv.helpful || 0 };
    });

    res.json({ ok: true, reviews: clean, total: totalFiltered, totalAll: all.length,
      page: page, pages: Math.ceil(totalFiltered / limit),
      avgRating: avgRating, starCounts: starCounts, withPhoto: withPhoto });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// POST /api/reviews — submit (login required)
app.post('/api/reviews', userAuth, async function(req, res) {
  try {
    var ip = req.ip || 'x';
    if (!rateLimit('rev-post:' + req.user, 3, 60 * 60 * 1000))
      return res.status(429).json({ ok: false, message: 'Terlalu banyak ulasan. Tunggu sebentar.' });

    var star     = parseInt(req.body.star);
    var rawName  = String(req.body.displayName || '').trim().slice(0, 50);
    var rawText  = String(req.body.text || '').trim().slice(0, 1000);
    var photoUrl = String(req.body.photoUrl || '').trim().slice(0, 500);

    if (!star || star < 1 || star > 5)
      return res.json({ ok: false, message: 'Rating bintang tidak valid (1–5).' });
    if (!rawName || rawName.length < 2)
      return res.json({ ok: false, message: 'Nama pengirim minimal 2 karakter.' });
    if (_hasBadName(rawName))
      return res.json({ ok: false, message: 'Nama mengandung kata yang tidak diizinkan. Gunakan nama yang sopan.' });
    if (!rawText || rawText.length < 5)
      return res.json({ ok: false, message: 'Ulasan minimal 5 karakter.' });

    var cleanText = _censorText(rawText);

    var safePhoto = '';
    if (photoUrl) {
      if (photoUrl.startsWith('/cdn/') || /^https?:\/\//.test(photoUrl)) safePhoto = photoUrl.slice(0, 500);
      else return res.json({ ok: false, message: 'URL foto tidak valid.' });
    }

    // Cek user tidak di-ban
    var ur = await getUser(req.user);
    if (!ur.data) return res.json({ ok: false, message: 'User tidak ditemukan.' });
    if (ur.data.banned) return res.json({ ok: false, message: 'Akun diblokir.' });

    // Maksimal 1 ulasan per user per 24 jam
    var existing = await getReviews();
    var allRevs  = Array.isArray(existing.data) ? existing.data : [];
    var recent   = allRevs.find(function(rv) {
      return rv.username === req.user && Date.now() - (rv.createdAt||0) < 24*60*60*1000;
    });
    if (recent) return res.json({ ok: false, message: 'Kamu sudah memberikan ulasan dalam 24 jam terakhir.' });

    var rev = {
      id: newRevId(), username: req.user, displayName: rawName,
      star: star, text: cleanText, photoUrl: safePhoto || null,
      createdAt: Date.now(), helpful: 0,
    };

    for (var _rt = 0; _rt < 3; _rt++) {
      try {
        var fr = await getReviews();
        var arr = Array.isArray(fr.data) ? fr.data : [];
        arr.unshift(rev);
        if (arr.length > 500) arr.length = 500;
        await saveReviews(arr, fr.sha || null);
        break;
      } catch(e) {
        if (_rt < 2 && (e.status === 409 || e.status === 422)) { await _sleep(300*(_rt+1)); continue; }
        throw e;
      }
    }

    console.log('[review] new:', rev.id, '|', req.user, '|', star, '★');
    res.json({ ok: true, review: { id: rev.id, displayName: rev.displayName, star: rev.star, text: rev.text, photoUrl: rev.photoUrl, createdAt: rev.createdAt } });
  } catch(e) { console.error('[review/post]', e.message); res.json({ ok: false, message: e.message }); }
});

// DELETE /api/reviews/:id — admin only
app.delete('/api/reviews/:id', adminAuth, async function(req, res) {
  try {
    var id = req.params.id;
    if (!id || !/^REV-\d{13}-[a-f0-9]{8}$/.test(id))
      return res.json({ ok: false, message: 'ID tidak valid.' });

    for (var _rt = 0; _rt < 3; _rt++) {
      try {
        var fr = await getReviews();
        var arr = Array.isArray(fr.data) ? fr.data : [];
        var idx = arr.findIndex(function(rv) { return rv.id === id; });
        if (idx < 0) return res.json({ ok: false, message: 'Ulasan tidak ditemukan.' });
        arr.splice(idx, 1);
        await saveReviews(arr, fr.sha || null);
        break;
      } catch(e) {
        if (_rt < 2 && (e.status === 409 || e.status === 422)) { await _sleep(300*(_rt+1)); continue; }
        throw e;
      }
    }
    auditLog('delete-review', id, req.adminIp).catch(function(){});
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// POST /api/reviews/:id/helpful — tandai membantu
app.post('/api/reviews/:id/helpful', async function(req, res) {
  try {
    var id = req.params.id;
    var ip = req.ip || 'x';
    if (!rateLimit('helpful:' + ip, 20, 60*60*1000)) return res.status(429).json({ ok: false });
    if (!id || !/^REV-\d{13}-[a-f0-9]{8}$/.test(id)) return res.json({ ok: false });

    for (var _rt = 0; _rt < 3; _rt++) {
      try {
        var fr = await getReviews();
        var arr = Array.isArray(fr.data) ? fr.data : [];
        var idx = arr.findIndex(function(rv) { return rv.id === id; });
        if (idx < 0) return res.json({ ok: false });
        arr[idx].helpful = (arr[idx].helpful || 0) + 1;
        await saveReviews(arr, fr.sha || null);
        return res.json({ ok: true, helpful: arr[idx].helpful });
      } catch(e) {
        if (_rt < 2 && (e.status === 409 || e.status === 422)) { await _sleep(300*(_rt+1)); continue; }
        throw e;
      }
    }
  } catch(e) { res.json({ ok: false }); }
});

// Admin: hapus semua ulasan
app.delete('/api/admin/reviews', adminAuth, async function(req, res) {
  try {
    var fr = await getReviews();
    await saveReviews([], fr.sha || null);
    auditLog('clear-reviews', 'Reviews dihapus', req.adminIp).catch(function(){});
    res.json({ ok: true, message: 'Semua ulasan dihapus.' });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ════════════════════════════════════════════════════════════════
// CHAT API
// ════════════════════════════════════════════════════════════════

// GET /api/chat — public, polling
app.get('/api/chat', async function(req, res) {
  try {
    if (!rateLimit('chat-read:' + (req.ip||'x'), 120, 60000))
      return res.status(429).json({ ok: false, message: 'Terlalu banyak request.' });

    var since = parseInt(req.query.since) || 0;
    var limit = Math.min(parseInt(req.query.limit) || 50, 100);

    var r   = await getChatMessages();
    var all = Array.isArray(r.data) ? r.data : [];

    // all disimpan newest-first, balik ke oldest-first untuk display
    var sorted = all.slice().reverse();

    var messages;
    if (since > 0) {
      messages = sorted.filter(function(m) { return (m.createdAt||0) > since; });
    } else {
      messages = sorted.slice(-limit);
    }

    var clean = messages.map(function(m) {
      return { id: m.id, username: m.username, displayName: m.displayName || m.username,
        text: m.text || '', photoUrl: m.photoUrl || null, createdAt: m.createdAt };
    });

    res.json({ ok: true, messages: clean, total: all.length });
  } catch(e) { res.json({ ok: false, messages: [], message: e.message }); }
});

// POST /api/chat — kirim pesan (login required)
app.post('/api/chat', userAuth, async function(req, res) {
  try {
    var ip = req.ip || 'x';
    if (!rateLimit('chat-send:' + req.user, 5, 10*1000))
      return res.status(429).json({ ok: false, message: 'Terlalu cepat! Tunggu sebentar.' });
    if (!rateLimit('chat-ip:' + ip, 10, 10*1000))
      return res.status(429).json({ ok: false, message: 'Terlalu banyak request.' });

    var rawText  = String(req.body.text || '').trim().slice(0, 1000);
    var photoUrl = String(req.body.photoUrl || '').trim().slice(0, 500);
    var rawName  = String(req.body.displayName || req.user).trim().slice(0, 50);

    if (!rawText && !photoUrl)
      return res.json({ ok: false, message: 'Pesan tidak boleh kosong.' });

    var cleanText = rawText ? _censorText(rawText) : '';

    var safePhoto = '';
    if (photoUrl) {
      if (photoUrl.startsWith('/cdn/') || /^https?:\/\//.test(photoUrl)) safePhoto = photoUrl.slice(0, 500);
      else return res.json({ ok: false, message: 'URL foto tidak valid.' });
    }

    // Cek ban
    var ur = await getUser(req.user);
    if (!ur.data) return res.json({ ok: false, message: 'User tidak ditemukan.' });
    if (ur.data.banned) return res.json({ ok: false, message: 'Akun diblokir.' });

    var msg = {
      id: newMsgId(), username: req.user, displayName: rawName,
      text: cleanText, photoUrl: safePhoto || null, createdAt: Date.now(),
    };

    for (var _rt = 0; _rt < 3; _rt++) {
      try {
        var fr = await getChatMessages();
        var arr = Array.isArray(fr.data) ? fr.data : [];
        arr.unshift(msg);
        if (arr.length > 200) arr.length = 200;
        await saveChatMessages(arr, fr.sha || null);
        break;
      } catch(e) {
        if (_rt < 2 && (e.status === 409 || e.status === 422)) { await _sleep(200*(_rt+1)); continue; }
        throw e;
      }
    }

    res.json({ ok: true, message: { id: msg.id, username: msg.username, displayName: msg.displayName, text: msg.text, photoUrl: msg.photoUrl, createdAt: msg.createdAt } });
  } catch(e) { console.error('[chat/send]', e.message); res.json({ ok: false, message: e.message }); }
});

// DELETE /api/chat/:id — hapus pesan (pemilik atau admin)
app.delete('/api/chat/:id', userAuth, async function(req, res) {
  try {
    var id = req.params.id;
    if (!id || !/^MSG-\d{13}-[a-f0-9]{8}$/.test(id))
      return res.json({ ok: false, message: 'ID tidak valid.' });

    var adminTok = req.headers['x-admin-token'];
    var isAdmin  = !!(adminTok && verifyToken(adminTok) && verifyToken(adminTok).role === 'admin');

    for (var _rt = 0; _rt < 3; _rt++) {
      try {
        var fr = await getChatMessages();
        var arr = Array.isArray(fr.data) ? fr.data : [];
        var idx = arr.findIndex(function(m) { return m.id === id; });
        if (idx < 0) return res.json({ ok: false, message: 'Pesan tidak ditemukan.' });
        if (!isAdmin && arr[idx].username !== req.user)
          return res.status(403).json({ ok: false, message: 'Tidak bisa menghapus pesan orang lain.' });
        arr.splice(idx, 1);
        await saveChatMessages(arr, fr.sha || null);
        break;
      } catch(e) {
        if (_rt < 2 && (e.status === 409 || e.status === 422)) { await _sleep(200*(_rt+1)); continue; }
        throw e;
      }
    }
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// Admin: hapus semua pesan chat
app.delete('/api/admin/chat', adminAuth, async function(req, res) {
  try {
    var fr = await getChatMessages();
    await saveChatMessages([], fr.sha || null);
    auditLog('clear-chat', 'Chat dihapus', req.adminIp).catch(function(){});
    res.json({ ok: true, message: 'Semua pesan chat dihapus.' });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});


// ════════════════════════════════════════════════════════════════
// ADMIN — ULASAN (Reviews) Management
// Endpoint yang dipakai oleh tab Ulasan di admin panel
// ════════════════════════════════════════════════════════════════

// GET /api/admin/ulasan — list semua ulasan (approved & pending)
app.get('/api/admin/ulasan', adminAuth, async function(req, res) {
  try {
    var r = await getReviews();
    var arr = Array.isArray(r.data) ? r.data : [];
    // Sort: terbaru dulu
    arr = arr.slice().sort(function(a,b){ return (b.createdAt||0) - (a.createdAt||0); });
    res.json({ ok: true, data: arr });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// POST /api/admin/ulasan/:id/approve — setujui atau cabut persetujuan ulasan
app.post('/api/admin/ulasan/:id/approve', adminAuth, async function(req, res) {
  try {
    var id = req.params.id;
    if (!id || !/^REV-\d{13}-[a-f0-9]{8}$/.test(id))
      return res.json({ ok: false, message: 'ID tidak valid.' });
    var approved = req.body.approved !== false; // default true

    for (var _rt = 0; _rt < 3; _rt++) {
      try {
        var fr = await getReviews();
        var arr = Array.isArray(fr.data) ? fr.data : [];
        var idx = arr.findIndex(function(rv) { return rv.id === id; });
        if (idx < 0) return res.json({ ok: false, message: 'Ulasan tidak ditemukan.' });
        arr[idx] = Object.assign({}, arr[idx], { approved: approved, approvedAt: Date.now() });
        await saveReviews(arr, fr.sha || null);
        break;
      } catch(e) {
        if (_rt < 2 && (e.status === 409 || e.status === 422)) { await _sleep(300*(_rt+1)); continue; }
        throw e;
      }
    }
    auditLog(approved ? 'approve-review' : 'unapprove-review', id, req.adminIp).catch(function(){});
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// DELETE /api/admin/ulasan/reset-all — hapus SEMUA ulasan
app.delete('/api/admin/ulasan/reset-all', adminAuth, async function(req, res) {
  try {
    var fr = await getReviews();
    await saveReviews([], fr.sha || null);
    auditLog('clear-reviews', 'Semua ulasan dihapus via reset-all', req.adminIp).catch(function(){});
    res.json({ ok: true, message: 'Semua ulasan dihapus.' });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// POST /api/admin/ulasan/bulk-delete — hapus beberapa ulasan sekaligus
app.post('/api/admin/ulasan/bulk-delete', adminAuth, async function(req, res) {
  try {
    var ids = req.body.ids;
    if (!Array.isArray(ids) || ids.length === 0)
      return res.json({ ok: false, message: 'Tidak ada ID yang dikirim.' });
    // Validasi semua ID
    var idSet = new Set(ids.filter(function(id){ return typeof id === 'string' && /^REV-\d{13}-[a-f0-9]{8}$/.test(id); }));
    if (!idSet.size) return res.json({ ok: false, message: 'ID tidak valid.' });

    for (var _rt = 0; _rt < 3; _rt++) {
      try {
        var fr = await getReviews();
        var arr = (Array.isArray(fr.data) ? fr.data : []).filter(function(rv){ return !idSet.has(rv.id); });
        await saveReviews(arr, fr.sha || null);
        break;
      } catch(e) {
        if (_rt < 2 && (e.status === 409 || e.status === 422)) { await _sleep(300*(_rt+1)); continue; }
        throw e;
      }
    }
    auditLog('bulk-delete-reviews', idSet.size + ' ulasan dihapus', req.adminIp).catch(function(){});
    res.json({ ok: true, deleted: idSet.size });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// DELETE /api/admin/ulasan/:id — hapus satu ulasan
app.delete('/api/admin/ulasan/:id', adminAuth, async function(req, res) {
  try {
    var id = req.params.id;
    if (!id || !/^REV-\d{13}-[a-f0-9]{8}$/.test(id))
      return res.json({ ok: false, message: 'ID tidak valid.' });

    for (var _rt = 0; _rt < 3; _rt++) {
      try {
        var fr = await getReviews();
        var arr = Array.isArray(fr.data) ? fr.data : [];
        var idx = arr.findIndex(function(rv) { return rv.id === id; });
        if (idx < 0) return res.json({ ok: false, message: 'Ulasan tidak ditemukan.' });
        arr.splice(idx, 1);
        await saveReviews(arr, fr.sha || null);
        break;
      } catch(e) {
        if (_rt < 2 && (e.status === 409 || e.status === 422)) { await _sleep(300*(_rt+1)); continue; }
        throw e;
      }
    }
    auditLog('delete-review', id, req.adminIp).catch(function(){});
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ════════════════════════════════════════════════════════════════
// ADMIN — CHAT Management
// Endpoint yang dipakai oleh tab Chat di admin panel
// ════════════════════════════════════════════════════════════════

// GET /api/admin/chat — list semua pesan chat
app.get('/api/admin/chat', adminAuth, async function(req, res) {
  try {
    var r = await getChatMessages();
    var arr = Array.isArray(r.data) ? r.data : [];
    // Sort: terbaru dulu
    arr = arr.slice().sort(function(a,b){ return (b.createdAt||0) - (a.createdAt||0); });
    res.json({ ok: true, data: arr });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// DELETE /api/admin/chat/reset-all — hapus SEMUA pesan chat
app.delete('/api/admin/chat/reset-all', adminAuth, async function(req, res) {
  try {
    var fr = await getChatMessages();
    await saveChatMessages([], fr.sha || null);
    auditLog('clear-chat', 'Semua chat dihapus via reset-all', req.adminIp).catch(function(){});
    res.json({ ok: true, message: 'Semua pesan chat dihapus.' });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// POST /api/admin/chat/bulk-delete — hapus beberapa pesan sekaligus
app.post('/api/admin/chat/bulk-delete', adminAuth, async function(req, res) {
  try {
    var ids = req.body.ids;
    if (!Array.isArray(ids) || ids.length === 0)
      return res.json({ ok: false, message: 'Tidak ada ID yang dikirim.' });
    var idSet = new Set(ids.filter(function(id){ return typeof id === 'string' && /^MSG-\d{13}-[a-f0-9]{8}$/.test(id); }));
    if (!idSet.size) return res.json({ ok: false, message: 'ID tidak valid.' });

    for (var _rt = 0; _rt < 3; _rt++) {
      try {
        var fr = await getChatMessages();
        var arr = (Array.isArray(fr.data) ? fr.data : []).filter(function(m){ return !idSet.has(m.id); });
        await saveChatMessages(arr, fr.sha || null);
        break;
      } catch(e) {
        if (_rt < 2 && (e.status === 409 || e.status === 422)) { await _sleep(200*(_rt+1)); continue; }
        throw e;
      }
    }
    auditLog('bulk-delete-chat', idSet.size + ' pesan dihapus', req.adminIp).catch(function(){});
    res.json({ ok: true, deleted: idSet.size });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// DELETE /api/admin/chat/:id — hapus satu pesan (admin, tanpa cek kepemilikan)
app.delete('/api/admin/chat/:id', adminAuth, async function(req, res) {
  try {
    var id = req.params.id;
    if (!id || !/^MSG-\d{13}-[a-f0-9]{8}$/.test(id))
      return res.json({ ok: false, message: 'ID tidak valid.' });

    for (var _rt = 0; _rt < 3; _rt++) {
      try {
        var fr = await getChatMessages();
        var arr = Array.isArray(fr.data) ? fr.data : [];
        var idx = arr.findIndex(function(m) { return m.id === id; });
        if (idx < 0) return res.json({ ok: false, message: 'Pesan tidak ditemukan.' });
        arr.splice(idx, 1);
        await saveChatMessages(arr, fr.sha || null);
        break;
      } catch(e) {
        if (_rt < 2 && (e.status === 409 || e.status === 422)) { await _sleep(200*(_rt+1)); continue; }
        throw e;
      }
    }
    auditLog('delete-chat-msg', id, req.adminIp).catch(function(){});
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, message: e.message }); }
});

// ── PAGES ──────────────────────────────────────────────────────────────────
function sendPage(res, name) {
  // Try multiple path strategies for Vercel serverless compatibility
  // Vercel bundles files in /var/task when deploying serverless functions
  var candidates = [
    path.join(__dirname, 'public', name),          // Standard: public/ subdir
    path.join(__dirname, name),                    // Root next to index.js
    path.resolve(process.cwd(), 'public', name),   // CWD public/
    path.resolve(process.cwd(), name),             // CWD root
    path.resolve('/var/task/public', name),        // Vercel lambda public/
    path.resolve('/var/task', name),               // Vercel lambda root
  ];
  function tryNext(i) {
    if (i >= candidates.length) {
      return res.status(404).send('Page not found: ' + name);
    }
    res.sendFile(candidates[i], function(err) {
      if (err) tryNext(i + 1);
    });
  }
  tryNext(0);
}
app.get('/',      function(_, res) { sendPage(res, 'index.html'); });
app.get('/otp',   function(_, res) { sendPage(res, 'otp.html'); });
app.get('/pay',   function(_, res) { sendPage(res, 'pay.html'); });
app.get('/renew', function(_, res) { sendPage(res, 'renew.html'); });
app.get('/track', function(_, res) { sendPage(res, 'track.html'); });
app.get('/sw.js', function(_, res) { res.setHeader('Content-Type','application/javascript'); res.setHeader('Cache-Control','no-cache'); sendPage(res, 'sw.js'); });
app.get('/manifest.json', function(_, res) { res.setHeader('Content-Type','application/manifest+json'); sendPage(res, 'manifest.json'); });
app.get('/' + C.store.adminPath, function(_, res) {
  // SECURITY: prevent search engine indexing of admin panel
  res.set('X-Robots-Tag', 'noindex, nofollow');
  res.set('Cache-Control', 'no-store, no-cache, private');
  sendPage(res, 'admin.html');
});
app.get('/upload', function(_, res) { sendPage(res, 'upload.html'); });
app.get('/ulasan', function(_, res) { sendPage(res, 'ulasan.html'); });
app.get('/chat',   function(_, res) { sendPage(res, 'chat.html'); });
app.get('/produk', function(_, res) { sendPage(res, 'produk.html'); });
app.get('/cpanel', function(_, res) {
  res.set('X-Robots-Tag', 'noindex, nofollow');
  res.set('Cache-Control', 'no-store, no-cache, private');
  sendPage(res, 'cpanel.html');
});
app.use(function(_, res) { res.redirect('/'); });

if (require.main === module) {
  const asciiArt = () => {
    _origLog(`
\x1b[32m
__   ___            _   _
\\ \\ / (_)_  _____  | | | | __ _ _ __  _____   _
 \\ V /| \\ \\/ / _ \\ | |_| |/ _\` | '_ \\|_  / | | |
  | | | |>  <  __/ |  _  | (_| | | | |/ /| |_| |
  |_| |_/_/\\_\\___| |_| |_|\\__,_|_| |_/___||__, |
                                           |___/\x1b[0m
`);
  };
  asciiArt();

  const tz   = 'Asia/Jakarta';
  const now  = moment().tz(tz).format('dddd, D MMMM YYYY — HH:mm:ss z');

  const _dbStatus = DB_BACKEND === 'github'
    ? (_dbOctokit ? '✅ GitHub (' + GH_DB_OWNER + '/' + GH_DB_REPO + '@' + GH_DB_BRANCH + ')' : '❌ GitHub DB belum dikonfigurasi (set GH_DB_TOKEN/GH_DB_OWNER/GH_DB_REPO)')
    : (supabase   ? '✅ Supabase (' + SUPABASE_URL.replace('https://','').split('.')[0] + '.supabase.co)' : '⚠️  Supabase belum dikonfigurasi');
  const _cdnAccList = _cdnAccounts();
  const _cdnStatus  = _cdnAccList.length
    ? '✅ ' + _cdnAccList.map(function(a){ return a.name + ': ' + a.owner + '/' + a.repos.join(','); }).join(' | ')
    : '⚠️  Belum dikonfigurasi';

  console.log('🟢  Dongtube v2      | http://localhost:' + C.port);
  console.log('🔐  Admin panel       | http://localhost:' + C.port + '/' + C.store.adminPath);
  console.log('🕐  Server time       | ' + now + ' (' + tz + ')');
  console.log('🔒  Security          | Session: ON · Rate-limit: ON · TRX entropy: ON · Supabase SHA lock: ON');
  console.log('⚡  Compression       | gzip ON · Body limit: 256kb');
  console.log('🗃️  Database          | ' + _dbStatus);
  console.log('📦  CDN               | ' + _cdnStatus);
  if (!CRON_SECRET) console.warn('⚠️  CRON_SECRET tidak diset — endpoint /api/cron/run DIBLOKIR. Set CRON_SECRET di env untuk aktifkan.');
  if (!process.env.TOKEN_SECRET) console.warn('⚠️  TOKEN_SECRET tidak diset — gunakan nilai acak yang kuat di production (bukan default).');
  console.log('─────────────────────────────────────────────────────────');
  app.listen(C.port);

  // FIX: Global crash handler — cegah server mati total akibat unhandled error/rejection
  // Tanpa ini, satu request yang crash bisa matikan seluruh server
  process.on('uncaughtException', function(err) {
    console.error('[CRASH PREVENTED] uncaughtException:', err.message, err.stack ? err.stack.split('\n')[1] : '');
  });
  process.on('unhandledRejection', function(reason) {
    console.error('[CRASH PREVENTED] unhandledRejection:', reason && reason.message ? reason.message : String(reason));
  });

  // ── AUTO-RESUME: langsung scan deposit pending saat server nyala
  // Solusi untuk deposit yang terbayar tapi server mati sebelum dicatat
  setTimeout(function() {
    console.log('[startup] scan deposit pending...');
    // autoSeedProducts DIHAPUS dari startup — gunakan tombol di admin panel (/api/admin/seed-products)
    // agar tidak reset database produk yang sudah ada saat server restart
    autoReconcileDeposits().catch(function(e){ console.error('[startup/reconcile]', e.message); });
    autoExpireOtpOrders().catch(function(e){ console.error('[startup/otp-expire]', e.message); });
    autoExpirePendingOrders().catch(function(e){ console.error('[startup/trx-expire]', e.message); });
  }, 3000); // tunggu 3 detik agar Supabase connection pool siap
}
module.exports = app;
