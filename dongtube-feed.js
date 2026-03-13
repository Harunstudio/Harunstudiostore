/*
        ••JANGAN HAPUS INI••
Di record ulang oleh © Yixe Hanzy
•• recode kasih credits 
•• contacts: (6283143961588)
•• tele t.me/hanzyy001 
•• (github.com/hanzywaifu-coder) 

• Menerima pembuatan script bot
• Menerima perbaikan script atau fitur bot
• Menerima pembuatan fitur bot
• Menerima semua kebutuhan bot
• Menerima Jadi Bot

"Dan janganlah kamu makan harta di antara kamu dengan jalan yang batil, dan janganlah kamu membunuh dirimu sendiri. Sesungguhnya Allah adalah Maha Penyayang kepadamu." (QS. Al-Baqarah: 188)
*/



/*
 * ═══════════════════════════════════════════════
 *   DONGTUBE FEED NOTIFIER
 *   Polling /api/feed tiap 10 detik
 *   Kirim notif otomatis ke Saluran WhatsApp
 * ═══════════════════════════════════════════════
 *
 * CARA PAKAI:
 * 1. Tambah di settings.js:
 *      global.dongtube_store_url = 'https://toko-kamu.vercel.app'
 *      // opsional, kalau feed butuh secret (default tidak perlu)
 *      // global.dongtube_feed_secret = 'secret123'
 *
 * 2. Di alipai-cmd.js, di bawah baris require('./settings'):
 *      const startDongtubeFeed = require('./dongtube-feed')
 *
 * 3. Di bagian setupPakasirWebhook / inisialisasi bot (sekitar baris 960):
 *      if (!global.dongtubeFeedInitialized) {
 *          startDongtubeFeed(alip)
 *          global.dongtubeFeedInitialized = true
 *      }
 */

const axios = require('axios')
const fs    = require('fs')

// File untuk menyimpan ID event terakhir yang sudah dikirim
// Agar tidak double-notif saat bot restart
const SEEN_PATH = './library/database/dongtube_seen.json'

// FIX BUG #3: Cache seen IDs di memory agar tidak baca file dari disk tiap event
let _seenCache = null

function loadSeen() {
    if (_seenCache) return _seenCache
    try {
        _seenCache = JSON.parse(fs.readFileSync(SEEN_PATH, 'utf8'))
    } catch {
        _seenCache = { ids: [] }
    }
    return _seenCache
}

function saveSeen(data) {
    try {
        if (!fs.existsSync('./library/database')) {
            fs.mkdirSync('./library/database', { recursive: true })
        }
        fs.writeFileSync(SEEN_PATH, JSON.stringify(data, null, 2))
    } catch (e) {}
}

function addSeen(id) {
    const data = loadSeen()
    if (!data.ids.includes(id)) {
        data.ids.unshift(id)
        // Simpan max 200 ID terakhir biar file tidak membesar
        data.ids = data.ids.slice(0, 200)
        saveSeen(data)
    }
}

function isSeen(id) {
    return loadSeen().ids.includes(id)
}

// Format Rupiah
const idr = (n) => 'Rp' + Number(n || 0).toLocaleString('id-ID')

// Format pesan per tipe event
// FIX BUG #1: Tipe yang benar dari /api/feed adalah: 'order' | 'deposit' | 'sewabot' | 'otp'
//             Plus tipe baru: 'new_order' | 'new_otp_order'
//             Bukan 'trx_completed' / 'new_order_admin' — itu adalah tipe internal server sebelum diformat
// FIX BUG #2: Field yang tersedia dari /api/feed HANYA: id, type, ts, label, amount
//             Tidak ada: productName, variantName, username — gunakan event.label saja
function formatPesan(event) {
    const now = new Date().toLocaleString('id-ID', { timeZone: 'Asia/Jakarta' })
    const { type } = event

    if (type === 'new_order') {
        return `🛒 *ORDER MASUK*\n\n`
            + `📦 *${event.label || '-'}*\n`
            + `💰 ${idr(event.amount)}\n`
            + `🆔 \`${event.id}\`\n`
            + `🕒 ${now}`

    } else if (type === 'order') {
        return `✅ *ORDER SELESAI*\n\n`
            + `📦 *${event.label || '-'}*\n`
            + `💰 ${idr(event.amount)}\n`
            + (event.phone ? `📱 ${event.phone}\n` : '')
            + `🆔 \`${event.id}\`\n`
            + `🕒 ${now}`

    } else if (type === 'deposit') {
        return `💳 *DEPOSIT MASUK*\n\n`
            + `📝 ${event.label || '-'}\n`
            + `💰 ${idr(event.amount)}\n`
            + `🆔 \`${event.id}\`\n`
            + `🕒 ${now}`

    } else if (type === 'sewabot') {
        return `🤖 *SEWA BOT LUNAS*\n\n`
            + `📝 ${event.label || '-'}\n`
            + `💰 ${idr(event.amount)}\n`
            + `🆔 \`${event.id}\`\n`
            + `🕒 ${now}`

    } else if (type === 'new_otp_order') {
        return `📱 *ORDER OTP MASUK*\n\n`
            + `📝 ${event.label || '-'}\n`
            + `💰 ${idr(event.amount)}\n`
            + (event.phone ? `📞 ${event.phone}\n` : '')
            + `🆔 \`${event.id}\`\n`
            + `🕒 ${now}`

    } else if (type === 'otp') {
        return `✅ *OTP DITERIMA*\n\n`
            + `📝 ${event.label || '-'}\n`
            + `💰 ${idr(event.amount)}\n`
            + (event.phone ? `📞 ${event.phone}\n` : '')
            + `🆔 \`${event.id}\`\n`
            + `🕒 ${now}`

    } else {
        // Tipe tidak dikenal — skip
        return null
    }
}

async function pollFeed(alip) {
    const BASE_URL = (global.dongtube_store_url || '').replace(/\/$/, '')
    if (!BASE_URL) {
        console.warn('[Dongtube Feed] ⚠️  global.dongtube_store_url belum diset di settings.js!')
        return
    }

    const saluranJid = global.idSaluran
    if (!saluranJid) {
        console.warn('[Dongtube Feed] ⚠️  global.idSaluran belum diset! Notif tidak bisa dikirim.')
        return
    }

    try {
        const res = await axios.get(BASE_URL + '/api/feed', {
            timeout: 8000,
            headers: global.dongtube_feed_secret
                ? { 'x-feed-secret': global.dongtube_feed_secret }
                : {}
        })

        const data = res.data
        if (!data || !data.ok || !Array.isArray(data.data)) return

        // Feed diurutkan terbaru di depan — proses terbalik agar kirim dari yang paling lama
        const events = [...data.data].reverse()

        for (const event of events) {
            if (!event.id) continue
            if (isSeen(event.id)) continue

            const pesan = formatPesan(event)
            if (!pesan) {
                addSeen(event.id) // tandai skip biar tidak diproses terus
                continue
            }

            try {
                await alip.sendMessage(saluranJid, { text: pesan })
                addSeen(event.id)
                // Jeda kecil agar tidak spam
                await new Promise(r => setTimeout(r, 1500))
            } catch (e) {
                console.error('[Dongtube Feed] Gagal kirim ke saluran:', e.message)
            }
        }
    } catch (e) {
        // Gagal fetch — diam saja, coba lagi 10 detik kemudian
        console.warn('[Dongtube Feed] Gagal fetch /api/feed:', e.message)
    }
}

function startDongtubeFeed(alip) {
    const INTERVAL = 10000 // 10 detik

    console.log('[Dongtube Feed] Notifier aktif ✅ — polling tiap 10 detik')

    // Jalankan pertama kali setelah 5 detik (beri waktu koneksi WA stabil)
    setTimeout(() => {
        pollFeed(alip)
        setInterval(() => pollFeed(alip), INTERVAL)
    }, 5000)
}

module.exports = startDongtubeFeed
