// © DONGTUBE | WhatsApp: 0831-4396-1588
// ⚠️ Do not remove this credit
// Dongtube Service Worker — PWA offline support
const CACHE_NAME = 'Dongtube-v2';
const STATIC_ASSETS = ['/', '/index.html', '/otp.html', '/track.html', '/pay.html', '/upload.html'];

self.addEventListener('install', function(e) {
  e.waitUntil(
    caches.open(CACHE_NAME).then(function(cache) {
      return cache.addAll(STATIC_ASSETS).catch(function(){});
    })
  );
  self.skipWaiting();
});

self.addEventListener('activate', function(e) {
  e.waitUntil(
    caches.keys().then(function(keys) {
      return Promise.all(keys.filter(function(k){ return k !== CACHE_NAME; }).map(function(k){ return caches.delete(k); }));
    })
  );
  // BUG FIX: Removed self.clients.claim() — it was causing unexpected page reloads
  // when the SW updated during an active session (e.g. after deployment).
  // Pages will use the new SW on next navigation instead, which is the correct behavior.
});

self.addEventListener('fetch', function(e) {
  // Only handle GET, skip API calls (always network)
  if (e.request.method !== 'GET') return;
  var url = new URL(e.request.url);
  if (url.pathname.startsWith('/api/')) return;

  e.respondWith(
    fetch(e.request)
      .then(function(res) {
        // Cache successful HTML/JS/CSS responses
        if (res.ok && ['document','script','style'].includes(e.request.destination)) {
          var clone = res.clone();
          caches.open(CACHE_NAME).then(function(c){ c.put(e.request, clone); });
        }
        return res;
      })
      .catch(function() {
        return caches.match(e.request).then(function(cached) {
          return cached || caches.match('/index.html');
        });
      })
  );
});
