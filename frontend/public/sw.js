// AMX Studio Service Worker — offline fallback only.
//
// Studio runs against a local CLI process. When the CLI stops, the
// browser tab can no longer reach localhost. Without a Service Worker
// the user lands on Chrome's "This site can't be reached" error,
// which gives no clue that the right next step is to restart
// ``amx /studio`` in a terminal. The Service Worker fixes this by
// caching a tiny static ``offline.html`` page on install and serving
// it whenever a navigation request fails.
//
// Deliberately minimal: we do NOT cache the SPA bundle here (Vite's
// hash-based chunk filenames already give us cache-busting via the
// HTTP layer; over-caching at the SW would mean stale chunks on
// every Studio upgrade). The Service Worker handles offline UX only.

// Bump the cache name when changing PRECACHE_URLS so the ``activate``
// handler wipes the old cache and the new payload (e.g. the AMX logo
// added in v2) is fetched on the next install.
const CACHE_NAME = 'amx-studio-offline-v2';
const OFFLINE_URL = '/offline.html';
const PRECACHE_URLS = [OFFLINE_URL, '/favicon.png', '/amx-logo.png'];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches
      .open(CACHE_NAME)
      .then((cache) => cache.addAll(PRECACHE_URLS))
      .then(() => self.skipWaiting()),
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((names) =>
        Promise.all(
          names
            .filter((name) => name !== CACHE_NAME)
            .map((name) => caches.delete(name)),
        ),
      )
      .then(() => self.clients.claim()),
  );
});

self.addEventListener('fetch', (event) => {
  const { request } = event;
  // Only intercept navigation requests. Other resources (JS chunks,
  // API calls, images) bubble up to the SPA's own error handling —
  // a failed ``/api/runs`` call should still surface as a toast or
  // banner inside the SPA when it's loaded, not be silently masked
  // by the offline page.
  if (request.mode !== 'navigate') {
    return;
  }
  event.respondWith(
    fetch(request).catch(() =>
      caches
        .match(OFFLINE_URL, { cacheName: CACHE_NAME })
        .then((cached) =>
          cached ||
          new Response(
            '<h1>AMX Studio is offline</h1><p>Run <code>amx /studio</code> to restart.</p>',
            { status: 503, headers: { 'Content-Type': 'text/html' } },
          ),
        ),
    ),
  );
});
