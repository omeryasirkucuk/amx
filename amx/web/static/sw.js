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

// Bump the cache name when changing PRECACHE_URLS or fetch logic so
// the ``activate`` handler wipes the old cache and the new payload
// is fetched on the next install.
const CACHE_NAME = 'amx-studio-offline-v3';
const OFFLINE_URL = '/offline.html';
const PRECACHE_URLS = [
  OFFLINE_URL,
  '/favicon.png',
  '/favicon.svg',
  '/amx-logo.png',
];

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

// Precached asset URLs as an absolute-path Set for cheap lookup.
const PRECACHED_PATHS = new Set(PRECACHE_URLS);

function isPrecachedAsset(url) {
  try {
    const u = new URL(url);
    return PRECACHED_PATHS.has(u.pathname);
  } catch (_err) {
    return false;
  }
}

self.addEventListener('fetch', (event) => {
  const { request } = event;
  const url = request.url;

  // 1. Navigation requests — serve the offline page when network fails.
  //    This is the headline UX: the user refreshes a Studio tab whose
  //    CLI has died and lands on our branded page instead of Chrome's
  //    "site can't be reached".
  if (request.mode === 'navigate') {
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
    return;
  }

  // 2. Precached static assets (favicon, logo) — these are loaded by
  //    the offline page itself, so they must resolve even when the
  //    CLI is down. Try network first (so an updated logo lands
  //    naturally on the next visit) and fall back to cache. Without
  //    this branch the offline page would show a broken-image icon
  //    for the logo and the browser tab would show a blank favicon
  //    while the CLI is offline.
  if (isPrecachedAsset(url)) {
    event.respondWith(
      fetch(request).catch(() =>
        caches.match(request, { cacheName: CACHE_NAME }).then(
          (cached) => cached || new Response('', { status: 504 }),
        ),
      ),
    );
    return;
  }

  // 3. Everything else (JS chunks, API calls) — bubble up to the
  //    SPA's own error handling. A failed ``/api/runs`` should still
  //    surface as a toast in the SPA when it's loaded, not be
  //    silently masked by the offline page.
});
