const CACHE_NAME = 'cadiz-ops-v1';
const SHELL_FILES = ['./', 'manifest.json'];

// Install — cache app shell
self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE_NAME).then(c => c.addAll(SHELL_FILES)));
  self.skipWaiting();
});

// Activate — clean old caches
self.addEventListener('activate', e => {
  e.waitUntil(caches.keys().then(keys =>
    Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k)))
  ));
  self.clients.claim();
});

// Fetch — network first for JSON, cache first for shell
self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);

  if (url.pathname.endsWith('dashboard.json')) {
    // Network first, fall back to cache
    e.respondWith(
      fetch(e.request).then(r => {
        const clone = r.clone();
        caches.open(CACHE_NAME).then(c => c.put(e.request, clone));
        return r;
      }).catch(() => caches.match(e.request))
    );
  } else {
    // Cache first, fall back to network
    e.respondWith(
      caches.match(e.request).then(r => r || fetch(e.request))
    );
  }
});
