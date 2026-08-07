const CACHE_NAME = 'market-pulse-shell-v1';
const SHELL_URLS = ['/', '/manifest.json', '/icon-192.png', '/icon-512.png'];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(SHELL_URLS))
  );
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)))
    )
  );
  self.clients.claim();
});

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);

  // Never intercept/cache API calls — this is a live dashboard; serving
  // stale price/index data from cache would be actively misleading.
  if (url.pathname.startsWith('/api/')) {
    return;
  }

  // Network-first for the app shell (HTML/manifest/icons), falling back
  // to cache only when genuinely offline — e.g. a brief mobile signal
  // drop — so the app still opens instead of showing a browser error.
  event.respondWith(
    fetch(event.request)
      .then((response) => {
        const copy = response.clone();
        caches.open(CACHE_NAME).then((cache) => cache.put(event.request, copy));
        return response;
      })
      .catch(() => caches.match(event.request))
  );
});

// ---------- Push notifications (calendar event reminders) ----------
self.addEventListener('push', (event) => {
  let data = { title: 'Market Pulse', body: 'You have a calendar reminder.' };
  try {
    if (event.data) data = event.data.json();
  } catch (e) {
    // Non-JSON payload — fall back to the default text above rather than failing silently.
  }
  event.waitUntil(
    self.registration.showNotification(data.title, {
      body: data.body,
      icon: '/icon-192.png',
      badge: '/icon-192.png',
      tag: data.tag || 'market-pulse-calendar',  // same tag replaces an existing notification instead of stacking duplicates
      data: { url: data.url || '/' },
    })
  );
});

// Tapping the notification focuses an already-open tab if there is one,
// rather than always opening a brand new one.
self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const targetUrl = (event.notification.data && event.notification.data.url) || '/';
  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then((windowClients) => {
      for (const client of windowClients) {
        if (client.url.includes(self.registration.scope) && 'focus' in client) {
          return client.focus();
        }
      }
      if (clients.openWindow) return clients.openWindow(targetUrl);
    })
  );
});
