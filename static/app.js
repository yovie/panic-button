(() => {
  'use strict';

  const map = L.map('map').setView([-6.1944, 106.8229], 12);
  L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution:
      '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
  }).addTo(map);
  L.control.scale({ imperial: false }).addTo(map);

  const els = {
    alertBar: document.getElementById('alert-bar'),
    alertText: document.getElementById('alert-text'),
    alertFocus: document.getElementById('alert-focus'),
    alertResolve: document.getElementById('alert-resolve'),
    conn: document.getElementById('conn'),
    connText: document.getElementById('conn-text'),
    deviceCount: document.getElementById('device-count'),
    deviceList: document.getElementById('device-list'),
    panicCount: document.getElementById('panic-count'),
    soundBtn: document.getElementById('sound-btn'),
    sidebar: document.getElementById('sidebar'),
    sidebarToggle: document.getElementById('sidebar-toggle'),
  };

  const devices = new Map();
  const markers = new Map();

  const state = {
    ws: null,
    reconnectTimer: null,
    reconnectDelay: 1000,
    firstSnapshot: true,
    batching: false,
    latestPanicId: null,
    soundEnabled: false,
    audioContext: null,
    alarmTimer: null,
  };

  function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, (character) => {
      switch (character) {
        case '&': return '&amp;';
        case '<': return '&lt;';
        case '>': return '&gt;';
        case '"': return '&quot;';
        default: return '&#39;';
      }
    });
  }

  function relativeTime(iso) {
    if (!iso) return '—';
    const timestamp = new Date(iso).getTime();
    if (Number.isNaN(timestamp)) return '—';
    const seconds = Math.max(0, (Date.now() - timestamp) / 1000);
    if (seconds < 5) return 'baru saja';
    if (seconds < 60) return `${Math.floor(seconds)} dtk lalu`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)} mnt lalu`;
    if (seconds < 86400) return `${Math.floor(seconds / 3600)} jam lalu`;
    return `${Math.floor(seconds / 86400)} hari lalu`;
  }

  function hasPanic() {
    return [...devices.values()].some((device) => device.status === 'panic');
  }

  function buildIcon(device) {
    const panic = device.status === 'panic';
    return L.divIcon({
      className: 'device-marker-wrap',
      html: `<div class="device-marker${panic ? ' panic' : ''}"><span class="dot"></span><span class="label">${escapeHtml(device.name)}</span></div>`,
      iconSize: [180, 24],
      iconAnchor: [7, 12],
    });
  }

  function upsertDevice(device, options = {}) {
    devices.set(device.id, device);

    const latlng = [device.lat, device.lon];
    let marker = markers.get(device.id);
    if (!marker) {
      marker = L.marker(latlng, {
        icon: buildIcon(device),
        riseOnHover: true,
      }).addTo(map);
      marker.on('click', () => focusDevice(device.id));
      markers.set(device.id, marker);
    } else {
      marker.setLatLng(latlng);
      marker.setIcon(buildIcon(device));
    }

    if (options.alarm) {
      state.latestPanicId = device.id;
      focusDevice(device.id, 15);
      if (state.soundEnabled) {
        beep();
        startAlarm();
      }
    }

    if (!state.batching) {
      renderSidebar();
      updateAlert();
    }
  }

  function applySnapshot(list) {
    const seen = new Set();
    state.batching = true;
    list.forEach((device) => {
      seen.add(device.id);
      upsertDevice(device);
    });

    for (const id of [...markers.keys()]) {
      if (!seen.has(id)) {
        markers.get(id).remove();
        markers.delete(id);
        devices.delete(id);
      }
    }

    state.batching = false;
    renderSidebar();
    updateAlert();

    if (state.firstSnapshot) {
      state.firstSnapshot = false;
      fitDevices();
      const panicDevice = list.find((device) => device.status === 'panic');
      if (panicDevice) {
        state.latestPanicId = panicDevice.id;
        focusDevice(panicDevice.id, 14);
      }
    }
  }

  function renderSidebar() {
    const list = [...devices.values()].sort((a, b) => {
      if (a.status !== b.status) return a.status === 'panic' ? -1 : 1;
      return String(a.name).localeCompare(String(b.name));
    });

    els.deviceCount.textContent = String(list.length);
    els.panicCount.textContent = `${list.filter((device) => device.status === 'panic').length} panic`;

    if (!list.length) {
      els.deviceList.innerHTML =
        '<li class="empty">Belum ada device.<br>Jalankan simulator atau arahkan gateway Meshtastic ke broker MQTT.</li>';
      return;
    }

    els.deviceList.innerHTML = list.map(renderDeviceItem).join('');
  }

  function renderDeviceItem(device) {
    const panic = device.status === 'panic';
    const meta = [];

    if (panic) {
      meta.push(`<span class="panic-text">PANIC · ${relativeTime(device.panic_since)}</span>`);
    } else {
      meta.push(`<span>${relativeTime(device.last_seen)}</span>`);
    }
    if (typeof device.speed_kmh === 'number') {
      meta.push(`<span>${device.speed_kmh.toFixed(1)} km/j</span>`);
    }
    if (typeof device.battery === 'number') {
      meta.push(`<span>baterai ${Math.round(device.battery)}%</span>`);
    }

    return `<li class="device${panic ? ' panic' : ''}" data-id="${escapeHtml(device.id)}">
      <div class="device-top">
        <span class="status-dot"></span>
        <div class="device-name">${escapeHtml(device.name)}<span class="device-id">${escapeHtml(device.id)}</span></div>
      </div>
      <div class="device-meta">${meta.join('')}</div>
      <div class="device-actions">
        <button class="btn ghost" type="button" data-action="focus">Fokus</button>
        ${panic ? '<button class="btn danger" type="button" data-action="resolve">Tandai Selesai</button>' : ''}
      </div>
    </li>`;
  }

  function updateAlert() {
    const panicDevices = [...devices.values()].filter((device) => device.status === 'panic');
    els.panicCount.textContent = `${panicDevices.length} panic`;

    if (!panicDevices.length) {
      els.alertBar.classList.add('hidden');
      state.latestPanicId = null;
      stopAlarm();
      document.title = 'Panic Button — Live Tracking';
      return;
    }

    const latest = panicDevices.reduce((current, candidate) => {
      const currentTime = new Date(current.panic_since || 0).getTime();
      const candidateTime = new Date(candidate.panic_since || 0).getTime();
      return candidateTime > currentTime ? candidate : current;
    });

    state.latestPanicId = latest.id;
    els.alertBar.classList.remove('hidden');
    els.alertText.textContent =
      `${panicDevices.length > 1 ? `${panicDevices.length} device — ` : ''}` +
      `${latest.name} (${latest.id}) · ${relativeTime(latest.panic_since)}`;
    document.title = `PANIC (${panicDevices.length}) — Panic Button`;

    if (state.soundEnabled) startAlarm();
    else stopAlarm();
  }

  function fitDevices() {
    const latlngs = [...markers.values()].map((marker) => marker.getLatLng());
    if (!latlngs.length) return;
    if (latlngs.length === 1) {
      map.setView(latlngs[0], 14);
      return;
    }
    map.fitBounds(L.latLngBounds(latlngs), { padding: [40, 40], maxZoom: 15 });
  }

  function focusDevice(id, zoom = 15) {
    const marker = markers.get(id);
    if (!marker) return;
    map.flyTo(marker.getLatLng(), Math.max(map.getZoom(), zoom), { duration: 0.7 });
    els.sidebar.classList.remove('open');
  }

  async function resolveDevice(id) {
    try {
      const response = await fetch(`/api/devices/${encodeURIComponent(id)}/resolve`, {
        method: 'POST',
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
    } catch (error) {
      console.error('Gagal menandai panic selesai', error);
    }
  }

  function ensureAudio() {
    if (!state.audioContext) {
      const AudioCtor = window.AudioContext || window.webkitAudioContext;
      if (AudioCtor) state.audioContext = new AudioCtor();
    }
    return state.audioContext;
  }

  function beep() {
    const context = ensureAudio();
    if (!context || !state.soundEnabled) return;
    if (context.state === 'suspended') context.resume().catch(() => {});

    const now = context.currentTime;
    [
      [0, 880],
      [0.18, 660],
    ].forEach(([offset, frequency]) => {
      const oscillator = context.createOscillator();
      const gain = context.createGain();
      oscillator.type = 'square';
      oscillator.frequency.value = frequency;
      gain.gain.setValueAtTime(0.0001, now + offset);
      gain.gain.exponentialRampToValueAtTime(0.07, now + offset + 0.02);
      gain.gain.exponentialRampToValueAtTime(0.0001, now + offset + 0.16);
      oscillator.connect(gain).connect(context.destination);
      oscillator.start(now + offset);
      oscillator.stop(now + offset + 0.18);
    });
  }

  function startAlarm() {
    if (state.alarmTimer || !state.soundEnabled) return;
    state.alarmTimer = setInterval(beep, 2500);
  }

  function stopAlarm() {
    if (state.alarmTimer) {
      clearInterval(state.alarmTimer);
      state.alarmTimer = null;
    }
  }

  function updateSoundButton() {
    els.soundBtn.textContent = state.soundEnabled ? 'Suara: ON' : 'Suara: OFF';
  }

  function setConnection(status) {
    els.conn.className = `conn ${status === 'live' ? 'live' : status === 'offline' ? 'offline' : ''}`.trim();
    els.connText.textContent =
      status === 'live' ? 'LIVE' : status === 'offline' ? 'Offline' : 'Menghubungkan';
  }

  function handleMessage(message) {
    switch (message.type) {
      case 'snapshot':
        applySnapshot(Array.isArray(message.devices) ? message.devices : []);
        break;
      case 'update':
        if (message.device) upsertDevice(message.device);
        break;
      case 'panic':
        if (message.device) upsertDevice(message.device, { alarm: true });
        break;
      case 'panic_cleared':
        if (message.device) upsertDevice(message.device);
        break;
      default:
        break;
    }
  }

  function connect() {
    clearTimeout(state.reconnectTimer);
    const scheme = window.location.protocol === 'https:' ? 'wss' : 'ws';
    const socket = new WebSocket(`${scheme}://${window.location.host}/ws`);
    state.ws = socket;
    setConnection('connecting');

    socket.addEventListener('open', () => {
      state.reconnectDelay = 1000;
      setConnection('live');
    });

    socket.addEventListener('message', (event) => {
      try {
        handleMessage(JSON.parse(event.data));
      } catch (error) {
        console.error('Pesan WebSocket tidak valid', error);
      }
    });

    socket.addEventListener('close', () => {
      setConnection('offline');
      clearTimeout(state.reconnectTimer);
      state.reconnectTimer = setTimeout(connect, state.reconnectDelay);
      state.reconnectDelay = Math.min(state.reconnectDelay * 1.7, 10000);
    });

    socket.addEventListener('error', () => socket.close());
  }

  els.deviceList.addEventListener('click', (event) => {
    const item = event.target.closest('li[data-id]');
    if (!item) return;

    const id = item.dataset.id;
    const button = event.target.closest('button[data-action]');
    if (button) {
      event.stopPropagation();
      if (button.dataset.action === 'focus') focusDevice(id);
      if (button.dataset.action === 'resolve') resolveDevice(id);
      return;
    }

    focusDevice(id);
  });

  els.alertFocus.addEventListener('click', () => {
    if (state.latestPanicId) focusDevice(state.latestPanicId, 15);
  });

  els.alertResolve.addEventListener('click', () => {
    if (state.latestPanicId) resolveDevice(state.latestPanicId);
  });

  els.soundBtn.addEventListener('click', () => {
    state.soundEnabled = !state.soundEnabled;
    if (state.soundEnabled) {
      const context = ensureAudio();
      if (context && context.state === 'suspended') context.resume().catch(() => {});
      beep();
      if (hasPanic()) startAlarm();
    } else {
      stopAlarm();
    }
    updateSoundButton();
  });

  els.sidebarToggle.addEventListener('click', () => {
    els.sidebar.classList.toggle('open');
  });

  setInterval(() => {
    renderSidebar();
    updateAlert();
  }, 5000);

  renderSidebar();
  updateAlert();
  updateSoundButton();
  connect();
})();
