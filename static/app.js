(() => {
  'use strict';

  const TYPE_LABELS = {
    panic_button: 'Panic Button',
    tracker: 'Tracker',
    gateway: 'Gateway',
    sensor: 'Sensor',
    other: 'Lainnya',
  };

  const PROTOCOL_LABELS = {
    meshtastic: 'Meshtastic',
    http: 'HTTP',
    simulator: 'Simulator',
    teltonika: 'Teltonika',
    other: 'Lainnya',
  };

  const STALE_AFTER_MS = 60 * 60 * 1000;

  const METADATA_KEYS = [
    'name',
    'label',
    'device_type',
    'protocol',
    'serial',
    'owner',
    'tags',
    'notes',
  ];

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
    manageBtn: document.getElementById('manage-btn'),
    manageOverlay: document.getElementById('manage-overlay'),
    manageClose: document.getElementById('manage-close'),
    manageSearch: document.getElementById('manage-search'),
    manageNew: document.getElementById('manage-new'),
    manageDevices: document.getElementById('manage-devices'),
    rawBtn: document.getElementById('raw-btn'),
    rawBadge: document.getElementById('raw-badge'),
    rawOverlay: document.getElementById('raw-overlay'),
    rawClose: document.getElementById('raw-close'),
    rawSearch: document.getElementById('raw-search'),
    rawClearAll: document.getElementById('raw-clear-all'),
    rawDeviceCount: document.getElementById('raw-device-count'),
    rawDeviceList: document.getElementById('raw-device-list'),
    rawSelected: document.getElementById('raw-selected'),
    rawSelectedMeta: document.getElementById('raw-selected-meta'),
    rawRefresh: document.getElementById('raw-refresh'),
    rawClearDevice: document.getElementById('raw-clear-device'),
    rawPayloadList: document.getElementById('raw-payload-list'),
    topicsBtn: document.getElementById('topics-btn'),
    topicsOverlay: document.getElementById('topics-overlay'),
    topicsClose: document.getElementById('topics-close'),
    topicsBaseline: document.getElementById('topics-baseline'),
    topicsStatus: document.getElementById('topics-status'),
    topicsWarning: document.getElementById('topics-warning'),
    topicsCount: document.getElementById('topics-count'),
    topicsList: document.getElementById('topics-list'),
    topicForm: document.getElementById('topic-form'),
    topicInput: document.getElementById('topic-input'),
    topicLabel: document.getElementById('topic-label'),
    topicError: document.getElementById('topic-error'),
    topicSubmit: document.getElementById('topic-submit'),
    deviceForm: document.getElementById('device-form'),
    formError: document.getElementById('form-error'),
    formSubmit: document.getElementById('form-submit'),
    formDelete: document.getElementById('form-delete'),
    formReset: document.getElementById('form-reset'),
    formMode: document.getElementById('form-mode'),
    idHint: document.getElementById('field-id-hint'),
    fields: {
      id: document.getElementById('field-id'),
      label: document.getElementById('field-label'),
      type: document.getElementById('field-type'),
      protocol: document.getElementById('field-protocol'),
      serial: document.getElementById('field-serial'),
      owner: document.getElementById('field-owner'),
      tags: document.getElementById('field-tags'),
      lat: document.getElementById('field-lat'),
      lon: document.getElementById('field-lon'),
      notes: document.getElementById('field-notes'),
    },
  };

  const devices = new Map();
  const markers = new Map();
  const markerStates = new Map();

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
    editingId: null,
    manageDevices: [],
    rawDevices: [],
    rawSelectedKey: null,
    rawPayloads: [],
    topics: { baseline: [], custom: [], effective: [], mqtt_enabled: false, mqtt_connected: false },
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

  function displayName(device) {
    const label = typeof device.label === 'string' ? device.label.trim() : '';
    if (label) return label;
    if (device.name) return device.name;
    return device.id;
  }

  function isStale(device) {
    if (!device.last_seen) return true;
    const timestamp = new Date(device.last_seen).getTime();
    return Number.isNaN(timestamp) || Date.now() - timestamp > STALE_AFTER_MS;
  }

  function markerState(device) {
    if (device.status === 'panic') return 'panic';
    return isStale(device) ? 'stale' : 'normal';
  }

  function isPositioned(device) {
    return typeof device.lat === 'number' && typeof device.lon === 'number';
  }

  function typeLabel(device) {
    return TYPE_LABELS[device.device_type] || device.device_type || 'Lainnya';
  }

  function protocolLabel(device) {
    return PROTOCOL_LABELS[device.protocol] || device.protocol || 'Lainnya';
  }

  function serialKind(device) {
    return typeof device.serial === 'string' && /^\d{15,16}$/.test(device.serial)
      ? 'IMEI'
      : 'Serial';
  }

  function normalizeSerialInput(value) {
    return String(value || '').replace(/[\s-]/g, '');
  }

  function hasPanic() {
    return [...devices.values()].some((device) => device.status === 'panic');
  }

  function buildIcon(device) {
    const state = markerState(device);
    const extraClass = state === 'normal' ? '' : ` ${state}`;
    return L.divIcon({
      className: 'device-marker-wrap',
      html: `<div class="device-marker${extraClass}"><span class="dot"></span><span class="label">${escapeHtml(displayName(device))}</span></div>`,
      iconSize: [180, 24],
      iconAnchor: [7, 12],
    });
  }

  function removeMarker(id) {
    const marker = markers.get(id);
    if (marker) {
      marker.remove();
      markers.delete(id);
    }
    markerStates.delete(id);
  }

  function refreshMarkerStates() {
    for (const [id, marker] of markers) {
      const device = devices.get(id);
      if (!device) continue;
      const next = markerState(device);
      if (markerStates.get(id) === next) continue;
      marker.setIcon(buildIcon(device));
      markerStates.set(id, next);
    }
  }

  function syncManageEntry(device) {
    if (els.manageOverlay.classList.contains('hidden')) return;
    const index = state.manageDevices.findIndex((item) => item.id === device.id);
    if (index >= 0) state.manageDevices[index] = device;
    else state.manageDevices.push(device);
    renderManageList();
  }

  function upsertDevice(device, options = {}) {
    const previous = devices.get(device.id);
    const metadataChanged = !previous || METADATA_KEYS.some(
      (key) => JSON.stringify(previous[key]) !== JSON.stringify(device[key]),
    );

    devices.set(device.id, device);

    if (isPositioned(device)) {
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
      markerStates.set(device.id, markerState(device));
    } else {
      removeMarker(device.id);
    }

    if (options.alarm) {
      state.latestPanicId = device.id;
      if (isPositioned(device)) focusDevice(device.id, 15);
      if (state.soundEnabled) {
        beep();
        startAlarm();
      }
    }

    if (!state.batching) {
      renderSidebar();
      updateAlert();
    }

    if (metadataChanged) syncManageEntry(device);
  }

  function removeDevice(id) {
    removeMarker(id);
    devices.delete(id);
    state.manageDevices = state.manageDevices.filter((item) => item.id !== id);

    if (state.editingId === id) resetForm();
    renderSidebar();
    updateAlert();
    if (!els.manageOverlay.classList.contains('hidden')) renderManageList();
  }

  function applySnapshot(list) {
    const seen = new Set();
    state.batching = true;
    list.forEach((device) => {
      seen.add(device.id);
      upsertDevice(device);
    });

    for (const id of [...devices.keys()]) {
      if (!seen.has(id)) {
        removeMarker(id);
        devices.delete(id);
        state.manageDevices = state.manageDevices.filter((item) => item.id !== id);
      }
    }

    state.batching = false;
    renderSidebar();
    updateAlert();

    if (!els.manageOverlay.classList.contains('hidden')) {
      refreshManageList().catch(() => {});
    }

    if (state.firstSnapshot) {
      state.firstSnapshot = false;
      fitDevices();
      const panicDevice = list.find((device) => device.status === 'panic');
      if (panicDevice) {
        state.latestPanicId = panicDevice.id;
        if (isPositioned(panicDevice)) focusDevice(panicDevice.id, 14);
      }
    }
  }

  function renderSidebar() {
    const list = [...devices.values()].sort((a, b) => {
      if (a.status !== b.status) return a.status === 'panic' ? -1 : 1;
      return displayName(a).localeCompare(displayName(b));
    });

    els.deviceCount.textContent = String(list.length);
    els.panicCount.textContent = `${list.filter((device) => device.status === 'panic').length} panic`;

    if (!list.length) {
      els.deviceList.innerHTML =
        '<li class="empty">Belum ada device.<br>Registrasi lewat "Kelola Device", jalankan simulator, atau arahkan gateway Meshtastic ke broker MQTT.</li>';
      return;
    }

    els.deviceList.innerHTML = list.map(renderDeviceItem).join('');
  }

  function renderDeviceItem(device) {
    const panic = device.status === 'panic';
    const stale = !panic && isStale(device);
    const positioned = isPositioned(device);
    const meta = [];

    if (panic) {
      meta.push(`<span class="panic-text">PANIC · ${relativeTime(device.panic_since)}</span>`);
    } else if (device.last_seen) {
      meta.push(`<span>${relativeTime(device.last_seen)}</span>`);
    } else {
      meta.push('<span>belum terhubung</span>');
    }

    meta.push(`<span>${escapeHtml(typeLabel(device))}</span>`);
    meta.push(`<span>${escapeHtml(protocolLabel(device))}</span>`);
    if (device.serial) {
      meta.push(`<span>${escapeHtml(serialKind(device))} ${escapeHtml(device.serial)}</span>`);
    }

    if (typeof device.speed_kmh === 'number') {
      meta.push(`<span>${device.speed_kmh.toFixed(1)} km/j</span>`);
    }
    if (typeof device.battery === 'number') {
      meta.push(`<span>baterai ${Math.round(device.battery)}%</span>`);
    }
    if (!positioned) {
      meta.push('<span>lokasi belum ada</span>');
    }

    return `<li class="device${panic ? ' panic' : ''}${stale ? ' stale' : ''}" data-id="${escapeHtml(device.id)}">
      <div class="device-top">
        <span class="status-dot"></span>
        <div class="device-name">${escapeHtml(displayName(device))}<span class="device-id">${escapeHtml(device.id)}</span></div>
      </div>
      <div class="device-meta">${meta.join('')}</div>
      <div class="device-actions">
        ${positioned ? '<button class="btn ghost" type="button" data-action="focus">Fokus</button>' : ''}
        <button class="btn ghost" type="button" data-action="manage">Kelola</button>
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
    els.alertFocus.disabled = !isPositioned(latest);
    els.alertText.textContent =
      `${panicDevices.length > 1 ? `${panicDevices.length} device — ` : ''}` +
      `${displayName(latest)} (${latest.id}) · ${relativeTime(latest.panic_since)}` +
      `${isPositioned(latest) ? '' : ' · lokasi belum tersedia'}`;
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

  function showFormError(message) {
    els.formError.textContent = message;
    els.formError.classList.remove('hidden');
  }

  function hideFormError() {
    els.formError.textContent = '';
    els.formError.classList.add('hidden');
  }

  function setSelectValue(select, value) {
    select.value = value || 'other';
    if (!select.value) select.value = 'other';
  }

  function updateIdHint() {
    if (state.editingId) {
      els.fields.id.required = false;
      els.idHint.textContent = 'Device ID tidak dapat diubah setelah registrasi.';
      return;
    }

    if (els.fields.protocol.value === 'meshtastic') {
      els.fields.id.required = true;
      els.fields.id.placeholder = '!a1b2c3d4';
      els.idHint.textContent =
        'Wajib: Node ID Meshtastic (!xxxxxxxx). Lihat di aplikasi Meshtastic → Node Info.';
    } else {
      els.fields.id.required = false;
      els.fields.id.placeholder = 'Kosongkan untuk memakai IMEI';
      els.idHint.textContent =
        'Opsional. Jika kosong, Serial/IMEI 15–16 digit dipakai sebagai Device ID. Device harus mengirim IMEI yang sama.';
    }
  }

  function resetForm() {
    state.editingId = null;
    els.deviceForm.reset();
    els.fields.id.readOnly = false;
    els.formDelete.classList.add('hidden');
    els.formSubmit.textContent = 'Simpan Device';
    els.formMode.textContent = 'Tambah device baru';
    hideFormError();
    updateIdHint();
    renderManageList();
  }

  function selectManageDevice(id) {
    const device = state.manageDevices.find((item) => item.id === id) || devices.get(id);
    if (!device) return;

    state.editingId = device.id;
    els.fields.id.value = device.id;
    els.fields.id.readOnly = true;
    els.fields.label.value = device.label || '';
    setSelectValue(els.fields.type, device.device_type);
    setSelectValue(els.fields.protocol, device.protocol);
    els.fields.serial.value = device.serial || '';
    els.fields.owner.value = device.owner || '';
    els.fields.tags.value = Array.isArray(device.tags) ? device.tags.join(', ') : '';
    els.fields.lat.value = isPositioned(device) ? String(device.lat) : '';
    els.fields.lon.value = isPositioned(device) ? String(device.lon) : '';
    els.fields.notes.value = device.notes || '';

    els.formDelete.classList.remove('hidden');
    els.formSubmit.textContent = 'Simpan Perubahan';
    els.formMode.textContent = 'Edit device';
    hideFormError();
    updateIdHint();
    renderManageList();
  }

  function renderManageList() {
    const query = els.manageSearch.value.trim().toLowerCase();
    const list = state.manageDevices.filter((device) => {
      if (!query) return true;
      const haystack = [
        device.id,
        device.name,
        device.label,
        device.serial,
        device.owner,
        ...(Array.isArray(device.tags) ? device.tags : []),
      ]
        .join(' ')
        .toLowerCase();
      return haystack.includes(query);
    });

    if (!list.length) {
      els.manageDevices.innerHTML =
        '<li class="empty">Tidak ada device. Klik "+ Baru" untuk registrasi.</li>';
      return;
    }

    els.manageDevices.innerHTML = list
      .map((device) => {
        const panic = device.status === 'panic';
        const stale = !panic && isStale(device);
        const active = device.id === state.editingId ? ' active' : '';
        return `<li class="manage-item${panic ? ' panic' : ''}${stale ? ' stale' : ''}${active}" data-id="${escapeHtml(device.id)}">
          <div class="manage-item-name">
            <span class="status-dot"></span>
            <span>${escapeHtml(displayName(device))}</span>
          </div>
          <div class="manage-item-id">${escapeHtml(device.id)}${device.serial ? ` · ${escapeHtml(serialKind(device))} ${escapeHtml(device.serial)}` : ''}</div>
          <div class="manage-item-meta">
            <span class="tag accent">${escapeHtml(typeLabel(device))}</span>
            <span class="tag">${escapeHtml(protocolLabel(device))}</span>
            ${panic ? '<span class="tag danger">PANIC</span>' : ''}
            ${isPositioned(device) ? '' : '<span class="tag">tanpa lokasi</span>'}
          </div>
        </li>`;
      })
      .join('');
  }

  async function refreshManageList() {
    try {
      const response = await fetch('/api/devices');
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const list = await response.json();
      state.manageDevices = Array.isArray(list) ? list : [];
      renderManageList();
    } catch (error) {
      console.error('Gagal memuat daftar device', error);
      showFormError('Gagal memuat daftar device. Coba lagi.');
    }
  }

  async function openManage(id = null) {
    els.manageOverlay.classList.remove('hidden');
    hideFormError();
    await refreshManageList();
    if (id) {
      selectManageDevice(id);
    } else if (!state.editingId) {
      resetForm();
    }
    els.manageSearch.focus();
  }

  function closeManage() {
    els.manageOverlay.classList.add('hidden');
  }

  function rawTotal() {
    return state.rawDevices.reduce((sum, device) => sum + device.count, 0);
  }

  function updateRawBadge() {
    const total = rawTotal();
    els.rawBadge.textContent = String(total);
    els.rawBadge.classList.toggle('hidden', total === 0);
  }

  function renderRawList() {
    const query = els.rawSearch.value.trim().toLowerCase();
    const list = state.rawDevices.filter((device) => {
      if (!query) return true;
      return (
        device.key.toLowerCase().includes(query) ||
        device.reasons.some((reason) => reason.toLowerCase().includes(query)) ||
        device.sources.some((source) => source.toLowerCase().includes(query))
      );
    });

    if (!list.length) {
      els.rawDeviceList.innerHTML = '<li class="empty">Belum ada payload yang tercatat.</li>';
      return;
    }

    els.rawDeviceList.innerHTML = list
      .map(
        (device) => `<li class="raw-device${device.key === state.rawSelectedKey ? ' active' : ''}" data-key="${escapeHtml(device.key)}">
          <div class="raw-device-top">
            <strong>${escapeHtml(device.key)}</strong>
            <span class="chip">${device.count}</span>
          </div>
          <div class="raw-device-meta">
            <span>${relativeTime(device.last_seen)}</span>
            ${device.sources.map((source) => `<span class="tag">${escapeHtml(source)}</span>`).join('')}
            ${device.reasons.slice(0, 2).map((reason) => `<span class="tag raw-reason">${escapeHtml(reason)}</span>`).join('')}
          </div>
        </li>`,
      )
      .join('');
  }

  function renderRawHistory() {
    if (!state.rawPayloads.length) {
      els.rawPayloadList.innerHTML = '<li class="empty">Tidak ada payload untuk device ini.</li>';
      return;
    }

    els.rawPayloadList.innerHTML = state.rawPayloads
      .map((item) => {
        const text = item.payload_text ?? item.payload_b64;
        return `<li class="raw-payload" data-id="${escapeHtml(item.id)}">
          <div class="raw-payload-head">
            <span>${escapeHtml(relativeTime(item.received_at))}</span>
            <span class="tag">${escapeHtml(item.source)}</span>
            <span class="tag raw-reason">${escapeHtml(item.reason)}</span>
            <span>${escapeHtml(item.topic)}</span>
            <button class="btn ghost raw-copy" type="button" data-copy="${escapeHtml(item.id)}">Salin</button>
          </div>
          <pre class="raw-payload-body">${escapeHtml(text)}${item.truncated ? ' … (dipotong)' : ''}</pre>
        </li>`;
      })
      .join('');
  }

  async function refreshRawHistory() {
    const key = state.rawSelectedKey;
    if (!key) {
      els.rawSelected.textContent = 'Pilih device';
      els.rawSelectedMeta.textContent = '';
      els.rawPayloadList.innerHTML = '<li class="empty">Pilih device di daftar sebelah kiri.</li>';
      return;
    }

    try {
      const response = await fetch(`/api/unmatched/payloads?key=${encodeURIComponent(key)}`);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const list = await response.json();
      state.rawPayloads = Array.isArray(list) ? list : [];
      const device = state.rawDevices.find((item) => item.key === key);
      els.rawSelected.textContent = key;
      els.rawSelectedMeta.textContent = device
        ? `${device.count} payload · terakhir ${relativeTime(device.last_seen)}`
        : `${state.rawPayloads.length} payload`;
      renderRawHistory();
    } catch (error) {
      console.error('Gagal memuat history payload', error);
    }
  }

  async function refreshRawList(options = {}) {
    try {
      const response = await fetch('/api/unmatched');
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const list = await response.json();
      state.rawDevices = Array.isArray(list) ? list : [];
      updateRawBadge();
      els.rawDeviceCount.textContent = `${state.rawDevices.length} device`;
      renderRawList();

      if (options.selectFirst && !state.rawSelectedKey && state.rawDevices.length) {
        state.rawSelectedKey = state.rawDevices[0].key;
        renderRawList();
      }
      await refreshRawHistory();
    } catch (error) {
      console.error('Gagal memuat data mentah', error);
    }
  }

  async function selectRawDevice(key) {
    state.rawSelectedKey = key;
    renderRawList();
    await refreshRawHistory();
  }

  async function openRaw() {
    els.rawOverlay.classList.remove('hidden');
    els.rawSearch.value = '';
    state.rawSelectedKey = null;
    state.rawPayloads = [];
    await refreshRawList({ selectFirst: true });
    els.rawSearch.focus();
  }

  function closeRaw() {
    els.rawOverlay.classList.add('hidden');
  }

  function showTopicError(message) {
    els.topicError.textContent = message;
    els.topicError.classList.remove('hidden');
  }

  function hideTopicError() {
    els.topicError.textContent = '';
    els.topicError.classList.add('hidden');
  }

  function renderTopics() {
    const data = state.topics;
    els.topicsBaseline.innerHTML = data.baseline.length
      ? data.baseline.map((topic) => `<span class="topic-chip">${escapeHtml(topic)}</span>`).join('')
      : '<span class="empty-inline">Tidak ada topik baseline.</span>';
    els.topicsStatus.textContent = !data.mqtt_enabled
      ? 'MQTT nonaktif'
      : data.mqtt_connected
        ? 'Terhubung'
        : 'Terputus';
    els.topicsStatus.className = `chip ${data.mqtt_enabled && data.mqtt_connected ? 'topic-status-live' : 'topic-status-offline'}`;
    if (data.baseline.includes('#') || data.effective.includes('#')) {
      els.topicsWarning.textContent = 'Topik # aktif: semua topik sudah tercakup, topik tambahan tidak menambah subscription.';
      els.topicsWarning.classList.remove('hidden');
    } else {
      els.topicsWarning.classList.add('hidden');
    }
    els.topicsCount.textContent = String(data.custom.length);
    if (!data.custom.length) {
      els.topicsList.innerHTML = '<li class="empty">Belum ada topik tambahan.</li>';
      return;
    }
    els.topicsList.innerHTML = data.custom.map((item) => `
      <li class="topic-item">
        <div class="topic-item-main">
          <strong>${escapeHtml(item.topic)}</strong>
          ${item.label ? `<span class="topic-label">${escapeHtml(item.label)}</span>` : ''}
        </div>
        <div class="topic-item-meta">
          <span class="tag ${item.active ? 'accent' : ''}">${item.active ? 'Aktif' : 'Tercakup'}</span>
          <button class="btn ghost danger-text topic-delete" type="button" data-topic="${escapeHtml(item.topic)}">Hapus</button>
        </div>
      </li>`).join('');
  }

  async function refreshTopics() {
    try {
      const response = await fetch('/api/topics');
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      state.topics = await response.json();
      renderTopics();
    } catch (error) {
      console.error('Gagal memuat topik MQTT', error);
      showTopicError('Gagal memuat topik MQTT. Coba lagi.');
    }
  }

  async function openTopics() {
    els.topicsOverlay.classList.remove('hidden');
    hideTopicError();
    await refreshTopics();
    els.topicInput.focus();
  }

  function closeTopics() {
    els.topicsOverlay.classList.add('hidden');
  }

  async function addTopic(event) {
    event.preventDefault();
    hideTopicError();
    const topic = els.topicInput.value.trim();
    const label = els.topicLabel.value.trim();
    if (!topic) {
      showTopicError('Topik wajib diisi.');
      return;
    }
    els.topicSubmit.disabled = true;
    try {
      const response = await fetch('/api/topics', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ topic, label: label || null }),
      });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || `HTTP ${response.status}`);
      }
      state.topics = await response.json();
      els.topicInput.value = '';
      els.topicLabel.value = '';
      renderTopics();
      els.topicInput.focus();
    } catch (error) {
      showTopicError(error.message || 'Gagal menambah topik.');
    } finally {
      els.topicSubmit.disabled = false;
    }
  }

  async function deleteTopic(topic) {
    if (!window.confirm(`Hapus topik ${topic}?`)) return;
    hideTopicError();
    try {
      const response = await fetch(`/api/topics?topic=${encodeURIComponent(topic)}`, { method: 'DELETE' });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || `HTTP ${response.status}`);
      }
      state.topics = await response.json();
      renderTopics();
    } catch (error) {
      showTopicError(error.message || 'Gagal menghapus topik.');
    }
  }
  function collectFormPayload() {
    const latText = els.fields.lat.value.trim();
    const lonText = els.fields.lon.value.trim();

    if ((latText && !lonText) || (!latText && lonText)) {
      throw new Error('Latitude dan longitude harus diisi bersamaan.');
    }

    const payload = {
      label: els.fields.label.value,
      device_type: els.fields.type.value,
      protocol: els.fields.protocol.value,
      serial: els.fields.serial.value,
      owner: els.fields.owner.value,
      tags: els.fields.tags.value
        .split(',')
        .map((tag) => tag.trim())
        .filter(Boolean),
      notes: els.fields.notes.value,
    };

    if (latText) {
      payload.lat = Number(latText);
      if (!Number.isFinite(payload.lat) || Math.abs(payload.lat) > 90) {
        throw new Error('Latitude tidak valid.');
      }
    }
    if (lonText) {
      payload.lon = Number(lonText);
      if (!Number.isFinite(payload.lon) || Math.abs(payload.lon) > 180) {
        throw new Error('Longitude tidak valid.');
      }
    }

    if (!state.editingId) {
      const id = els.fields.id.value.trim();
      if (!id) {
        if (els.fields.protocol.value === 'meshtastic') {
          throw new Error('Node ID Meshtastic wajib diisi (contoh !a1b2c3d4).');
        }
        if (!/^\d{15,16}$/.test(normalizeSerialInput(els.fields.serial.value))) {
          throw new Error(
            'Isi Device ID, atau Serial/IMEI 15–16 digit untuk memakai IMEI sebagai ID.',
          );
        }
      }
    }

    return payload;
  }

  async function saveDevice(event) {
    event.preventDefault();
    hideFormError();

    let payload;
    try {
      payload = collectFormPayload();
    } catch (error) {
      showFormError(error.message);
      return;
    }

    els.formSubmit.disabled = true;
    try {
      let response;
      if (state.editingId) {
        response = await fetch(`/api/devices/${encodeURIComponent(state.editingId)}`, {
          method: 'PATCH',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(payload),
        });
      } else {
        const id = els.fields.id.value.trim();
        const body = { ...payload };
        if (id) body.id = id;
        response = await fetch('/api/devices', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(body),
        });
      }

      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || `HTTP ${response.status}`);
      }

      const device = await response.json();
      state.editingId = device.id;
      upsertDevice(device);
      await refreshManageList();
      selectManageDevice(device.id);
    } catch (error) {
      showFormError(error.message || 'Gagal menyimpan device.');
    } finally {
      els.formSubmit.disabled = false;
    }
  }

  async function deleteSelectedDevice() {
    if (!state.editingId) return;
    const device = devices.get(state.editingId);
    const name = device ? displayName(device) : state.editingId;

    if (!window.confirm(`Hapus device ${name} (${state.editingId})?`)) return;

    els.formDelete.disabled = true;
    try {
      const response = await fetch(`/api/devices/${encodeURIComponent(state.editingId)}`, {
        method: 'DELETE',
      });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || `HTTP ${response.status}`);
      }

      removeDevice(state.editingId);
      state.editingId = null;
      resetForm();
      await refreshManageList();
    } catch (error) {
      showFormError(error.message || 'Gagal menghapus device.');
    } finally {
      els.formDelete.disabled = false;
    }
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
      case 'removed':
        if (message.id) removeDevice(message.id);
        break;
      case 'raw_captured':
        updateRawBadge();
        if (!els.rawOverlay.classList.contains('hidden')) {
          refreshRawList().catch(() => {});
        }
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
      if (button.dataset.action === 'manage') openManage(id);
      return;
    }

    if (markers.has(id)) focusDevice(id);
    else openManage(id);
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

  els.manageBtn.addEventListener('click', () => openManage());
  els.manageClose.addEventListener('click', closeManage);
  els.manageNew.addEventListener('click', () => {
    resetForm();
    els.fields.id.focus();
  });
  els.manageSearch.addEventListener('input', renderManageList);
  els.fields.protocol.addEventListener('change', updateIdHint);

  els.manageOverlay.addEventListener('click', (event) => {
    if (event.target === els.manageOverlay) closeManage();
  });

  els.manageDevices.addEventListener('click', (event) => {
    const item = event.target.closest('li[data-id]');
    if (!item) return;
    selectManageDevice(item.dataset.id);
  });

  els.deviceForm.addEventListener('submit', saveDevice);
  els.formDelete.addEventListener('click', deleteSelectedDevice);
  els.formReset.addEventListener('click', () => {
    resetForm();
    els.fields.id.focus();
  });

  els.topicsBtn.addEventListener('click', () => openTopics());
  els.topicsClose.addEventListener('click', closeTopics);
  els.topicsOverlay.addEventListener('click', (event) => {
    if (event.target === els.topicsOverlay) closeTopics();
  });
  els.topicForm.addEventListener('submit', addTopic);
  els.topicsList.addEventListener('click', (event) => {
    const button = event.target.closest('button[data-topic]');
    if (button) deleteTopic(button.dataset.topic);
  });

  els.rawBtn.addEventListener('click', () => openRaw());
  els.rawClose.addEventListener('click', closeRaw);
  els.rawRefresh.addEventListener('click', () => refreshRawList());
  els.rawSearch.addEventListener('input', renderRawList);
  els.rawDeviceList.addEventListener('click', (event) => {
    const item = event.target.closest('li[data-key]');
    if (item) selectRawDevice(item.dataset.key);
  });
  els.rawPayloadList.addEventListener('click', async (event) => {
    const button = event.target.closest('button[data-copy]');
    if (!button) return;
    const item = state.rawPayloads.find((payload) => payload.id === button.dataset.copy);
    if (!item) return;
    try {
      await navigator.clipboard.writeText(item.payload_text ?? item.payload_b64);
      button.textContent = 'Tersalin';
      setTimeout(() => {
        button.textContent = 'Salin';
      }, 1200);
    } catch (error) {
      console.error('Gagal menyalin payload', error);
    }
  });
  els.rawClearAll.addEventListener('click', async () => {
    if (!window.confirm('Hapus semua payload mentah?')) return;
    await fetch('/api/unmatched', { method: 'DELETE' });
    state.rawSelectedKey = null;
    state.rawPayloads = [];
    await refreshRawList();
  });
  els.rawClearDevice.addEventListener('click', async () => {
    const key = state.rawSelectedKey;
    if (!key || !window.confirm(`Hapus semua payload dari ${key}?`)) return;
    await fetch(`/api/unmatched?key=${encodeURIComponent(key)}`, { method: 'DELETE' });
    state.rawSelectedKey = null;
    state.rawPayloads = [];
    await refreshRawList();
  });
  els.rawOverlay.addEventListener('click', (event) => {
    if (event.target === els.rawOverlay) closeRaw();
  });

  window.addEventListener('keydown', (event) => {
    if (event.key !== 'Escape') return;
    if (!els.manageOverlay.classList.contains('hidden')) closeManage();
    else if (!els.rawOverlay.classList.contains('hidden')) closeRaw();
    else if (!els.topicsOverlay.classList.contains('hidden')) closeTopics();
  });

  setInterval(() => {
    refreshMarkerStates();
    renderSidebar();
    updateAlert();
    if (!els.manageOverlay.classList.contains('hidden')) renderManageList();
    if (!els.rawOverlay.classList.contains('hidden')) refreshRawList().catch(() => {});
    if (!els.topicsOverlay.classList.contains('hidden')) refreshTopics().catch(() => {});
  }, 5000);

  renderSidebar();
  updateAlert();
  updateSoundButton();
  updateIdHint();
  refreshRawList().catch(() => {});
  connect();
})();
