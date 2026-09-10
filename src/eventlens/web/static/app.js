// EventLens Web Studio JavaScript Client
document.addEventListener("DOMContentLoaded", () => {
  // State
  let ws = null;
  let records = [];
  let dlqRecords = [];
  let isPaused = false;
  let filterText = "";
  let selectedRecord = null;
  let selectedDlqRecord = null;
  let eventCounter = 0;
  let lastTime = Date.now();

  // DOM Elements
  const connStatus = document.getElementById("connection-status");
  const connText = document.getElementById("conn-text");
  const activeTopicEl = document.getElementById("active-topic");
  const activeBrokerEl = document.getElementById("active-broker");
  const epsCounterEl = document.getElementById("eps-counter");

  const tabBtns = document.querySelectorAll(".tab-btn");
  const tabPanes = document.querySelectorAll(".tab-pane");

  const streamTbody = document.getElementById("stream-tbody");
  const filterInput = document.getElementById("stream-filter-input");
  const btnTogglePause = document.getElementById("btn-toggle-pause");
  const btnClear = document.getElementById("btn-clear-stream");

  const detailHeaders = document.getElementById("detail-headers");
  const detailPayload = document.getElementById("detail-payload");
  const btnCopyPayload = document.getElementById("btn-copy-payload");

  const dlqList = document.getElementById("dlq-items-list");
  const btnRefreshDlq = document.getElementById("btn-refresh-dlq");
  const diagCategory = document.getElementById("diag-category");
  const diagOriginalTopic = document.getElementById("diag-original-topic");
  const diagRootCause = document.getElementById("diag-root-cause");
  const diagAction = document.getElementById("diag-action");
  const diagStacktrace = document.getElementById("diag-stacktrace");
  const dlqEditor = document.getElementById("dlq-payload-editor");
  const redriveTargetInput = document.getElementById("redrive-target-input");
  const btnExecuteRedrive = document.getElementById("btn-execute-redrive");
  const redriveStatusBar = document.getElementById("redrive-status-bar");
  const dlqCountBadge = document.getElementById("dlq-count-badge");

  const lagGroupId = document.getElementById("lag-group-id");
  const lagTotalVal = document.getElementById("lag-total-val");
  const lagPartCount = document.getElementById("lag-part-count");
  const lagTbody = document.getElementById("lag-tbody");

  // Tab Switching
  tabBtns.forEach((btn) => {
    btn.addEventListener("click", () => {
      tabBtns.forEach((b) => b.classList.remove("active"));
      tabPanes.forEach((p) => p.classList.remove("active"));

      btn.classList.add("active");
      const targetTab = btn.dataset.tab;
      document.getElementById(`pane-${targetTab}`).classList.add("active");

      if (targetTab === "dlq") {
        fetchDLQ();
      } else if (targetTab === "lag") {
        fetchLag();
      }
    });
  });

  // Fetch initial status
  async function fetchStatus() {
    try {
      const res = await fetch("/api/status");
      const data = await res.json();
      activeTopicEl.textContent = data.topic;
      activeBrokerEl.textContent = data.demo ? "DEMO CLUSTER" : data.broker;
      redriveTargetInput.value = data.topic;
    } catch (err) {
      console.error("Failed fetching status", err);
    }
  }

  // WebSocket Setup
  function connectWebSocket() {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${protocol}//${window.location.host}/ws/events`;
    ws = new WebSocket(wsUrl);

    ws.onopen = () => {
      connStatus.innerHTML = '<span class="status-dot pulse-green"></span><span>Connected</span>';
    };

    ws.onclose = () => {
      connStatus.innerHTML = '<span class="status-dot" style="background:#f87171"></span><span>Disconnected</span>';
      setTimeout(connectWebSocket, 2000);
    };

    ws.onerror = (err) => {
      console.error("WS error:", err);
      ws.close();
    };

    ws.onmessage = (event) => {
      try {
        const msg = jsonParse(event.data);
        if (msg.type === "record") {
          handleIncomingRecord(msg.data);
        }
      } catch (e) {
        console.error("Failed to parse message", e);
      }
    };
  }

  function handleIncomingRecord(record) {
    eventCounter++;
    records.unshift(record);
    if (records.length > 500) records.pop();

    if (record.is_dlq) {
      updateDlqCount(1);
    }

    if (!isPaused && recordMatchesFilter(record)) {
      renderRecordRow(record, true);
    }
  }

  // Throughput counter
  setInterval(() => {
    const now = Date.now();
    const elapsed = (now - lastTime) / 1000;
    const eps = eventCounter / elapsed;
    epsCounterEl.textContent = `${eps.toFixed(1)} eps`;
    eventCounter = 0;
    lastTime = now;
  }, 1000);

  function recordMatchesFilter(rec) {
    if (!filterText) return true;
    const q = filterText.toLowerCase();
    const str = JSON.stringify(rec).toLowerCase();
    return str.includes(q);
  }

  function renderRecordRow(rec, prepend = false) {
    const tr = document.createElement("tr");
    tr.dataset.offset = rec.offset;
    tr.dataset.partition = rec.partition;

    const statusHtml = rec.is_dlq
      ? '<span class="status-tag dlq">DLQ</span>'
      : '<span class="status-tag ok">OK</span>';

    const payloadPreview = typeof rec.payload === "object"
      ? JSON.stringify(rec.payload)
      : String(rec.payload || "");

    const timeStr = rec.timestamp
      ? new Date(rec.timestamp).toLocaleTimeString()
      : "-";

    tr.innerHTML = `
      <td>${statusHtml}</td>
      <td>${rec.partition}</td>
      <td>${rec.offset}</td>
      <td><strong>${rec.key || "-"}</strong></td>
      <td>${rec.format}</td>
      <td title="${escapeHtml(payloadPreview)}">${escapeHtml(payloadPreview)}</td>
      <td>${timeStr}</td>
    `;

    tr.addEventListener("click", () => {
      document.querySelectorAll("#stream-tbody tr").forEach((r) => r.classList.remove("selected"));
      tr.classList.add("selected");
      selectRecord(rec);
    });

    if (prepend && streamTbody.firstChild) {
      streamTbody.insertBefore(tr, streamTbody.firstChild);
      if (streamTbody.children.length > 250) {
        streamTbody.removeChild(streamTbody.lastChild);
      }
    } else {
      streamTbody.appendChild(tr);
    }
  }

  function selectRecord(rec) {
    selectedRecord = rec;
    const headersText = Object.keys(rec.headers).length
      ? JSON.stringify(rec.headers, null, 2)
      : "(No headers)";
    detailHeaders.textContent = headersText;

    const payloadText = typeof rec.payload === "object"
      ? JSON.stringify(rec.payload, null, 2)
      : String(rec.payload || "");
    detailPayload.textContent = payloadText;
  }

  // Filter input
  filterInput.addEventListener("input", (e) => {
    filterText = e.target.value.trim();
    rebuildStreamTable();
  });

  function rebuildStreamTable() {
    streamTbody.innerHTML = "";
    records.filter(recordMatchesFilter).slice(0, 200).forEach((rec) => {
      renderRecordRow(rec, false);
    });
  }

  btnTogglePause.addEventListener("click", () => {
    isPaused = !isPaused;
    btnTogglePause.textContent = isPaused ? "Resume Feed" : "Pause Feed";
    btnTogglePause.classList.toggle("btn-danger", isPaused);
  });

  btnClear.addEventListener("click", () => {
    streamTbody.innerHTML = "";
    records = [];
  });

  btnCopyPayload.addEventListener("click", () => {
    if (selectedRecord) {
      const text = typeof selectedRecord.payload === "object"
        ? JSON.stringify(selectedRecord.payload, null, 2)
        : String(selectedRecord.payload || "");
      navigator.clipboard.writeText(text);
      btnCopyPayload.style.color = "var(--accent-green)";
      setTimeout(() => (btnCopyPayload.style.color = ""), 1500);
    }
  });

  // DLQ Studio Logic
  async function fetchDLQ() {
    try {
      const res = await fetch("/api/dlq");
      dlqRecords = await res.json();
      dlqCountBadge.textContent = dlqRecords.length;
      renderDLQList();
    } catch (e) {
      console.error("Failed fetching DLQ", e);
    }
  }

  function renderDLQList() {
    dlqList.innerHTML = "";
    if (dlqRecords.length === 0) {
      dlqList.innerHTML = '<div style="padding:1rem; color:var(--text-dim)">No poison pills recorded.</div>';
      return;
    }

    dlqRecords.forEach((item, idx) => {
      const el = document.createElement("div");
      el.className = `dlq-item ${idx === 0 ? "active" : ""}`;
      const err = item.headers?.["x-exception-message"] || item.error_message || "Error";

      el.innerHTML = `
        <div class="dlq-item-top">
          <span class="dlq-offset">Offset ${item.offset}</span>
          <span class="dlq-part">P:${item.partition}</span>
        </div>
        <div class="dlq-err-preview">${escapeHtml(err)}</div>
      `;

      el.addEventListener("click", () => {
        document.querySelectorAll(".dlq-item").forEach((i) => i.classList.remove("active"));
        el.classList.add("active");
        selectDlqItem(item);
      });

      dlqList.appendChild(el);
    });

    if (dlqRecords.length > 0) {
      selectDlqItem(dlqRecords[0]);
    }
  }

  function selectDlqItem(item) {
    selectedDlqRecord = item;
    const diag = item.diagnostic || {};
    diagCategory.textContent = diag.failure_category || "Poison Pill";
    diagOriginalTopic.textContent = `Original Topic: ${diag.original_topic || "order.events"}`;
    diagRootCause.textContent = diag.root_cause || "-";
    diagAction.textContent = diag.suggested_action || "-";
    diagStacktrace.textContent = diag.stack_trace || "(No stack trace found in headers)";

    redriveTargetInput.value = diag.original_topic || "order.events";

    const payloadText = typeof item.payload === "object"
      ? JSON.stringify(item.payload, null, 2)
      : String(item.payload || "");
    dlqEditor.value = payloadText;
  }

  btnRefreshDlq.addEventListener("click", fetchDLQ);

  btnExecuteRedrive.addEventListener("click", async () => {
    if (!selectedDlqRecord) return;
    const targetTopic = redriveTargetInput.value.trim();
    if (!targetTopic) {
      alert("Specify destination topic");
      return;
    }

    let patched = null;
    try {
      patched = JSON.parse(dlqEditor.value);
    } catch {
      patched = dlqEditor.value;
    }

    btnExecuteRedrive.disabled = true;
    btnExecuteRedrive.textContent = "Publishing...";

    try {
      const res = await fetch("/api/dlq/redrive", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          source_topic: selectedDlqRecord.topic,
          target_topic: targetTopic,
          offset: selectedDlqRecord.offset,
          partition: selectedDlqRecord.partition,
          patched_payload: patched,
          headers: selectedDlqRecord.headers || {},
        }),
      });
      const result = await res.json();
      redriveStatusBar.className = `notification-bar ${result.success ? "success" : "error"}`;
      redriveStatusBar.textContent = result.success
        ? `[OK] Successfully redriven to ${result.target_topic} (Partition: ${result.produced_partition}, Offset: ${result.produced_offset})`
        : `[FAIL] Redrive failed: ${result.error}`;
    } catch (err) {
      redriveStatusBar.className = "notification-bar error";
      redriveStatusBar.textContent = `Error: ${err.message}`;
    } finally {
      btnExecuteRedrive.disabled = false;
      btnExecuteRedrive.textContent = "Redrive Message";
    }
  });

  // Lag Monitor
  async function fetchLag() {
    try {
      const res = await fetch("/api/lag");
      const lag = await res.json();
      lagGroupId.textContent = lag.group_id;
      lagTotalVal.textContent = lag.total_lag;
      lagPartCount.textContent = lag.partitions?.length || 0;

      lagTbody.innerHTML = "";
      (lag.partitions || []).forEach((p) => {
        const tr = document.createElement("tr");
        const maxExpectedLag = 300;
        const pct = Math.min(100, Math.round((p.lag / maxExpectedLag) * 100));
        const colorClass = p.lag > 100 ? "danger" : p.lag > 30 ? "warn" : "";

        tr.innerHTML = `
          <td>Partition ${p.partition}</td>
          <td>${p.log_end_offset}</td>
          <td>${p.current_offset}</td>
          <td><strong>${p.lag}</strong></td>
          <td>
            <div class="lag-bar-wrapper">
              <div class="lag-bar-fill ${colorClass}" style="width: ${Math.max(5, pct)}%"></div>
            </div>
          </td>
        `;
        lagTbody.appendChild(tr);
      });
    } catch (err) {
      console.error("Failed fetching lag data", err);
    }
  }

  function updateDlqCount(delta) {
    const curr = parseInt(dlqCountBadge.textContent, 10) || 0;
    dlqCountBadge.textContent = curr + delta;
  }

  function escapeHtml(str) {
    if (!str) return "";
    return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }

  function jsonParse(str) {
    try {
      return JSON.parse(str);
    } catch {
      return {};
    }
  }

  // Boot
  fetchStatus();
  connectWebSocket();
});
