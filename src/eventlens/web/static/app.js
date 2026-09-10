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
  let soundEnabled = false;
  let audioCtx = null;

  // DOM Elements - Navigation & Header
  const connStatus = document.getElementById("connection-status");
  const activeTopicEl = document.getElementById("active-topic");
  const activeBrokerEl = document.getElementById("active-broker");
  const epsCounterEl = document.getElementById("eps-counter");

  const tabBtns = document.querySelectorAll(".tab-btn");
  const tabPanes = document.querySelectorAll(".tab-pane");

  // Stream Tab Elements
  const streamTbody = document.getElementById("stream-tbody");
  const filterInput = document.getElementById("stream-filter-input");
  const btnTogglePause = document.getElementById("btn-toggle-pause");
  const btnClear = document.getElementById("btn-clear-stream");

  const detailHeaders = document.getElementById("detail-headers");
  const detailPayload = document.getElementById("detail-payload");
  const btnCopyPayload = document.getElementById("btn-copy-payload");

  // DLQ Tab Elements
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

  // Lag Tab Elements
  const lagGroupId = document.getElementById("lag-group-id");
  const lagTotalVal = document.getElementById("lag-total-val");
  const lagPartCount = document.getElementById("lag-part-count");
  const lagTbody = document.getElementById("lag-tbody");

  // Trace Tab Elements
  const traceInput = document.getElementById("trace-input");
  const btnRunTrace = document.getElementById("btn-run-trace");
  const traceSummaryBar = document.getElementById("trace-summary-bar");
  const traceTbody = document.getElementById("trace-tbody");

  // Diff Tab Elements
  const diffInputA = document.getElementById("diff-input-a");
  const diffInputB = document.getElementById("diff-input-b");
  const btnRunDiff = document.getElementById("btn-run-diff");
  const diffResultsContainer = document.getElementById("diff-results-container");

  // Produce Modal Elements
  const btnOpenProduce = document.getElementById("btn-open-produce");
  const produceModal = document.getElementById("produce-modal");
  const btnCloseProduce = document.getElementById("btn-close-produce");
  const btnCancelProduce = document.getElementById("btn-cancel-produce");
  const btnSubmitProduce = document.getElementById("btn-submit-produce");
  const produceTopic = document.getElementById("produce-topic");
  const produceKey = document.getElementById("produce-key");
  const producePayload = document.getElementById("produce-payload");
  const produceStatusBar = document.getElementById("produce-status-bar");

  // Alert & Export Elements
  const btnToggleSound = document.getElementById("btn-toggle-sound");
  const globalAlertBanner = document.getElementById("global-alert-banner");
  const btnExportDropdown = document.getElementById("btn-export-dropdown");
  const exportMenu = document.getElementById("export-menu");

  // Tab Switching
  tabBtns.forEach((btn) => {
    btn.addEventListener("click", () => {
      tabBtns.forEach((b) => b.classList.remove("active"));
      tabPanes.forEach((p) => p.classList.remove("active"));

      btn.classList.add("active");
      const targetTab = btn.dataset.tab;
      const targetPane = document.getElementById(`pane-${targetTab}`);
      if (targetPane) targetPane.classList.add("active");

      if (targetTab === "dlq") {
        fetchDLQ();
      } else if (targetTab === "lag") {
        fetchLag();
      } else if (targetTab === "trace") {
        if (!traceInput.value) {
          traceInput.value = "ord_9011";
        }
        executeTrace();
      } else if (targetTab === "diff") {
        if (btnRunDiff) {
          btnRunDiff.click();
        }
      }
    });
  });

  // Sound and Alerting
  function playAlertBeep() {
    if (!soundEnabled) return;
    try {
      if (!audioCtx) {
        audioCtx = new (window.AudioContext || window.webkitAudioContext)();
      }
      if (audioCtx.state === "suspended") {
        audioCtx.resume();
      }
      const osc = audioCtx.createOscillator();
      const gain = audioCtx.createGain();
      osc.type = "sine";
      osc.frequency.setValueAtTime(587.33, audioCtx.currentTime); // D5
      osc.frequency.setValueAtTime(880, audioCtx.currentTime + 0.08); // A5
      gain.gain.setValueAtTime(0.15, audioCtx.currentTime);
      gain.gain.exponentialRampToValueAtTime(0.001, audioCtx.currentTime + 0.25);
      osc.connect(gain);
      gain.connect(audioCtx.destination);
      osc.start();
      osc.stop(audioCtx.currentTime + 0.25);
    } catch (e) {
      console.warn("AudioContext playback blocked", e);
    }
  }

  let bannerTimer = null;
  function showAlertBanner(message, type = "warn") {
    if (!globalAlertBanner) return;
    globalAlertBanner.textContent = message;
    globalAlertBanner.className = `alert-banner show ${type}`;
    if (bannerTimer) clearTimeout(bannerTimer);
    bannerTimer = setTimeout(() => {
      globalAlertBanner.className = "alert-banner";
    }, 4500);
  }

  if (btnToggleSound) {
    btnToggleSound.addEventListener("click", () => {
      soundEnabled = !soundEnabled;
      btnToggleSound.textContent = soundEnabled ? "🔊" : "🔔";
      btnToggleSound.style.color = soundEnabled ? "var(--accent-green)" : "";
      if (soundEnabled) {
        showAlertBanner("Audio alerts enabled for poison pills", "info");
        playAlertBeep();
      }
    });
  }

  // Export Dropdown
  if (btnExportDropdown && exportMenu) {
    btnExportDropdown.addEventListener("click", (e) => {
      e.stopPropagation();
      exportMenu.classList.toggle("show");
    });
    document.addEventListener("click", () => {
      exportMenu.classList.remove("show");
    });
  }

  // Fetch initial status
  async function fetchStatus() {
    try {
      const res = await fetch("/api/status");
      const data = await res.json();
      if (activeTopicEl) activeTopicEl.textContent = data.topic;
      if (activeBrokerEl) activeBrokerEl.textContent = data.demo ? "DEMO CLUSTER" : data.broker;
      if (redriveTargetInput) redriveTargetInput.value = data.topic;
      if (produceTopic) produceTopic.value = data.topic;
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
      playAlertBeep();
      showAlertBanner(`Poison Pill detected on ${record.topic} [P:${record.partition} Offset:${record.offset}]!`);
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
    if (epsCounterEl) epsCounterEl.textContent = `${eps.toFixed(1)} eps`;
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
    const headersText = Object.keys(rec.headers || {}).length
      ? JSON.stringify(rec.headers, null, 2)
      : "(No headers)";
    detailHeaders.textContent = headersText;

    const payloadText = typeof rec.payload === "object"
      ? JSON.stringify(rec.payload, null, 2)
      : String(rec.payload || "");
    detailPayload.textContent = payloadText;
  }

  // Filter input
  if (filterInput) {
    filterInput.addEventListener("input", (e) => {
      filterText = e.target.value.trim();
      rebuildStreamTable();
    });
  }

  function rebuildStreamTable() {
    streamTbody.innerHTML = "";
    records.filter(recordMatchesFilter).slice(0, 200).forEach((rec) => {
      renderRecordRow(rec, false);
    });
  }

  if (btnTogglePause) {
    btnTogglePause.addEventListener("click", () => {
      isPaused = !isPaused;
      btnTogglePause.textContent = isPaused ? "Resume Feed" : "Pause Feed";
      btnTogglePause.classList.toggle("btn-danger", isPaused);
    });
  }

  if (btnClear) {
    btnClear.addEventListener("click", () => {
      streamTbody.innerHTML = "";
      records = [];
    });
  }

  if (btnCopyPayload) {
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
  }

  // DLQ Studio Logic
  async function fetchDLQ() {
    try {
      const res = await fetch("/api/dlq");
      dlqRecords = await res.json();
      if (dlqCountBadge) dlqCountBadge.textContent = dlqRecords.length;
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

  if (btnRefreshDlq) {
    btnRefreshDlq.addEventListener("click", fetchDLQ);
  }

  if (btnExecuteRedrive) {
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
  }

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
    if (!dlqCountBadge) return;
    const curr = parseInt(dlqCountBadge.textContent, 10) || 0;
    dlqCountBadge.textContent = curr + delta;
  }

  // Event Trace Feature
  async function executeTrace() {
    const cid = (traceInput.value || "").trim();
    if (!cid) {
      showAlertBanner("Please enter a correlation or order ID to trace", "warn");
      return;
    }

    btnRunTrace.disabled = true;
    btnRunTrace.textContent = "Tracing...";
    traceSummaryBar.textContent = `Searching event lifecycle for: "${cid}"...`;
    traceTbody.innerHTML = '<tr><td colspan="6" style="text-align:center; padding:1.5rem; color:var(--text-dim);">Querying cluster topics...</td></tr>';

    try {
      const res = await fetch(`/api/trace?correlation_id=${encodeURIComponent(cid)}`);
      const graph = await res.json();

      const topics = [...new Set((graph.hops || []).map((h) => h.topic))];
      const statusBadge = graph.has_dlq ? "badge-red" : "badge-green";
      const statusText = graph.has_dlq ? "POISON (DLQ)" : "SUCCESS";
      const durationMs = graph.total_latency_ms !== null && graph.total_latency_ms !== undefined ? graph.total_latency_ms : 0;

      traceSummaryBar.innerHTML = `
        <strong>Trace ID:</strong> <code>${escapeHtml(graph.correlation_id)}</code> &nbsp;|&nbsp; 
        <strong>Total Hops:</strong> ${graph.hops?.length || 0} &nbsp;|&nbsp; 
        <strong>Duration:</strong> ${durationMs} ms &nbsp;|&nbsp; 
        <strong>Topics:</strong> ${topics.join(" &rarr; ")} &nbsp;|&nbsp; 
        <span class="badge ${statusBadge}">${statusText}</span>
      `;

      traceTbody.innerHTML = "";
      if (!graph.hops || graph.hops.length === 0) {
        traceTbody.innerHTML = '<tr><td colspan="6" style="text-align:center; padding:1.5rem; color:var(--text-dim);">No events matched this correlation ID.</td></tr>';
        return;
      }

      graph.hops.forEach((hop, idx) => {
        const tr = document.createElement("tr");
        const hopStatusClass = hop.is_dlq ? "dlq" : "ok";
        const topicBadge = hop.is_dlq ? "badge-red" : "badge-green";
        const actionStr = hop.event_type || hop.action || (hop.is_dlq ? "DeadLetter" : "Message");
        const latencyStr = hop.latency_from_prev_ms !== null && hop.latency_from_prev_ms !== undefined ? `+${hop.latency_from_prev_ms} ms` : "START";

        tr.innerHTML = `
          <td><strong>#${idx + 1}</strong></td>
          <td><span class="badge ${topicBadge}">${escapeHtml(hop.topic)}</span></td>
          <td>P:${hop.partition} #${hop.offset}</td>
          <td><strong>${escapeHtml(actionStr)}</strong></td>
          <td>${latencyStr}</td>
          <td><span class="status-tag ${hopStatusClass}">${escapeHtml(hop.status || (hop.is_dlq ? "DLQ_POISON" : "OK"))}</span></td>
        `;
        traceTbody.appendChild(tr);
      });
    } catch (err) {
      traceSummaryBar.textContent = `Trace error: ${err.message}`;
      traceTbody.innerHTML = `<tr><td colspan="6" style="color:var(--accent-red); padding:1rem;">Failed to fetch trace graph: ${escapeHtml(err.message)}</td></tr>`;
    } finally {
      btnRunTrace.disabled = false;
      btnRunTrace.textContent = "Search Trace";
    }
  }

  if (btnRunTrace) {
    btnRunTrace.addEventListener("click", executeTrace);
  }
  if (traceInput) {
    traceInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") executeTrace();
    });
  }

  // Diff Tool Feature
  if (btnRunDiff) {
    btnRunDiff.addEventListener("click", async () => {
      let objA, objB;
      try {
        objA = JSON.parse(diffInputA.value);
      } catch {
        objA = diffInputA.value;
      }
      try {
        objB = JSON.parse(diffInputB.value);
      } catch {
        objB = diffInputB.value;
      }

      btnRunDiff.disabled = true;
      btnRunDiff.textContent = "Comparing...";

      try {
        const res = await fetch("/api/diff", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ payload_a: objA, payload_b: objB }),
        });
        const diffData = await res.json();

        if (diffData.identical) {
          diffResultsContainer.innerHTML = `
            <div style="padding:1.5rem; background:rgba(34,197,94,0.1); border:1px solid var(--accent-green); border-radius:6px; color:var(--accent-green); text-align:center;">
              <strong>Paylod Identical</strong> — No property additions, deletions, or type mutations found.
            </div>
          `;
          return;
        }

        let rowsHtml = "";
        (diffData.items || []).forEach((item) => {
          const typeBadge = item.diff_type === "added" ? "badge-green" : item.diff_type === "removed" ? "badge-red" : "badge-orange";
          const oldValStr = item.old_value !== null ? escapeHtml(JSON.stringify(item.old_value)) : "<span style='color:var(--text-dim)'>null</span>";
          const newValStr = item.new_value !== null ? escapeHtml(JSON.stringify(item.new_value)) : "<span style='color:var(--text-dim)'>null</span>";

          rowsHtml += `
            <tr>
              <td><code>${escapeHtml(item.path)}</code></td>
              <td><span class="badge ${typeBadge}">${item.diff_type.toUpperCase()}</span></td>
              <td><pre style="margin:0; font-size:0.8rem;">${oldValStr}</pre></td>
              <td><pre style="margin:0; font-size:0.8rem;">${newValStr}</pre></td>
            </tr>
          `;
        });

        diffResultsContainer.innerHTML = `
          <div style="margin-bottom:1rem; display:flex; gap:1rem; font-size:0.85rem;">
            <span class="badge badge-green">Added: ${diffData.added_count}</span>
            <span class="badge badge-red">Removed: ${diffData.removed_count}</span>
            <span class="badge badge-orange">Modified: ${diffData.modified_count}</span>
          </div>
          <table class="stream-table">
            <thead>
              <tr>
                <th width="200">Property Path</th>
                <th width="110">Diff Type</th>
                <th>Baseline (A)</th>
                <th>Target (B)</th>
              </tr>
            </thead>
            <tbody>${rowsHtml}</tbody>
          </table>
        `;
      } catch (err) {
        diffResultsContainer.innerHTML = `<div style="color:var(--accent-red); padding:1rem;">Error running diff: ${escapeHtml(err.message)}</div>`;
      } finally {
        btnRunDiff.disabled = false;
        btnRunDiff.textContent = "Compare Payloads";
      }
    });
  }

  // Produce Modal Logic
  if (btnOpenProduce) {
    btnOpenProduce.addEventListener("click", () => {
      produceModal.classList.add("active");
      produceStatusBar.className = "notification-bar";
      produceStatusBar.textContent = "";
    });
  }

  function closeProduceModal() {
    produceModal.classList.remove("active");
  }

  if (btnCloseProduce) btnCloseProduce.addEventListener("click", closeProduceModal);
  if (btnCancelProduce) btnCancelProduce.addEventListener("click", closeProduceModal);

  if (btnSubmitProduce) {
    btnSubmitProduce.addEventListener("click", async () => {
      const topic = produceTopic.value.trim();
      const key = produceKey.value.trim() || null;
      if (!topic) {
        alert("Topic is required");
        return;
      }

      let parsedPayload;
      try {
        parsedPayload = JSON.parse(producePayload.value);
      } catch {
        parsedPayload = producePayload.value;
      }

      btnSubmitProduce.disabled = true;
      btnSubmitProduce.textContent = "Publishing...";

      try {
        const res = await fetch("/api/produce", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            topic: topic,
            key: key,
            payload: parsedPayload,
            headers: { "x-produced-by": "eventlens-web" },
          }),
        });
        const result = await res.json();
        produceStatusBar.className = "notification-bar success";
        produceStatusBar.textContent = `[OK] Published to ${topic} (Partition: ${result.partition}, Offset: ${result.offset})`;
        setTimeout(closeProduceModal, 1800);
      } catch (err) {
        produceStatusBar.className = "notification-bar error";
        produceStatusBar.textContent = `Failed: ${err.message}`;
      } finally {
        btnSubmitProduce.disabled = false;
        btnSubmitProduce.textContent = "Publish Event";
      }
    });
  }

  function escapeHtml(str) {
    if (!str) return "";
    return String(str).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
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

  function handleHash() {
    const hash = window.location.hash.replace("#", "");
    if (hash) {
      const btn = document.querySelector(`.tab-btn[data-tab="${hash}"]`);
      if (btn) btn.click();
    }
  }
  handleHash();
  window.addEventListener("hashchange", handleHash);
});
