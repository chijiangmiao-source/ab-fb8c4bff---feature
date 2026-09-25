/* 标定谱系页面：所有数据均来自真实接口 */
const $ = (sel) => document.querySelector(sel);

const state = { records: [] };

function esc(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

async function api(method, path, body) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const resp = await fetch(path, opts);
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    const err = new Error(data?.error?.message || `HTTP ${resp.status}`);
    err.status = resp.status;
    err.code = data?.error?.code;
    err.details = data?.error?.details;
    throw err;
  }
  return data;
}

function feedback(msg, cls) {
  const el = $("#feedback");
  el.textContent = msg;
  el.className = "feedback " + (cls || "muted");
}

async function refresh() {
  const records = await api("GET", "/api/records");
  state.records = records;
  renderRecords();
  renderParentOptions();
}

function renderRecords() {
  if (!state.records.length) {
    $("#records").innerHTML = '<p class="muted">暂无记录，先建立一条原始记录吧。</p>';
    return;
  }
  $("#records").innerHTML = state.records.map((r) => {
    const text = typeof r.payload.value !== "undefined"
      ? r.payload.value : JSON.stringify(r.payload);
    const basis = r.parent_ids.length
      ? `<div class="parents-line">直接依据：${
          r.parent_ids.map((p) => `<span class="pid">${esc(p)}</span>`).join("、")
        }</div>`
      : "";
    const src = r.invalidated_by
      ? `<div class="meta">失效来源（稳定）：<span class="src">${esc(r.invalidated_by)}</span>${
          r.invalidated_at ? ` · ${esc(r.invalidated_at)}` : ""
        }</div>`
      : "";
    return `
      <div class="record">
        <div class="head">
          <span class="id">${esc(r.id)}</span>
          <span class="badge ${esc(r.kind)}">${r.kind === "raw" ? "原始" : "推导"}</span>
          <span class="badge ${esc(r.status)}">${r.status === "valid" ? "有效" : "已失效"}</span>
          <button type="button" class="linklike export-btn" data-export="${esc(r.id)}">导出转交包</button>
        </div>
        <div class="meta">${esc(text)}</div>
        ${basis}
        ${src}
      </div>`;
  }).join("");
}

function renderParentOptions() {
  const valids = state.records.filter((r) => r.status === "valid");
  const box = $("#parents");
  if (!valids.length) {
    box.innerHTML = '<span class="muted">当前没有可引用的有效记录</span>';
    return;
  }
  box.innerHTML = valids.map((r) => `
    <label><input type="checkbox" value="${esc(r.id)}">
      <span class="id">${esc(r.id)}</span>
      <span class="muted">（${r.kind === "raw" ? "原始" : "推导"}）</span>
    </label>`).join("");
}

$("#kind").addEventListener("change", () => {
  $("#parents-row").hidden = $("#kind").value !== "derived";
});

$("#create-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const body = {
    kind: $("#kind").value,
    payload: { value: $("#value").value || "" },
  };
  if ($("#kind").value === "derived") {
    body.parent_ids = [...document.querySelectorAll("#parents input:checked")]
      .map((c) => c.value);
  }
  try {
    const rec = await api("POST", "/api/records", body);
    feedback(`已创建记录 ${rec.id}（${rec.status}），直接依据：${
      rec.parent_ids.length ? rec.parent_ids.join("、") : "无"
    }`, "ok-text");
    $("#value").value = "";
    await refresh();
  } catch (err) {
    feedback(`创建被拒绝（${err.code || err.status}）：${err.message}\n` +
      (err.details ? `定位信息：${JSON.stringify(err.details, null, 2)}` : ""),
      "error-text");
  }
});

$("#invalidate-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const target = $("#inv-target").value.trim();
  const op = $("#inv-op").value.trim();
  try {
    const res = await api("POST",
      `/api/records/${encodeURIComponent(target)}/invalidate`,
      { operation_id: op });
    feedback(
      `裁决完成${res.replayed ? "（重复裁决，返回首次结果）" : ""}\n` +
      `操作标识：${res.operation_id}\n失效来源：${res.target_record_id}\n` +
      `级联失效 ${res.cascade.length} 条：${res.cascade.map((c) => c.id).join("、")}`,
      "ok-text");
    await refresh();
  } catch (err) {
    feedback(`裁决失败（${err.code || err.status}）：${err.message}\n` +
      (err.details ? `定位信息：${JSON.stringify(err.details, null, 2)}` : ""),
      "error-text");
  }
});

$("#refresh").addEventListener("click", () =>
  refresh().catch((e) => feedback(e.message, "error-text")));

// ---------------- 密封转交包：导出 / 接入 ---------------- #
function renderPackageSummary(label, pkg) {
  return `${label}\n` +
    `包标识：${pkg.package_id}\n` +
    `包摘要：${pkg.payload_digest}\n` +
    `根外部标识：${pkg.root_external_id}\n` +
    `闭包记录数：${pkg.records.length}`;
}

async function exportPackage(target) {
  const res = await api("POST",
    `/api/records/${encodeURIComponent(target)}/export`, {});
  $("#export-target").value = target;
  $("#export-box").value = JSON.stringify(res.package, null, 2);
  feedback(
    `${renderPackageSummary("已导出密封转交包", res.package)}\n` +
    `本地根编号：${res.root_record_id}\n直接依据：${
      res.root_parent_ids.length ? res.root_parent_ids.join("、") : "无（原始记录）"}`,
    "ok-text");
}

// 记录卡片上的「导出转交包」按钮（事件委托）
$("#records").addEventListener("click", async (e) => {
  const btn = e.target.closest("[data-export]");
  if (!btn) return;
  try {
    await exportPackage(btn.getAttribute("data-export"));
  } catch (err) {
    feedback(`导出被拒绝（${err.code || err.status}）：${err.message}\n` +
      (err.details ? `定位信息：${JSON.stringify(err.details, null, 2)}` : ""),
      "error-text");
  }
});

$("#export-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  try {
    await exportPackage($("#export-target").value.trim());
  } catch (err) {
    feedback(`导出被拒绝（${err.code || err.status}）：${err.message}\n` +
      (err.details ? `定位信息：${JSON.stringify(err.details, null, 2)}` : ""),
      "error-text");
  }
});

$("#import-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const raw = $("#import-box").value.trim();
  let envelope;
  try {
    envelope = JSON.parse(raw);
  } catch (_err) {
    feedback("接入被拒绝：转交包不是合法 JSON", "error-text");
    return;
  }
  try {
    const res = await api("POST", "/api/packages/import",
      envelope.format ? envelope : { package: envelope });
    const root = res.root_record || {};
    feedback(
      `${res.replayed ? "重复接入：返回首次映射" : "接入完成"}\n` +
      `包标识：${res.package_id}\n包摘要：${res.payload_digest}\n` +
      `导入本地根编号：${res.root_record_id}\n直接依据：${
        (root.parent_ids || []).length
          ? root.parent_ids.join("、") : "无（原始记录）"}\n` +
      `新写入 ${res.imported_count ?? res.record_count} 条，` +
      `复用既有映射 ${res.reused_count ?? 0} 条`,
      "ok-text");
    await refresh();
  } catch (err) {
    feedback(`接入被原子拒绝（${err.code || err.status}）：${err.message}\n` +
      (err.details ? `定位信息：${JSON.stringify(err.details, null, 2)}` : ""),
      "error-text");
  }
});

refresh().catch((e) => feedback(e.message, "error-text"));
