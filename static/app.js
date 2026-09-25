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
    const ext = r.ext_id
      ? `<div class="meta">外部标识：<span class="pid">${esc(r.ext_id)}</span></div>`
      : "";
    const exportBtn = r.status === "valid"
      ? `<button type="button" class="linklike export-btn" data-id="${esc(r.id)}">导出密封包</button>`
      : "";
    return `
      <div class="record">
        <div class="head">
          <span class="id">${esc(r.id)}</span>
          <span class="badge ${esc(r.kind)}">${r.kind === "raw" ? "原始" : "推导"}</span>
          <span class="badge ${esc(r.status)}">${r.status === "valid" ? "有效" : "已失效"}</span>
          ${exportBtn}
        </div>
        <div class="meta">${esc(text)}</div>
        ${basis}
        ${ext}
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
$("#records").addEventListener("click", async (e) => {
  const btn = e.target.closest(".export-btn");
  if (!btn) return;
  const id = btn.dataset.id;
  try {
    const pkg = await api("POST",
      `/api/records/${encodeURIComponent(id)}/export`, {});
    $("#export-package").value = JSON.stringify(pkg, null, 2);
    const root = pkg.records.find((r) => r.ext_id === pkg.root_ext_id);
    $("#export-summary").innerHTML =
      `包标识 <span class="pid">${esc(pkg.package_id)}</span><br>` +
      `摘要 <span class="pid">${esc(pkg.digest)}</span><br>` +
      `转交结论外部标识 <span class="pid">${esc(pkg.root_ext_id)}</span>` +
      `（本地 ${esc(id)}），闭包记录 ${pkg.records.length} 条，` +
      `直接依据 ${(root?.parent_ext_ids || []).map(esc).join("、") || "无"}`;
    feedback(`已导出密封转交包 ${pkg.package_id}（${pkg.records.length} 条记录）`,
      "ok-text");
  } catch (err) {
    feedback(`导出被拒绝（${err.code || err.status}）：${err.message}`,
      "error-text");
  }
});

$("#import-btn").addEventListener("click", async () => {
  const text = $("#import-package").value.trim();
  if (!text) {
    feedback("请先粘贴密封转交包 JSON", "error-text");
    return;
  }
  let body;
  try {
    body = JSON.parse(text);
  } catch (err) {
    feedback(`接入失败：粘贴内容不是合法 JSON（${err.message}）`, "error-text");
    return;
  }
  try {
    const res = await api("POST", "/api/packages/import", body);
    $("#import-summary").innerHTML =
      `${res.replayed ? "重复接入，返回首次映射" : "接入成功"}<br>` +
      `包标识 <span class="pid">${esc(res.package_id)}</span><br>` +
      `摘要 <span class="pid">${esc(res.digest)}</span><br>` +
      `导入后本地编号 <span class="pid">${esc(res.root_record_id)}</span>` +
      `（有效：${res.root_status}）<br>` +
      `直接依据 ${(res.direct_parent_ids || []).map(esc).join("、") || "无"}<br>` +
      `写入映射 ${res.mapping.length} 条（复用既有 ${res.reused_ext_ids.length} 条）`;
    feedback(
      `${res.replayed ? "重复接入（幂等）" : "接入完成"}：` +
      `根本地编号 ${res.root_record_id}，映射 ${res.mapping.length} 条`,
      "ok-text");
    await refresh();
  } catch (err) {
    feedback(`接入被拒绝（${err.code || err.status}）：${err.message}\n` +
      (err.details ? `定位信息：${JSON.stringify(err.details, null, 2)}` : ""),
      "error-text");
    $("#import-summary").textContent = "接入被原子拒绝，本地既有结论未改变";
  }
});

refresh().catch((e) => feedback(e.message, "error-text"));
