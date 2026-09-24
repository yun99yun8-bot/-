const API = "https://api.trongrid.io/wallet/getnowblock";
const POLL_MS = 5000;
const STORAGE_KEY = "tron_block_dashboard_v1";
const MAX_RECORDS = 2000;

const $ = (id) => document.getElementById(id);

let records = loadRecords();
let timer = null;
let secondsLeft = POLL_MS / 1000;

function loadRecords() {
  try { return JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]"); }
  catch { return []; }
}
function saveRecords() {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(records.slice(0, MAX_RECORDS)));
}
function setStatus(ok, text) {
  $("statusDot").className = "dot " + (ok ? "ok" : "err");
  $("statusText").textContent = text;
}
function formatTime(ms) {
  if (!ms) return "—";
  return new Date(ms).toLocaleString("zh-CN", {hour12:false});
}

/*
  规则：
  1. 取 Hash 中从右往左遇到的前 7 个 A-E 字母。
  2. 取 Hash 中从右往左遇到的前 7 个 0-9 数字。
  3. 按提取顺序一一组合：字母[i] + 数字[i]。
  4. A=0, B=1, C=2, D=3, E=4，将字母转换成数字。
  5. 同号码处理：
     若两个“字母+数字”组合转换后完全相同，则保留后出现的组合，
     并继续向左侧寻找可用的字母/数字组合，直到得到最多 7 个唯一号码。
  注意：截图对“同号码”的描述存在简写/示例化表达，因此这里把“重复号码”按最终两位号码去重。
*/
function extractRightToLeft(hash) {
  const s = String(hash).toUpperCase();
  const letters = [];
  const digits = [];
  for (let i = s.length - 1; i >= 0; i--) {
    const c = s[i];
    if ("ABCDE".includes(c) && letters.length < 7) letters.push(c);
    if (/[0-9]/.test(c) && digits.length < 7) digits.push(c);
    if (letters.length === 7 && digits.length === 7) break;
  }
  return { letters, digits };
}

function calculate(hash) {
  const s = String(hash).toUpperCase();
  const letters = [];
  const digits = [];

  // First collect the first 7 of each category from right to left.
  for (let i = s.length - 1; i >= 0 && (letters.length < 7 || digits.length < 7); i--) {
    const c = s[i];
    if ("ABCDE".includes(c) && letters.length < 7) letters.push(c);
    if (/[0-9]/.test(c) && digits.length < 7) digits.push(c);
  }

  if (letters.length < 7 || digits.length < 7) {
    return { letters, digits, rawPairs: [], finalPairs: [], valid:false,
             note:"该 Hash 无法同时提取 7 个 A-E 字母和 7 个数字，按规则应视为无法开奖。" };
  }

  const rawPairs = letters.map((l, i) => l + digits[i]);
  const finalPairs = rawPairs.map(p => {
    const n = Number("ABCDE".indexOf(p[0])) * 10 + Number(p[1]);
    return String(n).padStart(2, "0");
  });

  // Preserve the displayed 7 positions by applying the duplicate rule
  // to final two-digit results: duplicate values are skipped.
  const seen = new Set();
  const uniquePairs = [];
  for (const p of finalPairs) {
    if (!seen.has(p)) {
      seen.add(p);
      uniquePairs.push(p);
    }
  }

  let note = "";
  if (uniquePairs.length < finalPairs.length) {
    note = `检测到重复号码：原始结果 ${finalPairs.join("、")}；统计区采用去重后的 ${uniquePairs.join("、")}。`;
  } else {
    note = "本期没有检测到重复的最终两位号码。";
  }

  return { letters, digits, rawPairs, finalPairs: uniquePairs, valid:true, note };
}

function renderResult(block) {
  const hash = block.blockID || "";
  const result = calculate(hash);

  $("blockNumber").textContent = block.block_header?.raw_data?.number ?? "—";
  $("blockTime").textContent = formatTime(block.block_header?.raw_data?.timestamp);
  $("blockHash").textContent = hash || "—";
  $("letters").textContent = result.letters.join(" ");
  $("digits").textContent = result.digits.join(" ");
  $("rawPairs").innerHTML = result.rawPairs.map(x => `<span>${x}</span>`).join("") || "—";
  $("finalPairs").innerHTML = result.finalPairs.map(x => `<span>${x}</span>`).join("") || "—";
  $("ruleNote").textContent = result.note;

  if (!hash || !result.valid) return;

  const record = {
    block: Number(block.block_header?.raw_data?.number || 0),
    timestamp: Number(block.block_header?.raw_data?.timestamp || 0),
    hash,
    letters: result.letters,
    digits: result.digits,
    rawPairs: result.rawPairs,
    finalPairs: result.finalPairs
  };

  if (!records.some(r => r.hash === hash)) {
    records.unshift(record);
    records = records.slice(0, MAX_RECORDS);
    saveRecords();
  }
  renderHistory();
  renderStats();
  $("recordCount").textContent = records.length;
}

async function fetchLatestBlock() {
  try {
    const res = await fetch(API, {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: "{}",
      cache: "no-store"
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    if (!data.blockID) throw new Error(data.Error || "未返回 blockID");
    setStatus(true, "实时连接正常");
    renderResult(data);
  } catch (err) {
    setStatus(false, "读取失败");
    $("ruleNote").textContent = `读取 TRON 区块失败：${err.message}。可以稍后重试；若 GitHub Pages 浏览器请求受跨域或频率限制影响，应把 API 调用移到你的后端/Serverless Function。`;
  }
}

function renderHistory() {
  const body = $("historyBody");
  body.innerHTML = records.slice(0, 200).map(r => `
    <tr>
      <td>${r.block}</td>
      <td>${formatTime(r.timestamp)}</td>
      <td class="hash-cell">${r.hash}</td>
      <td>${(r.finalPairs || []).join("、")}</td>
    </tr>
  `).join("") || `<tr><td colspan="4">暂无记录，等待新区块。</td></tr>`;
}

function getSelectedRecords() {
  const v = $("windowSelect").value;
  if (v === "all") return records;
  return records.slice(0, Number(v));
}

function renderStats() {
  const rs = getSelectedRecords();
  const counts = Array(10).fill(0);
  let total = 0;
  for (const r of rs) {
    for (const p of (r.finalPairs || [])) {
      const n = Number(p);
      if (Number.isFinite(n)) {
        // Count the two digits separately.
        String(n).padStart(2,"0").split("").forEach(d => counts[Number(d)]++);
        total += 2;
      }
    }
  }
  const max = Math.max(1, ...counts);
  $("stats").innerHTML = counts.map((c, i) => `
    <div class="stat">
      <div class="stat-top"><span class="stat-num">${i}</span><span class="stat-count">${c}</span></div>
      <div class="bar"><i style="width:${(c/max)*100}%"></i></div>
      <div class="label" style="margin-top:7px">占比 ${total ? ((c/total)*100).toFixed(2) : "0.00"}%</div>
    </div>
  `).join("");
}

function startPolling() {
  clearInterval(timer);
  secondsLeft = POLL_MS / 1000;
  timer = setInterval(async () => {
    secondsLeft--;
    if (secondsLeft <= 0) {
      await fetchLatestBlock();
      secondsLeft = POLL_MS / 1000;
    }
    $("countdown").textContent = `${Math.max(0, secondsLeft)}s`;
  }, 1000);
}

$("refreshBtn").addEventListener("click", async () => {
  $("countdown").textContent = "读取中";
  await fetchLatestBlock();
  secondsLeft = POLL_MS / 1000;
});
$("windowSelect").addEventListener("change", renderStats);
$("clearBtn").addEventListener("click", () => {
  if (confirm("确定清空本机历史记录吗？")) {
    records = [];
    saveRecords();
    renderHistory();
    renderStats();
    $("recordCount").textContent = "0";
  }
});

renderHistory();
renderStats();
$("recordCount").textContent = records.length;
fetchLatestBlock();
startPolling();
