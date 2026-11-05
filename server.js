#!/usr/bin/env node
/**
 * 草图转 UI · ZCode 版 —— 本地服务
 * 零依赖，Node >= 18。用法: node server.js
 * 功能: 托管编辑器页面 / 保存草图产物(sketch.png|json + REQUIREMENTS.md)
 *       / 以无头模式调用 ZCode 引擎生成 UI 代码 / SSE 流式回传运行日志
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const os = require('os');

const APP_ROOT = __dirname;
const PUBLIC_DIR = path.join(APP_ROOT, 'public');
const PROJECTS_DIR = path.join(APP_ROOT, 'projects');
const HOST = '0.0.0.0';
const PORT_START = 17580;
const JOB_TIMEOUT_MS = 40 * 60 * 1000; // 单次生成最长 40 分钟
const JOB_TTL_MS = 60 * 60 * 1000;     // 任务结束后保留 60 分钟供修正续聊

// ---------- ZCode 引擎定位 ----------
function findEngine() {
  const candidates = [
    process.env.ZCODE_ENGINE, // 用户可自行覆盖
    path.join(os.homedir(), '.zcode', 'cli', 'resources', 'glm', 'zcode.cjs'),
    path.join(os.homedir(), 'AppData', 'Local', 'Programs', 'ZCode', 'resources', 'glm', 'zcode.cjs'),
    'C:\\Program Files\\ZCode\\resources\\glm\\zcode.cjs',
  ].filter(Boolean);
  for (const p of candidates) {
    try { if (fs.statSync(p).isFile()) return p; } catch { /* 忽略 */ }
  }
  return null;
}
const ENGINE = findEngine();

// 读取无头引擎实际会用的模型：个人供应商规则表中第一个 enabled 供应商的 modelOrder[0]
// （实测：defaultModelSelection 不参与无头路由；业务错误也不回退到下一供应商）
function currentModel() {
  const candidates = [
    process.env.ZCODE_PERSONAL_PROVIDER_CONFIG_FILE,
    path.join(os.homedir(), '.zcode', 'v2', 'provider_config.json'),
  ].filter(Boolean);
  for (const p of candidates) {
    try {
      const cfg = JSON.parse(fs.readFileSync(p, 'utf8'));
      const rules = cfg.config && cfg.config.providerConfigRules && cfg.config.providerConfigRules.providerRules;
      if (Array.isArray(rules)) {
        const first = rules.find((r) => r.enabled !== false);
        if (first) {
          const model = (first.config && (first.config.modelOrder || first.config.personalModelIds) || [])[0] || '?';
          const named = { 'glm-free-e0e19036': 'GLM免费档', 'glm-paid-e0e19036': 'GLM付费档' }[first.providerId] || first.providerName || first.providerId;
          return `${named} / ${model}`;
        }
      }
    } catch { /* 继续尝试下一个路径 */ }
  }
  return '(未找到供应商规则)';
}

// ---------- 任务管理 ----------
const jobs = new Map(); // id -> {proc, events:[], clients:Set, done, startedAt, timer}
let jobSeq = 0;

function jobPush(job, evt) {
  job.events.push(evt);
  if (evt.t !== 'tick') job.lastOutputAt = Date.now(); // 心跳不算输出
  const line = `data: ${JSON.stringify(evt)}\n\n`;
  for (const res of job.clients) res.write(line);
}

function sseLine(res, evt) { res.write(`data: ${JSON.stringify(evt)}\n\n`); }

// 解析 zcode --json 的 stdout：逐行尝试 JSON，提取助手文本/工具调用/最终结果
function handleStdoutChunk(job, buf) {
  job._buf = (job._buf || '') + buf.toString('utf8');
  let idx;
  while ((idx = job._buf.indexOf('\n')) >= 0) {
    const line = job._buf.slice(0, idx).trim();
    job._buf = job._buf.slice(idx + 1);
    if (!line) continue;
    let obj = null;
    try { obj = JSON.parse(line); } catch { obj = null; }
    if (obj) {
      emitZcodeEvent(job, obj);
    } else if (line.length < 4000) {
      jobPush(job, { t: 'out', text: line });
    }
  }
}

function emitZcodeEvent(job, obj) {
  // 引擎最终结果行：{sessionId, response, usage, projection, ...}
  if (obj.sessionId && obj.response !== undefined) {
    job.sessionId = obj.sessionId;
    jobPush(job, { t: 'result', text: obj.response, sessionId: obj.sessionId,
      turns: obj.usage && obj.usage.modelRequestCount,
      tokens: obj.usage && `${obj.usage.inputTokens}in/${obj.usage.outputTokens}out` });
    return;
  }
  // 兼容 Claude-Code 风格 stream-json 与普通结果对象
  if (obj.type === 'assistant' && obj.message && Array.isArray(obj.message.content)) {
    for (const c of obj.message.content) {
      if (c.type === 'text' && c.text) jobPush(job, { t: 'ai', text: c.text });
      if (c.type === 'tool_use') {
        jobPush(job, { t: 'tool', name: c.name, input: summarizeInput(c.input) });
      }
    }
    return;
  }
  if (obj.type === 'result') {
    jobPush(job, { t: 'result', subtype: obj.subtype, cost: obj.total_cost_usd,
      turns: obj.num_turns, text: (typeof obj.result === 'string' ? obj.result : '') });
    return;
  }
  if (obj.type === 'system' && obj.subtype === 'init') {
    jobPush(job, { t: 'sys', text: `会话已建立 model=${obj.model || '?'}` });
    return;
  }
  // 其他结构：压缩为一条
  const s = JSON.stringify(obj);
  if (s.length < 2000) jobPush(job, { t: 'out', text: s });
}

function summarizeInput(input) {
  if (!input || typeof input !== 'object') return '';
  const keys = ['file_path', 'path', 'command', 'description', 'pattern', 'url'];
  const parts = [];
  for (const k of keys) if (input[k]) parts.push(String(input[k]).slice(0, 120));
  return parts.join(' | ');
}

function handleStderrChunk(job, buf) {
  const text = buf.toString('utf8');
  for (const line of text.split(/\r?\n/)) {
    const l = line.trim();
    if (!l) continue;
    // 过滤过噪的进度行，保留 warn/error 与关键阶段
    if (/event=(bootstrap|log\.retention|mcp\.server\.connect\.started)/.test(l)) continue;
    if (l.length > 500) jobPush(job, { t: 'run', text: l.slice(0, 500) + '…' });
    else jobPush(job, { t: 'run', text: l });
  }
}

function spawnEnv() {
  const env = { ...process.env };
  // 独立启动（双击 bat）时没有桌面端注入的引擎环境，这里补齐：
  // 1) 内置供应商配置：用工具目录里的快照
  if (!env.ZCODE_BUILTIN_PROVIDER_CONFIG_FILE) {
    const snap = path.join(APP_ROOT, 'zcode-builtin.snapshot.json');
    if (fs.existsSync(snap)) env.ZCODE_BUILTIN_PROVIDER_CONFIG_FILE = snap;
  }
  // 2) 个人供应商配置：优先沿用已有指向；否则探测桌面端重定向的数据目录，最后回退默认家目录
  if (!env.ZCODE_PERSONAL_PROVIDER_CONFIG_FILE) {
    const candidates = [
      path.join(os.homedir(), '.zcode', 'v2', 'provider_config.json'),
    ];
    // 桌面端数据目录重定向（ZCODE_DATA_BASE_DIR 或自定义数据目录，若本机存在则优先）
    for (const base of [process.env.ZCODE_DATA_BASE_DIR, os.homedir()].filter(Boolean)) {
      const p = path.join(base, '.zcode', 'v2', 'provider_config.json');
      if (fs.existsSync(p)) { env.ZCODE_PERSONAL_PROVIDER_CONFIG_FILE = p; break; }
    }
    for (const p of candidates) {
      if (!env.ZCODE_PERSONAL_PROVIDER_CONFIG_FILE && fs.existsSync(p)) {
        env.ZCODE_PERSONAL_PROVIDER_CONFIG_FILE = p; break;
      }
    }
  }
  return env;
}

function startJob(targetDir, prompt, opts = {}) {
  const id = `job_${Date.now()}_${++jobSeq}`;
  // 纵深防御：即使上游漏检，也不允许形如选项的 resume 值进入 argv
  const resumeId = opts.resumeSessionId && SESSION_ID_RE.test(opts.resumeSessionId) ? opts.resumeSessionId : null;
  const job = { id, events: [], clients: new Set(), done: false, startedAt: Date.now(),
    targetDir, sessionId: resumeId, shot: opts.shot || null };
  jobs.set(id, job);

  // 动态值统一用 `--opt=value` 形式，值不会被拆成独立 argv 项（CWE-88）
  const args = [ENGINE, '--json', '--no-color'];
  if (resumeId) args.push(`--resume=${resumeId}`);
  if (opts.attach && opts.attach.length) {
    for (const a of opts.attach) args.push(`--attach=${a}`);
  }
  args.push(`--prompt=${prompt}`);
  const proc = spawn(process.execPath, args, {
    cwd: targetDir,
    env: spawnEnv(),
    stdio: ['ignore', 'pipe', 'pipe'],
    windowsHide: true,
  });
  job.proc = proc;
  jobPush(job, { t: 'start', pid: proc.pid, cwd: targetDir });

  proc.stdout.on('data', (d) => handleStdoutChunk(job, d));
  proc.stderr.on('data', (d) => handleStderrChunk(job, d));
  proc.on('error', (e) => {
    jobPush(job, { t: 'error', text: `进程启动失败: ${e.message}` });
    finishJob(job, -1);
  });
  proc.on('close', async (code) => {
    if (code === 0 && job.shot) {
      try {
        await takePageScreenshot(path.join(job.targetDir, 'index.html'), path.join(job.targetDir, 'generated-preview.png'), job.shot.w, job.shot.h);
        jobPush(job, { t: 'shot', file: 'generated-preview.png' });
      } catch (e) {
        jobPush(job, { t: 'run', text: `自动截图失败: ${e.message}（不影响生成结果）` });
      }
    }
    finishJob(job, code);
  });

  job.timer = setTimeout(() => {
    jobPush(job, { t: 'error', text: '超时（40 分钟），已终止。' });
    try { proc.kill(); } catch { /* 忽略 */ }
  }, JOB_TIMEOUT_MS);
  // 静默心跳：引擎长时间无输出时告知前端"仍在运行"，避免看起来像卡死
  job.lastOutputAt = Date.now();
  job.tick = setInterval(() => {
    if (job.done) return clearInterval(job.tick);
    jobPush(job, { t: 'tick', quiet: Math.round((Date.now() - job.lastOutputAt) / 1000),
      secs: Math.round((Date.now() - job.startedAt) / 1000) });
  }, 15000);
  return id;
}

function finishJob(job, code) {
  if (job.done) return;
  job.done = true;
  clearTimeout(job.timer);
  clearInterval(job.tick);
  const secs = Math.round((Date.now() - job.startedAt) / 1000);
  jobPush(job, { t: 'exit', code, secs, sessionId: job.sessionId || null, targetDir: job.targetDir });
  for (const res of job.clients) res.end();
  job.clients.clear();
  setTimeout(() => { // 事件保留一段时间供修正续聊
    jobs.delete(job.id);
  }, JOB_TTL_MS);
}

// ---------- 无头 Chrome 截图 ----------
function findChrome() {
  const candidates = [
    process.env.CHROME_PATH,
    'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
    'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
    path.join(os.homedir(), 'AppData', 'Local', 'Google', 'Chrome', 'Application', 'chrome.exe'),
    'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',
    'C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe',
  ].filter(Boolean);
  for (const p of candidates) {
    try { if (fs.statSync(p).isFile()) return p; } catch { /* 忽略 */ }
  }
  return null;
}

function takePageScreenshot(htmlPath, outPath, w, h) {
  const browser = findChrome();
  if (!browser) return Promise.reject(new Error('未找到 Chrome/Edge'));
  const url = 'file:///' + htmlPath.replace(/\\/g, '/');
  const tmpProfile = path.join(os.tmpdir(), `ui-sketcher-shot-${Date.now()}`);
  return new Promise((resolve, reject) => {
    const proc = spawn(browser, [
      '--headless=new', '--disable-gpu', '--hide-scrollbars', '--no-first-run', '--no-default-browser-check',
      `--user-data-dir=${tmpProfile}`,
      `--screenshot=${outPath}`, `--window-size=${w},${h}`, '--default-background-color=FFFFFFFF', url,
    ], { stdio: 'ignore', windowsHide: true });
    const timer = setTimeout(() => { try { proc.kill(); } catch { /* 忽略 */ } reject(new Error('截图超时')); }, 45000);
    proc.on('close', () => {
      clearTimeout(timer);
      fs.rmSync(tmpProfile, { recursive: true, force: true });
      setTimeout(() => { // 给磁盘写入留一点时间
        fs.existsSync(outPath) ? resolve(outPath) : reject(new Error('截图文件未生成'));
      }, 300);
    });
    proc.on('error', (e) => { clearTimeout(timer); reject(e); });
  });
}

// ---------- 产物写入 ----------
function sanitizeName(name) {
  const n = (name || '').trim().replace(/[\\/:*?"<>|]/g, '_').slice(0, 60);
  return n || '未命名界面';
}

// ---------- 输出目录边界 ----------
// 允许写入/读取的根：默认 projects/，可用环境变量 UI_SKETCHER_OUTPUT_ROOTS 追加（分号分隔）。
// 服务绑定 0.0.0.0，局域网可达，故对动态路径做严格边界校验。
function allowedRoots() {
  const extra = (process.env.UI_SKETCHER_OUTPUT_ROOTS || '')
    .split(';').map(s => s.trim()).filter(Boolean).map(s => path.resolve(s));
  return [PROJECTS_DIR, ...extra];
}
function withinRoot(dir, root) {
  return dir === root || dir.startsWith(root + path.sep);
}
function isAllowedDir(dir) {
  return allowedRoots().some((r) => withinRoot(dir, r));
}
function sanitizeTargetDir(input) {
  const dir = path.resolve(input && input.trim() ? input.trim() : '');
  return isAllowedDir(dir) ? dir : null;
}

const SESSION_ID_RE = /^sess_[A-Za-z0-9._-]{1,128}$/;

function resolveTargetDir(input) {
  return path.resolve(input && input.trim() ? input.trim() : '');
}
void resolveTargetDir; // 已由 sanitizeTargetDir 取代（含边界校验），保留仅为兼容外部引用

function buildRequirementsMd(sketch) {
  const frameName = { phone: '手机竖屏', tablet: '平板', desktop: '桌面/网页' }[sketch.frameType] || sketch.frameType;
  const L = [];
  L.push(`# UI 生成需求：${sketch.projectName}`);
  L.push('');
  const fw = sketch.frame.width || sketch.frame.w;
  const fh = sketch.frame.height || sketch.frame.h;
  L.push(`- 画布类型：${frameName}（${fw} × ${fh} px）`);
  L.push('');
  L.push('## 一、全局要求（用户填写）');
  L.push('');
  L.push(sketch.globalRequirements && sketch.globalRequirements.trim()
    ? sketch.globalRequirements.trim() : '（用户未补充全局要求）');
  L.push('');
  L.push('## 二、草图元素清单');
  L.push('');
  L.push('| 编号 | 控件 | 位置 x,y | 尺寸 w×h | 文字/占位 | 用途与要求（用户标注） |');
  L.push('|---|---|---|---|---|---|');
  for (const e of sketch.elements) {
    const notes = (e.notes || '').replace(/\|/g, '／').replace(/\n/g, ' ');
    const label = (e.label || '').replace(/\|/g, '／').replace(/\n/g, ' ');
    L.push(`| ${e.id} | ${e.typeName} | ${Math.round(e.x)},${Math.round(e.y)} | ${Math.round(e.w)}×${Math.round(e.h)} | ${label} | ${notes || '—'} |`);
  }
  L.push('');
  L.push('## 三、全局样式参数（用户设置，生成时必须遵守）');
  L.push('');
  const st = sketch.style || {};
  const CORNER_TEXT = { sharp:'直角（圆角 0-2px）', round:'圆角（6-10px）', large:'大圆角（12-20px）', pill:'胶囊/全圆角' };
  const DASH_TEXT = { solid:'实线', dashed:'虚线', dotted:'点线' };
  L.push(`- 线条粗细：${st.strokeWidth || 2.5}px（边框、描边、分割线等线类元素）`);
  L.push(`- 线型：${DASH_TEXT[st.dash] || '实线'}`);
  L.push(`- 描边颜色：${st.stroke || '#3d4a5d'}`);
  L.push(`- 控件填充色：${st.fill || '#ffffff'}`);
  L.push(`- 主色调：${st.primary || '#4f6ef7'}（按钮、强调、选中态等）`);
  L.push(`- 形状风格：${CORNER_TEXT[st.corner] || '圆角（6-10px）'}`);
  if (st.extra) L.push(`- 附加样式要求：${st.extra}`);
  L.push('');
  L.push('## 四、给 ZCode 的任务说明');
  L.push('');
  L.push('请阅读同目录下的 `sketch.json`（元素精确坐标/类型/标注，结构化真相来源）与 `sketch.png`（视觉参考草图），');
  L.push('再结合上文的全局要求、全局样式参数与元素清单，实现该界面的 UI 代码。');
  L.push('');
  L.push(`- 技术栈：${sketch.stack}`);
  L.push('- 严格保持草图中的元素类型、相对位置与层级关系；标注文字渲染为控件文字。');
  L.push('- 元素含 `strokes` 字段的为用户手绘笔画：结合其位置、形状与上下文理解意图（框选区域、箭头指向、示意图形等），在实现中转为对应 UI 结构或装饰元素；笔画按全局样式参数的粗细/线型/颜色呈现。');
  L.push('- 元素含 `group` 字段的为用户手动组合的逻辑组（同组元素共享同一 group 值）：实现时保持其关联关系（位置相邻、样式一致、可包入同一容器）。');
  L.push('- **第三节的全局样式参数必须逐项落实**：所有线类元素用指定粗细与线型，颜色使用指定色值（视觉微调可接受但色相一致），控件圆角按形状风格统一。');
  L.push('- 元素标注（用途与要求）必须逐条落实：交互行为、状态、跳转、校验等以标注为准。');
  L.push('- 视觉风格在草图基础上做专业的现代化美化（配色、阴影、留白、字体层级），但不得违背全局样式参数。');
  L.push('- 全局要求与技术栈冲突时，以全局要求为准并在代码注释中说明取舍。');
  return L.join('\n');
}

function writeArtifacts(targetDir, body) {
  fs.mkdirSync(targetDir, { recursive: true });
  const sketch = body.sketch;
  if (body.png && /^data:image\/png;base64,/.test(body.png)) {
    fs.writeFileSync(path.join(targetDir, 'sketch.png'),
      Buffer.from(body.png.slice('data:image/png;base64,'.length), 'base64'));
  }
  fs.writeFileSync(path.join(targetDir, 'sketch.json'), JSON.stringify(sketch, null, 2));
  fs.writeFileSync(path.join(targetDir, 'REQUIREMENTS.md'), buildRequirementsMd(sketch));
  return ['sketch.png', 'sketch.json', 'REQUIREMENTS.md'].filter((f) =>
    fs.existsSync(path.join(targetDir, f)));
}

function buildPrompt(sketch) {
  return [
    '请阅读当前目录下的 REQUIREMENTS.md、sketch.json 与 sketch.png。',
    '这是一个 UI 草图：sketch.json 是元素坐标/类型/标注的结构化来源，sketch.png 是视觉参考，',
    'REQUIREMENTS.md 含全局要求、元素清单与任务说明。',
    `请按这些要求实现该界面的 UI 代码，技术栈：${sketch.stack}。`,
    '将代码文件直接写入当前目录，并在最后简要说明生成了哪些文件。',
  ].join('');
}

// ---------- HTTP ----------
function sendJson(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': 'null' });
  res.end(body);
}

function readBody(req, limit = 30 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    const chunks = []; let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > limit) { reject(new Error('body too large')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${HOST}`);
  try {
    if (req.method === 'OPTIONS') {
      res.writeHead(204, {
        'Access-Control-Allow-Origin': 'null',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
      });
      return res.end();
    }

    // 页面与静态资源
    if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/index.html')) {
      const html = fs.readFileSync(path.join(PUBLIC_DIR, 'index.html'));
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      return res.end(html);
    }

    // 环境自检
    if (req.method === 'GET' && url.pathname === '/api/config') {
      return sendJson(res, 200, {
        engine: ENGINE, engineFound: !!ENGINE, model: currentModel(),
        projectsDir: PROJECTS_DIR, appRoot: APP_ROOT, node: process.version,
      });
    }

    // 读取 .uisketch 草图文件（AI 生成后自动打开的回环：编辑器 ?open= 直载）
    if (req.method === 'GET' && url.pathname === '/api/open-uisketch') {
      const p = url.searchParams.get('file') || '';
      if (!/\.uisketch$/i.test(p)) return sendJson(res, 400, { error: '仅支持 .uisketch 文件' });
      let abs;
      try { abs = path.resolve(p); } catch (e) { return sendJson(res, 400, { error: '路径无效' }); }
      if (!fs.existsSync(abs)) return sendJson(res, 404, { error: '文件不存在：' + abs });
      try {
        const doc = JSON.parse(fs.readFileSync(abs, 'utf8'));
        if (!doc || doc._format && doc._format.type !== 'uisketch') return sendJson(res, 400, { error: '不是 uisketch 内容' });
        return sendJson(res, 200, { ok: true, doc, file: abs });
      } catch (e) { return sendJson(res, 400, { error: 'JSON 解析失败：' + e.message }); }
    }

    // 开始生成（写产物 + 调 zcode）
    if (req.method === 'POST' && url.pathname === '/api/generate') {
      if (!ENGINE) return sendJson(res, 500, { error: '未找到 ZCode 引擎(zcode.cjs)，可用环境变量 ZCODE_ENGINE 指定' });
      const body = JSON.parse(await readBody(req));
      const targetDir = sanitizeTargetDir(body.targetDir || path.join(PROJECTS_DIR, sanitizeName(body.sketch && body.sketch.projectName)));
      if (!targetDir) return sendJson(res, 400, { error: '输出目录越界：仅允许 projects/ 目录或 UI_SKETCHER_OUTPUT_ROOTS 声明的根之下' });
      const files = writeArtifacts(targetDir, body);
      const shot = body.canvas && body.canvas.w ? { w: body.canvas.w, h: body.canvas.h } : null;
      const jobId = startJob(targetDir, buildPrompt(body.sketch), { shot });
      return sendJson(res, 200, { jobId, targetDir, files });
    }

    // 修正续聊：基于上次会话（--resume）继续修 UI，可选附草图与生成页截图自动比对
    if (req.method === 'POST' && url.pathname === '/api/refine') {
      if (!ENGINE) return sendJson(res, 500, { error: '未找到 ZCode 引擎' });
      const body = JSON.parse(await readBody(req));
      const targetDir = sanitizeTargetDir(body.targetDir);
      if (!targetDir) return sendJson(res, 400, { error: '输出目录越界：仅允许 projects/ 目录或 UI_SKETCHER_OUTPUT_ROOTS 声明的根之下' });
      if (!fs.existsSync(path.join(targetDir, 'index.html')) &&
          !fs.existsSync(path.join(targetDir, 'App.vue')) &&
          !fs.existsSync(path.join(targetDir, 'App.jsx'))) {
        return sendJson(res, 400, { error: '目标目录没有可修正的生成产物' });
      }
      const parts = [];
      if (body.compare) {
        parts.push('附图1是原始UI草图（sketch.png），附图2是当前生成页面的实际截图（generated-preview.png）。');
        parts.push('请仔细对比两张图：修正生成代码中与草图不符的布局、缺失或多余的元素、明显样式偏差，保持已实现的交互逻辑。');
      }
      if (body.feedback && body.feedback.trim()) {
        parts.push(`用户对当前界面的反馈如下，请落实到代码：\n${body.feedback.trim()}`);
      }
      if (!parts.length) return sendJson(res, 400, { error: '没有可执行的内容：请填写反馈或勾选自动比对' });
      parts.push('修改完成后简要列出改动点。');
      const attach = [];
      if (body.compare) {
        for (const f of ['sketch.png', 'generated-preview.png']) {
          const p = path.join(targetDir, f);
          if (fs.existsSync(p)) attach.push(p);
        }
        if (attach.length < 2) return sendJson(res, 400, { error: '自动比对需要 sketch.png 与 generated-preview.png 同时存在' });
      }
      const jobId = startJob(targetDir, parts.join('\n'), {
        resumeSessionId: SESSION_ID_RE.test(String(body.sessionId || '')) ? body.sessionId : null,
        attach,
        shot: body.canvas && body.canvas.w ? { w: body.canvas.w, h: body.canvas.h } : null,
      });
      return sendJson(res, 200, { jobId, targetDir });
    }

    // 图片导入解析：视觉模型把界面图/手绘照片解析为画布元素 JSON
    if (req.method === 'POST' && url.pathname === '/api/import-image') {
      if (!ENGINE) return sendJson(res, 500, { error: '未找到 ZCode 引擎' });
      const body = JSON.parse(await readBody(req));
      if (!/^data:image\/(png|jpeg|jpg|webp);base64,/.test(body.dataUrl || '')) {
        return sendJson(res, 400, { error: '仅支持 png/jpg/webp 图片' });
      }
      const ext = body.dataUrl.startsWith('data:image/png') ? 'png' : (body.dataUrl.startsWith('data:image/webp') ? 'webp' : 'jpg');
      const imgPath = path.join(PROJECTS_DIR, `import-${Date.now()}.${ext}`);
      fs.mkdirSync(PROJECTS_DIR, { recursive: true });
      fs.writeFileSync(imgPath, Buffer.from(body.dataUrl.slice(body.dataUrl.indexOf(',') + 1), 'base64'));
      const typesList = 'navbar顶部导航,tabbar底部标签栏,container区块容器,card卡片,divider分割线,heading标题,text文本,image图片,icon图标,listitem列表项,button按钮,input输入框,textarea多行输入,select下拉框,checkbox复选框,switch开关,badge徽章,tabs标签页,table表格,chart图表';
      const prompt = [
        '附图是一张界面设计图或手绘草图。请分析图中的 UI 元素，只输出一个 JSON 对象（不要任何解释、不要 markdown 代码围栏），格式：',
        '{"width":图片宽,"height":图片高,"elements":[{"type":"类型","x":数字,"y":数字,"w":数字,"h":数字,"label":"元素文字"}]}',
        `坐标为图片像素坐标系(左上角原点)。type 只能从这些里选：${typesList}。`,
        '要求：元素齐全、位置尺寸尽量贴近图中真实比例；没有文字的元素 label 留空字符串；忽略纯装饰。只输出 JSON。',
      ].join('\n');
      const parsed = await new Promise((resolve) => {
        let out = '', done = false;
        // 所有动态值都用 `--opt=value` 形式，值不会拆成独立 argv 项，杜绝以 `-` 开头的选项注入
        const proc = spawn(process.execPath, [ENGINE, '--json', '--no-color', `--attach=${imgPath}`, `--prompt=${prompt}`], {
          cwd: PROJECTS_DIR, env: spawnEnv(), stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true,
        });
        const timer = setTimeout(() => { if (!done) { done = true; try { proc.kill(); } catch { /* 忽略 */ } resolve({ error: '解析超时' }); } }, 5 * 60 * 1000);
        proc.stdout.on('data', (d) => { out += d.toString('utf8'); });
        proc.stderr.on('data', () => { /* 忽略进度 */ });
        proc.on('close', () => {
          if (done) return;
          done = true; clearTimeout(timer);
          const extract = (raw) => {
            // 1) 整段 stdout 就是一个 JSON 对象（多行美化输出是常态）
            try { const o = JSON.parse(raw); if (o && o.response !== undefined) return o.response; } catch { /* 继续 */ }
            // 2) 从每个行首 '{' 起尝试解析子块（stdout 混有多条输出时）
            const lines = raw.split('\n');
            for (let i = 0; i < lines.length; i++) {
              if (lines[i].trim() !== '{') continue;
              for (let j = lines.length; j > i + 1; j--) {
                try { const o = JSON.parse(lines.slice(i, j).join('\n')); if (o && o.response !== undefined) return o.response; } catch { /* 继续 */ }
              }
            }
            return null;
          };
          try {
            const respText = extract(out);
            if (respText == null) return resolve({ error: '未能从模型输出中找到结果' });
            let txt = String(respText).trim().replace(/^```(json)?/i, '').replace(/```$/, '').trim();
            const a = txt.indexOf('{'), b = txt.lastIndexOf('}');
            if (a < 0 || b <= a) return resolve({ error: '模型输出中没有 JSON 对象' });
            resolve(JSON.parse(txt.slice(a, b + 1)));
          } catch (e) { resolve({ error: '解析失败: ' + e.message }); }
        });
        proc.on('error', (e) => { if (!done) { done = true; clearTimeout(timer); resolve({ error: e.message }); } });
      });
      try { fs.unlinkSync(imgPath); } catch { /* 忽略 */ }
      if (parsed.error) return sendJson(res, 500, { error: parsed.error });
      const ALLOWED = new Set(typesList.split(',').map(s => s.split(/[\u4e00-\u9fa5]+/)[0]));
      const elements = (Array.isArray(parsed.elements) ? parsed.elements : [])
        .filter(e => e && Number.isFinite(+e.x) && Number.isFinite(+e.y) && +e.w > 0 && +e.h > 0)
        .map(e => ({ type: ALLOWED.has(String(e.type)) ? String(e.type) : 'text',
          x: Math.round(+e.x), y: Math.round(+e.y), w: Math.round(+e.w), h: Math.round(+e.h),
          label: String(e.label || '') }));
      return sendJson(res, 200, { width: +parsed.width || body.width || 0, height: +parsed.height || body.height || 0, elements });
    }

    // 导出 .uisketch 单文件到输出目录
    if (req.method === 'POST' && url.pathname === '/api/export-uisketch') {
      const body = JSON.parse(await readBody(req));
      const targetDir = sanitizeTargetDir(body.targetDir || path.join(PROJECTS_DIR, sanitizeName(body.projectName)));
      if (!targetDir) return sendJson(res, 400, { error: '输出目录越界' });
      if (!body.content || typeof body.content !== 'string' || body.content.length > 20 * 1024 * 1024) {
        return sendJson(res, 400, { error: 'content 缺失或超大' });
      }
      let doc; try { doc = JSON.parse(body.content); } catch { return sendJson(res, 400, { error: 'content 不是合法 JSON' }); }
      if (!doc || doc._format && doc._format.type !== 'uisketch') return sendJson(res, 400, { error: '不是 uisketch 内容' });
      fs.mkdirSync(targetDir, { recursive: true });
      const file = path.join(targetDir, `${sanitizeName(body.projectName)}.uisketch`);
      fs.writeFileSync(file, body.content);
      return sendJson(res, 200, { path: file });
    }

    // 预览生成产物（iframe 同源可用）
    if (req.method === 'GET' && url.pathname === '/preview') {
      const dir = path.resolve(url.searchParams.get('dir') || '');
      const file = path.basename(url.searchParams.get('file') || 'index.html');
      const full = path.join(dir, file);
      if (!isAllowedDir(dir)) { res.writeHead(403); return res.end('forbidden'); }
      if (!fs.existsSync(full) || !fs.statSync(full).isFile()) { res.writeHead(404); return res.end('not found'); }
      const mime = { '.html': 'text/html; charset=utf-8', '.css': 'text/css', '.js': 'text/javascript',
        '.png': 'image/png', '.jpg': 'image/jpeg', '.svg': 'image/svg+xml', '.json': 'application/json' }[path.extname(full).toLowerCase()] || 'application/octet-stream';
      res.writeHead(200, { 'Content-Type': mime, 'Access-Control-Allow-Origin': 'null' });
      return res.end(fs.readFileSync(full));
    }

    // 仅导出草图产物，不调用 zcode
    if (req.method === 'POST' && url.pathname === '/api/export-only') {
      const body = JSON.parse(await readBody(req));
      const targetDir = sanitizeTargetDir(body.targetDir || path.join(PROJECTS_DIR, sanitizeName(body.sketch && body.sketch.projectName)));
      if (!targetDir) return sendJson(res, 400, { error: '输出目录越界：仅允许 projects/ 目录或 UI_SKETCHER_OUTPUT_ROOTS 声明的根之下' });
      const files = writeArtifacts(targetDir, body);
      return sendJson(res, 200, { targetDir, files });
    }

    // SSE 事件流
    if (req.method === 'GET' && url.pathname === '/api/events') {
      const job = jobs.get(url.searchParams.get('job'));
      if (!job) { res.writeHead(404); return res.end('no such job'); }
      res.writeHead(200, {
        'Content-Type': 'text/event-stream; charset=utf-8',
        'Cache-Control': 'no-cache', Connection: 'keep-alive',
        'Access-Control-Allow-Origin': 'null',
      });
      for (const evt of job.events) sseLine(res, evt);
      if (job.done) return res.end();
      job.clients.add(res);
      req.on('close', () => job.clients.delete(res));
      return;
    }

    // 停止任务
    if (req.method === 'POST' && url.pathname === '/api/stop') {
      const { jobId } = JSON.parse(await readBody(req));
      const job = jobs.get(jobId);
      if (job && job.proc) { try { job.proc.kill(); } catch { /* 忽略 */ } }
      return sendJson(res, 200, { ok: !!job });
    }

    // 用资源管理器打开输出目录
    if (req.method === 'POST' && url.pathname === '/api/open-folder') {
      const { targetDir } = JSON.parse(await readBody(req));
      if (process.platform === 'win32') spawn('explorer.exe', [targetDir], { detached: true, stdio: 'ignore' });
      return sendJson(res, 200, { ok: true });
    }

    res.writeHead(404); return res.end('not found');
  } catch (e) {
    return sendJson(res, 500, { error: String(e.message || e) });
  }
});

// ---------- 启动 ----------
function localUrls(port) {
  const urls = [];
  const nets = os.networkInterfaces();
  for (const list of Object.values(nets)) {
    for (const n of list || []) {
      if (n.family === 'IPv4' && !n.internal) urls.push(`http://${n.address}:${port}`);
    }
  }
  return urls;
}

function listen(port) {
  server.once('error', (e) => {
    if (e.code === 'EADDRINUSE' && port < PORT_START + 10) listen(port + 1);
    else { console.error('端口监听失败:', e.message); process.exit(1); }
  });
  server.listen(port, HOST, () => {
    const urls = [`http://localhost:${port}`, ...localUrls(port)];
    console.log('==============================================');
    console.log('  草图转 UI · ZCode 版  已启动');
    console.log('  编辑器地址(任选其一):');
    for (const u of urls) console.log(`    ${u}`);
    console.log(`  ZCode 引擎: ${ENGINE || '未找到! (设置环境变量 ZCODE_ENGINE)'}`);
    console.log(`  当前模型  : ${currentModel()}`);
    console.log(`  默认输出  : ${PROJECTS_DIR}`);
    console.log('==============================================');
    if (process.platform === 'win32' && !process.env.NO_AUTOPEN) {
      spawn('cmd.exe', ['/c', 'start', '', urls[urls.length - 1]], { detached: true, stdio: 'ignore' }).unref();
    }
  });
}
listen(PORT_START);
