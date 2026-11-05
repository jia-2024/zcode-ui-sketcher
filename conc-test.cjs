#!/usr/bin/env node
/**
 * 并发无头会话测试台
 * 用法: node conc-test.mjs <轮次名> [并发数=4]
 * 每个会话: 独立目录 + 独立任务，收集 exit/耗时/响应/token
 */
'use strict';
const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

const ENGINE = process.env.ZCODE_ENGINE || 'I:\\allai\\ZCode\\resources\\glm\\zcode.cjs';
const ROOT = __dirname;
const ROUND = process.argv[2] || 'round';
const N = parseInt(process.argv[3] || '4', 10);
const PER_TIMEOUT = 8 * 60 * 1000;

const TASKS = [
  { name: 'reply',  prompt: '不使用任何工具，只回复四个字：会话正常' },
  { name: 'reply2', prompt: '不使用任何工具，只回复四个字：链路畅通' },
  { name: 'write',  prompt: '在当前目录创建 hello.html，内容是一个包含标题"并发测试"和一段中文说明的完整 HTML 页面，写完后只回复：已写入' },
  { name: 'write2', prompt: '在当前目录创建 card.html，内容是一个带圆角卡片样式的完整 HTML 页面，写完后只回复：已写入' },
];

function runOne(idx, dir) {
  return new Promise((resolve) => {
    const task = TASKS[idx % TASKS.length];
    fs.mkdirSync(dir, { recursive: true });
    const t0 = Date.now();
    const proc = spawn(process.execPath, [ENGINE, '-p', task.prompt, '--json', '--no-color'], {
      cwd: dir, env: process.env, stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true,
    });
    let out = '', err = '';
    const timer = setTimeout(() => { try { proc.kill(); } catch {} }, PER_TIMEOUT);
    proc.stdout.on('data', (d) => { out += d; });
    proc.stderr.on('data', (d) => { err += d; });
    proc.on('close', (code) => {
      clearTimeout(timer);
      const secs = Math.round((Date.now() - t0) / 1000);
      let resp = null, usage = null, sessionId = null;
      try {
        const j = JSON.parse(out);
        resp = j.response; usage = j.usage; sessionId = j.sessionId;
      } catch { resp = out.slice(0, 200) || null; }
      const four29 = /1305|访问量过大/.test(err);
      resolve({ idx, task: task.name, dir, code, secs, sessionId, resp,
        tokens: usage ? `${usage.inputTokens}in/${usage.outputTokens}out` : null,
        rateLimited: four29, errHead: code !== 0 ? err.slice(-300) : undefined });
    });
  });
}

(async () => {
  const dirs = [];
  for (let i = 0; i < N; i++) dirs.push(path.join(ROOT, `conc-${ROUND}-${i + 1}`));
  const t0 = Date.now();
  const results = await Promise.all(dirs.map((d, i) => runOne(i, d)));
  console.log(`\n===== 并发轮 ${ROUND}: ${N} 会话, 总耗时 ${Math.round((Date.now() - t0) / 1000)}s =====`);
  for (const r of results) {
    const ok = r.code === 0 && r.resp;
    console.log(`[#${r.idx + 1}] ${ok ? '✓' : '✗'} ${r.task} | exit=${r.code} | ${r.secs}s | resp=${JSON.stringify(r.resp).slice(0, 60)} | token=${r.tokens || '-'} | 429=${r.rateLimited}${r.errHead ? ' | err=' + r.errHead.replace(/\n/g, ' ').slice(0, 160) : ''}`);
  }
  const okCount = results.filter((r) => r.code === 0 && r.resp).length;
  console.log(`结论: ${okCount}/${N} 会话正常完成`);
  process.exit(0);
})();
