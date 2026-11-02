// core.js — قواعد اليوزرات + محرك طابور آمن لأي مزوّد فحص مسموح به.
// لا يحتوي على أي عنوان/طلب لديسكورد. المزوّد (provider) يُمرَّر من الخارج.
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else root.UsernameCore = factory();
})(typeof self !== "undefined" ? self : this, function () {
  const STATUS = Object.freeze({
    AVAILABLE: "AVAILABLE", TAKEN: "TAKEN", UNKNOWN: "UNKNOWN",
    RATE_LIMITED: "RATE_LIMITED", TEMPORARY_ERROR: "TEMPORARY_ERROR", INVALID_USERNAME: "INVALID_USERNAME"
  });
  const MAX_NAMES = 2000;
  const CACHE_TTL_MS = 24 * 3600 * 1000;

  const normalize = s => String(s == null ? "" : s).trim().replace(/^@/, "").toLowerCase();

  // قواعد الصيغة المعروفة حاليًا (قد تتغير من ديسكورد): 2-32، a-z 0-9 _ .، بدون نقطتين متتاليتين
  function validateUsername(u) {
    if (u.length < 2 || u.length > 32) return "الطول لازم يكون بين 2 و 32";
    if (!/^[a-z0-9_.]+$/.test(u)) return "مسموح فقط a-z و 0-9 و _ و .";
    if (u.includes("..")) return "ممنوع نقطتين ورا بعض";
    return null;
  }

  function parseList(text, max) {
    max = max || MAX_NAMES;
    const seen = new Set(), names = []; let duplicates = 0, overflow = 0;
    for (const line of String(text).split(/\r?\n/)) {
      const u = normalize(line);
      if (!u) continue;
      if (seen.has(u)) { duplicates++; continue; }
      seen.add(u);
      if (names.length >= max) { overflow++; continue; }
      names.push(u);
    }
    return { names, duplicates, overflow };
  }

  const isFresh = (rec, now, ttl) => !!rec && (now == null ? Date.now() : now) - rec.at < (ttl == null ? CACHE_TTL_MS : ttl);

  // المزوّد يرجع { status:number, taken?:boolean, retryAfter?:seconds }
  // taken لا يوضع إلا عند رد مؤكد وموثوق. أي شيء غير ذلك لا يُعتبر دليلًا على الاستخدام.
  function classify(res) {
    const status = res && res.status;
    if (status === 200 && typeof res.taken === "boolean") return { status: res.taken ? STATUS.TAKEN : STATUS.AVAILABLE };
    if (status === 429) {
      const sec = Number(res.retryAfter);
      return { status: STATUS.RATE_LIMITED, retryAfterMs: (sec > 0 ? sec : 30) * 1000 };
    }
    if (status === 401 || status === 403) return { status: STATUS.UNKNOWN, fatal: true, reason: "HTTP " + status };
    if (status >= 500) return { status: STATUS.TEMPORARY_ERROR, reason: "HTTP " + status };
    return { status: STATUS.UNKNOWN, reason: "HTTP " + status };
  }

  const defaultSleep = (ms, signal) => new Promise(res => {
    const t = setTimeout(res, ms);
    if (signal) signal.addEventListener("abort", () => { clearTimeout(t); res(); }, { once: true });
  });

  class CheckQueue {
    constructor(o) {
      this.provider = o.provider;
      this.concurrency = o.concurrency || 1;
      this.minIntervalMs = o.minIntervalMs == null ? 1000 : o.minIntervalMs;
      this.maxAttempts = o.maxAttempts || 4;
      this.maxAutoWaitMs = o.maxAutoWaitMs == null ? 120000 : o.maxAutoWaitMs;
      this.max429 = o.max429 || 3;
      this.now = o.now || Date.now;
      this.sleep = o.sleep || defaultSleep;
      this.random = o.random || Math.random;
      this.onEvent = o.onEvent || (() => {});
      this.cache = o.cache || new Map();
      this.ttl = o.ttl == null ? CACHE_TTL_MS : o.ttl;
      this.queue = []; this.queued = new Set(); this.inflight = new Set(); this.results = new Map();
      this.state = "idle"; this.nextAt = 0; this.blockedUntil = 0; this.resumeAt = 0;
      this._chain = Promise.resolve(); this.ac = null;
    }
    add(names) {
      for (const raw of names) {
        const u = normalize(raw);
        if (!u || this.queued.has(u) || this.inflight.has(u)) continue;
        const bad = validateUsername(u);
        if (bad) { this.results.set(u, { status: STATUS.INVALID_USERNAME, reason: bad, at: this.now() }); continue; }
        const c = this.cache.get(u);
        if (isFresh(c, this.now(), this.ttl)) { this.results.set(u, c); continue; }
        this.queue.push(u); this.queued.add(u);
      }
    }
    async start() {
      if (this.state === "running" || this.state === "halted") return;
      this.state = "running"; this.ac = new AbortController();
      await Promise.all(Array.from({ length: this.concurrency }, () => this._worker()));
      if (this.state === "running") this.state = this.queue.length ? "idle" : "done";
    }
    pause() { if (this.state === "running") { this.state = "paused"; this.ac.abort(); } }
    cancel() { this.state = "cancelled"; this.queue = []; this.queued.clear(); if (this.ac) this.ac.abort(); }
    resume() {
      if (this.state === "limited" && this.now() < this.resumeAt) return false;
      if (this.state === "halted") return false;
      if (this.state === "limited" || this.state === "paused") this.state = "idle";
      this.start(); return true;
    }
    _gate() {
      const p = this._chain.then(async () => {
        const wait = Math.max(this.nextAt, this.blockedUntil) - this.now();
        if (wait > 0) await this.sleep(wait, this.ac.signal);
        this.nextAt = this.now() + this.minIntervalMs;
      });
      this._chain = p.catch(() => {}); return p;
    }
    _requeue(u) { if (this.state !== "cancelled") { this.queue.unshift(u); this.queued.add(u); } }
    _done(u, c) {
      const rec = { status: c.status, reason: c.reason, at: this.now() };
      this.results.set(u, rec);
      if (c.status === STATUS.AVAILABLE || c.status === STATUS.TAKEN) this.cache.set(u, rec);
      this.onEvent({ type: "result", name: u, ...rec });
    }
    async _worker() {
      while (this.state === "running") {
        const u = this.queue.shift();
        if (!u) return;
        this.queued.delete(u); this.inflight.add(u);
        try { await this._process(u); } finally { this.inflight.delete(u); }
      }
    }
    async _process(u) {
      let attempts = 0, limited = 0;
      while (true) {
        if (this.state !== "running") return this._requeue(u);
        await this._gate();
        if (this.state !== "running") return this._requeue(u);
        let c;
        try { c = classify(await this.provider(u, { signal: this.ac.signal })); }
        catch (e) {
          if (this.state !== "running") return this._requeue(u);
          c = { status: STATUS.TEMPORARY_ERROR, reason: String((e && e.message) || e) };
        }
        if (c.status === STATUS.AVAILABLE || c.status === STATUS.TAKEN) return this._done(u, c);
        if (c.fatal) { this._done(u, c); this.state = "halted"; this.ac.abort(); this.onEvent({ type: "halted", reason: c.reason }); return; }
        if (c.status === STATUS.RATE_LIMITED) {
          limited++;
          this.blockedUntil = Math.max(this.blockedUntil, this.now() + c.retryAfterMs);
          this.onEvent({ type: "rate_limited", retryAfterMs: c.retryAfterMs, until: this.blockedUntil });
          if (c.retryAfterMs > this.maxAutoWaitMs || limited >= this.max429) {
            this._requeue(u); this.state = "limited"; this.resumeAt = this.blockedUntil; this.ac.abort();
            this.onEvent({ type: "limited", resumeAt: this.resumeAt }); return;
          }
          continue;
        }
        if (c.status === STATUS.TEMPORARY_ERROR) {
          if (++attempts >= this.maxAttempts) return this._done(u, c);
          const delay = Math.floor(this.random() * Math.min(30000, 1000 * Math.pow(2, attempts))); // Exponential backoff + full jitter
          this.onEvent({ type: "retry", name: u, attempts, delay });
          await this.sleep(delay, this.ac.signal);
          continue;
        }
        return this._done(u, c); // UNKNOWN: بدون إعادة محاولة
      }
    }
  }

  return { STATUS, MAX_NAMES, CACHE_TTL_MS, normalize, validateUsername, parseList, isFresh, classify, CheckQueue };
});
