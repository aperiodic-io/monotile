// The site's few behaviours: the menu, the current page, copy buttons, the hero's tape, the
// cookbook filter. No dependencies.
(function () {
  var menu = document.querySelector(".menu");
  var nav = document.getElementById("nav");
  if (menu) {
    menu.addEventListener("click", function () {
      var open = nav.classList.toggle("open");
      menu.setAttribute("aria-expanded", String(open));
    });
  }

  // the current page, in the top bar and the docs rail
  var here = location.pathname.replace(/\/$/, "/index.html");
  document.querySelectorAll(".side a, .nav a").forEach(function (a) {
    var path = new URL(a.getAttribute("href"), location.href).pathname;
    if (path === here) a.setAttribute("aria-current", "page");
  });
  var section = (document.body.className.match(/s-(\S+)/) || [])[1];
  document.querySelectorAll(".nav a[data-s]").forEach(function (a) {
    if (a.dataset.s === section && !document.querySelector(".nav a[aria-current]")) a.setAttribute("aria-current", "page");
  });

  // tables of three or more columns: label each cell with its header, shown when phones stack the rows
  document.querySelectorAll(".tbl table").forEach(function (t) {
    var heads = Array.prototype.map.call(t.querySelectorAll("thead th"), function (th) { return th.textContent.trim(); });
    if (heads.length > 2) t.querySelectorAll("tbody td").forEach(function (td) {
      if (td.cellIndex > 0 && heads[td.cellIndex]) td.dataset.label = heads[td.cellIndex];
    });
  });

  // copy buttons
  function copier(button, text) {
    button.addEventListener("click", function () {
      var done = function () {
        button.textContent = "Copied";
        setTimeout(function () { button.textContent = "Copy"; }, 1400);
      };
      if (navigator.clipboard) {
        navigator.clipboard.writeText(text()).then(done, function () { button.textContent = "Select it"; });
      }
    });
  }
  document.querySelectorAll("pre:not(.out):not(.nocopy)").forEach(function (pre) {
    var b = document.createElement("button");
    b.className = "copy";
    b.type = "button";
    b.textContent = "Copy";
    pre.appendChild(b);
    copier(b, function () {
      var code = pre.querySelector("code") || pre;
      return code.innerText.replace(/^\$ /gm, "").trim();
    });
  });
  document.querySelectorAll(".install").forEach(function (box) {
    var b = box.querySelector(".copy");
    copier(b, function () { return box.querySelector("code").innerText.trim(); });
  });

  // cookbook filter
  var filter = document.getElementById("recipe-filter");
  if (filter) {
    filter.addEventListener("input", function () {
      var q = filter.value.trim().toLowerCase();
      document.querySelectorAll(".recipe").forEach(function (r) {
        r.hidden = q !== "" && r.textContent.toLowerCase().indexOf(q) < 0;
      });
      document.querySelectorAll("main h2[id]").forEach(function (h) {
        var next = h.nextElementSibling, any = false;
        while (next && next.tagName !== "H2") {
          if (next.classList.contains("recipe") && !next.hidden) any = true;
          next = next.nextElementSibling;
        }
        h.hidden = !any;
      });
    });
  }

  // search: the site's pages and sections (search.json, written by build.py), as you type;
  // `/` focuses the box. Without JavaScript the box stays hidden.
  var box = document.querySelector(".search");
  if (box && window.fetch) {
    box.hidden = false;
    var input = document.getElementById("search"), list = document.getElementById("search-results");
    var root = box.dataset.root, index = null, picked = -1;
    var load = function () {
      if (!index) index = fetch(root + "search.json").then(function (r) { return r.json(); }).catch(function () { return []; });
      return index;
    };
    var esc = function (s) { return s.replace(/[&<>"]/g, function (c) { return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]; }); };
    var rx = function (t) { return t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"); };
    var mark = function (s, terms) {
      var re = new RegExp("(" + terms.map(rx).join("|") + ")", "gi");
      return s.split(re).map(function (part, i) { return i % 2 ? "<mark>" + esc(part) + "</mark>" : esc(part); }).join("");
    };
    var snippet = function (text, terms) {
      var at = text.toLowerCase().indexOf(terms[0]);
      return (at > 60 ? "…" : "") + text.slice(Math.max(0, at - 60), Math.max(0, at - 60) + 180);
    };
    var show = function () {
      // as-of and asof alike
      var norm = function (x) { return x.toLowerCase().replace(/-/g, ""); };
      var terms = norm(input.value).split(/\s+/).filter(Boolean);
      if (!terms.length) { list.hidden = true; input.setAttribute("aria-expanded", "false"); return; }
      load().then(function (all) {
        var hits = [];
        all.forEach(function (e) {
          var h = norm(e.h), t = norm(e.t), p = norm(e.p), score = 0;
          for (var i = 0; i < terms.length; i++) {
            var w = terms[i], s = 0;
            if (h.indexOf(w) >= 0) s += new RegExp("\\b" + rx(w)).test(h) ? 8 : 5;
            if (p.indexOf(w) >= 0) s += 2;
            if (t.indexOf(w) >= 0) s += 1;
            if (!s) return;
            score += s;
          }
          hits.push([score - (e.u.indexOf("#") < 0 ? 0.5 : 0), e]);
        });
        hits.sort(function (a, b) { return b[0] - a[0]; });
        picked = hits.length ? 0 : -1;
        list.innerHTML = hits.length
          ? hits.slice(0, 8).map(function (x, i) {
              var e = x[1];
              return '<a role="option" href="' + root + e.u + '"' + (i === 0 ? ' aria-selected="true"' : "") + '><span class="page">' + esc(e.p) + "</span><b>" + mark(e.h, terms) + "</b><small>" + mark(snippet(e.t, terms), terms) + "</small></a>";
            }).join("")
          : '<div class="none">Nothing found. Try the <a href="' + root + 'docs/cookbook.html">cookbook</a>.</div>';
        list.hidden = false;
        input.setAttribute("aria-expanded", "true");
      });
    };
    var move = function (d) {
      var links = list.querySelectorAll("a[role=option]");
      if (!links.length) return;
      picked = (picked + d + links.length) % links.length;
      links.forEach(function (a, i) { a.setAttribute("aria-selected", String(i === picked)); });
      links[picked].scrollIntoView({ block: "nearest" });
    };
    input.addEventListener("focus", load);
    input.addEventListener("input", show);
    input.addEventListener("keydown", function (e) {
      if (e.key === "ArrowDown") { e.preventDefault(); move(1); }
      else if (e.key === "ArrowUp") { e.preventDefault(); move(-1); }
      else if (e.key === "Enter") { var a = list.querySelectorAll("a[role=option]")[picked]; if (a) location.href = a.href; }
      else if (e.key === "Escape") { input.value = ""; list.hidden = true; input.blur(); }
    });
    document.addEventListener("keydown", function (e) {
      var typing = /^(INPUT|TEXTAREA|SELECT)$/.test((document.activeElement || {}).tagName || "");
      if (e.key === "/" && !typing && !e.metaKey && !e.ctrlKey) { e.preventDefault(); input.focus(); }
    });
    document.addEventListener("click", function (e) { if (!box.contains(e.target)) list.hidden = true; });
  }

  // the hero: ticks arrive and one-minute bars form, as `brrrrr serve` would emit them
  var canvas = document.getElementById("tape");
  if (!canvas) return;
  var ctx = canvas.getContext("2d");
  var css = getComputedStyle(document.documentElement);
  var color = function (name) { return css.getPropertyValue(name).trim(); };
  var still = matchMedia("(prefers-reduced-motion: reduce)").matches;
  var TICKS_PER_BAR = 36, MAX_BARS = 34;
  var price = 42017.5, seed = 7, ticks = 0, bars = [], recent = [];
  function rand() { seed = (seed * 16807) % 2147483647; return (seed - 1) / 2147483646; }
  function tick() {
    var drift = (rand() - 0.5) * 7 + (rand() < 0.03 ? (rand() - 0.5) * 40 : 0);
    price = Math.max(41500, Math.min(42600, price + drift));
    ticks++;
    var bar = bars[bars.length - 1];
    if (!bar || bar.n >= TICKS_PER_BAR) {
      bar = { o: price, h: price, l: price, c: price, n: 0 };
      bars.push(bar);
      if (bars.length > MAX_BARS) bars.shift();
    }
    bar.h = Math.max(bar.h, price);
    bar.l = Math.min(bar.l, price);
    bar.c = price;
    bar.n++;
    recent.push(price);
    if (recent.length > TICKS_PER_BAR) recent.shift();
  }
  for (var i = 0; i < TICKS_PER_BAR * 24; i++) tick();

  var out = { ticks: document.getElementById("t-ticks"), bars: document.getElementById("t-bars"), last: document.getElementById("t-last") };
  function draw() {
    var w = canvas.clientWidth, h = canvas.clientHeight, dpr = window.devicePixelRatio || 1;
    if (canvas.width !== Math.round(w * dpr)) { canvas.width = Math.round(w * dpr); canvas.height = Math.round(h * dpr); }
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, w, h);
    var lo = Infinity, hi = -Infinity;
    bars.forEach(function (b) { lo = Math.min(lo, b.l); hi = Math.max(hi, b.h); });
    var pad = (hi - lo) * 0.12 + 1, top = 14, bottom = h - 14;
    var y = function (p) { return bottom - (p - (lo - pad)) / (hi - lo + 2 * pad) * (bottom - top); };
    var tickArea = Math.min(110, w * 0.22), step = (w - tickArea - 16) / MAX_BARS, body = Math.max(2, step * 0.62);
    ctx.strokeStyle = color("--code-rule");
    ctx.lineWidth = 1;
    for (var g = 1; g < 4; g++) {
      var gy = Math.round(top + (bottom - top) * g / 4) + 0.5;
      ctx.beginPath(); ctx.moveTo(0, gy); ctx.lineTo(w, gy); ctx.stroke();
    }
    bars.forEach(function (b, k) {
      var x = 8 + k * step + step / 2, up = b.c >= b.o, forming = k === bars.length - 1;
      ctx.strokeStyle = ctx.fillStyle = up ? color("--up") : color("--down");
      ctx.globalAlpha = forming ? 0.55 : 1;
      ctx.beginPath(); ctx.moveTo(Math.round(x) + 0.5, y(b.h)); ctx.lineTo(Math.round(x) + 0.5, y(b.l)); ctx.stroke();
      var y0 = y(Math.max(b.o, b.c)), y1 = y(Math.min(b.o, b.c));
      ctx.fillRect(x - body / 2, y0, body, Math.max(1.5, y1 - y0));
      ctx.globalAlpha = 1;
    });
    // the ticks of the forming bar, arriving at the right
    var x0 = w - tickArea;
    ctx.fillStyle = color("--tok-k");
    recent.forEach(function (p, k) {
      ctx.globalAlpha = 0.25 + 0.75 * k / recent.length;
      ctx.beginPath(); ctx.arc(x0 + 6 + k * (tickArea - 14) / TICKS_PER_BAR, y(p), 1.8, 0, 7); ctx.fill();
    });
    ctx.globalAlpha = 1;
    ctx.setLineDash([3, 4]);
    ctx.strokeStyle = color("--tok-c");
    ctx.beginPath(); ctx.moveTo(x0 - 0.5, top); ctx.lineTo(x0 - 0.5, bottom); ctx.stroke();
    ctx.setLineDash([]);
    out.ticks.textContent = ticks.toLocaleString("en-US");
    out.bars.textContent = String(Math.floor(ticks / TICKS_PER_BAR));
    out.last.textContent = price.toLocaleString("en-US", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
  }
  draw();
  addEventListener("resize", draw);
  if (still) return;
  var last = 0;
  (function frame(t) {
    if (t - last > 90 && !document.hidden) { tick(); draw(); last = t; }
    requestAnimationFrame(frame);
  })(0);
})();
