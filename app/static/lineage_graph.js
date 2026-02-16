
/* Production-ish lineage graph renderer (no external deps).
   Renders a left-to-right DAG view with zoom/pan + drilldown.
*/
function esc(s){return (s??"").toString().replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;");}

function labelFor(n){
  const id = n.id || "";
  if (id.startsWith("ds:")) return (n.name || id.slice(3));
  if (id.startsWith("col:")) {
    const full = (n.name || id.slice(4));
    const parts = full.split(".");
    return parts[parts.length-1] || full;
  }
  return n.name || id;
}

function kindFor(n){
  const id = n.id || "";
  if (id.startsWith("ds:")) return "dataset";
  if (id.startsWith("col:")) return "column";
  return n.type || "node";
}

function buildMaps(graph){
  const nodes = graph.nodes || [];
  const edges = (graph.edges || []).filter(e => (e.edge_type || "") !== "contains");
  const nodeById = new Map(nodes.map(n => [n.id, n]));
  const out = new Map(); // u -> [v]
  const inn = new Map(); // v -> [u]
  for (const e of edges){
    if (!out.has(e.u)) out.set(e.u, []);
    if (!inn.has(e.v)) inn.set(e.v, []);
    out.get(e.u).push(e.v);
    inn.get(e.v).push(e.u);
  }
  return {nodeById, edges, out, inn};
}

function bfsLevels(centerId, maps, maxDepth){
  const {out, inn} = maps;
  const level = new Map();
  level.set(centerId, 0);

  // upstream (reverse)
  const qU = [{id:centerId, d:0}];
  while(qU.length){
    const {id, d} = qU.shift();
    if (d >= maxDepth) continue;
    const preds = inn.get(id) || [];
    for (const p of preds){
      if (!level.has(p)){
        level.set(p, -(d+1));
        qU.push({id:p, d:d+1});
      }
    }
  }

  // downstream
  const qD = [{id:centerId, d:0}];
  while(qD.length){
    const {id, d} = qD.shift();
    if (d >= maxDepth) continue;
    const succ = out.get(id) || [];
    for (const s of succ){
      if (!level.has(s)){
        level.set(s, (d+1));
        qD.push({id:s, d:d+1});
      }
    }
  }
  return level;
}

function layout(nodes, edges, levelMap){
  // group nodes by level
  const groups = new Map();
  for (const n of nodes){
    const lv = levelMap.get(n.id);
    if (lv === undefined) continue;
    if (!groups.has(lv)) groups.set(lv, []);
    groups.get(lv).push(n);
  }
  const levels = Array.from(groups.keys()).sort((a,b)=>a-b);

  // sort within each level for stability
  for (const lv of levels){
    groups.get(lv).sort((a,b)=>labelFor(a).localeCompare(labelFor(b)));
  }

  // compute positions
  const xStep = 280;
  const yStep = 74;
  const marginX = 40;
  const marginY = 40;
  const boxW = 220;
  const boxH = 44;

  const pos = new Map();
  let maxY = 0;

  for (let i=0;i<levels.length;i++){
    const lv = levels[i];
    const arr = groups.get(lv);
    const x = marginX + (i * xStep);
    for (let j=0;j<arr.length;j++){
      const y = marginY + (j * yStep);
      pos.set(arr[j].id, {x, y, w: boxW, h: boxH, level: lv});
      maxY = Math.max(maxY, y + boxH + marginY);
    }
  }

  const maxX = marginX + (levels.length-1)*xStep + boxW + marginX;
  const width = Math.max(900, maxX);
  const height = Math.max(520, maxY);

  // edge routes
  const routed = [];
  for (const e of edges){
    if (!pos.has(e.u) || !pos.has(e.v)) continue;
    const a = pos.get(e.u), b = pos.get(e.v);
    // only draw forward-ish edges for clarity
    // but allow same/any direction
    const x1 = a.x + a.w;
    const y1 = a.y + a.h/2;
    const x2 = b.x;
    const y2 = b.y + b.h/2;
    const dx = Math.max(80, (x2 - x1) * 0.5);
    routed.push({...e, x1, y1, x2, y2, cx1:x1+dx, cy1:y1, cx2:x2-dx, cy2:y2});
  }

  return {pos, width, height, routed, levels};
}

function makeSVG(tag, attrs){
  const el = document.createElementNS("http://www.w3.org/2000/svg", tag);
  for (const k in attrs){
    el.setAttribute(k, attrs[k]);
  }
  return el;
}

function enablePanZoom(svg, viewport){
  let state = {drag:false, sx:0, sy:0, vb:null};
  function getVB(){
    const vb = viewport.getAttribute("viewBox").split(" ").map(Number);
    return {x:vb[0], y:vb[1], w:vb[2], h:vb[3]};
  }
  function setVB(vb){
    viewport.setAttribute("viewBox", `${vb.x} ${vb.y} ${vb.w} ${vb.h}`);
  }

  svg.addEventListener("wheel", (ev)=>{
    ev.preventDefault();
    const vb = getVB();
    const zoom = ev.deltaY < 0 ? 0.9 : 1.1;
    const mx = ev.offsetX / svg.clientWidth;
    const my = ev.offsetY / svg.clientHeight;
    const nw = vb.w * zoom;
    const nh = vb.h * zoom;
    const nx = vb.x + (vb.w - nw) * mx;
    const ny = vb.y + (vb.h - nh) * my;
    setVB({x:nx,y:ny,w:nw,h:nh});
  }, {passive:false});

  svg.addEventListener("mousedown",(ev)=>{
    state.drag=true;
    state.sx=ev.clientX; state.sy=ev.clientY;
    state.vb=getVB();
  });
  window.addEventListener("mousemove",(ev)=>{
    if(!state.drag) return;
    const dx = (ev.clientX - state.sx) * (state.vb.w / svg.clientWidth);
    const dy = (ev.clientY - state.sy) * (state.vb.h / svg.clientHeight);
    setVB({x:state.vb.x - dx, y:state.vb.y - dy, w:state.vb.w, h:state.vb.h});
  });
  window.addEventListener("mouseup",()=>{state.drag=false;});
}

async function apiGet(url){
  const res = await fetch(url);
  return await res.json();
}

function setPanel(html){
  const p = document.getElementById("panel");
  if (p) p.innerHTML = html;
}

function pill(text){
  return `<span class="pill">${esc(text)}</span>`;
}

function renderPanelDataset(dsName, meta){
  const cols = (meta.columns || []).slice(0, 200);
  const colLis = cols.map(c => `<li><button class="linkbtn" data-col="${esc(c)}">${esc(c.split(".").slice(-1)[0])}</button><span class="muted">${esc(c)}</span></li>`).join("");
  setPanel(`
    <div class="panel-title">Dataset</div>
    <div class="panel-main">${pill("TABLE")} <b>${esc(dsName)}</b></div>
    <div class="hint">Click a column to open column lineage.</div>
    <div class="panel-title mt">Columns</div>
    <ul class="col-list">${colLis || `<li class="muted">No columns captured in this run.</li>`}</ul>
  `);

  // attach handlers
  const p = document.getElementById("panel");
  p.querySelectorAll("button[data-col]").forEach(btn=>{
    btn.addEventListener("click", ()=>{
      const full = btn.getAttribute("data-col");
      window.location.href = `/lineage/column?column=${encodeURIComponent(full)}`;
    });
  });
}

function renderPanelColumn(colName){
  setPanel(`
    <div class="panel-title">Column</div>
    <div class="panel-main">${pill("COL")} <b>${esc(colName)}</b></div>
    <div class="hint">Use the graph to traverse upstream/downstream mappings. Click edges for evidence.</div>
  `);
}

function renderPanelEdge(e){
  const conf = (e.confidence ?? 0).toFixed(2);
  const ev = (e.evidence || "").toString();
  setPanel(`
    <div class="panel-title">Evidence</div>
    <div class="panel-main">${pill("EDGE")} <b>${esc(e.u)}</b> → <b>${esc(e.v)}</b></div>
    <div class="hint">Confidence: <b>${esc(conf)}</b></div>
    <pre class="evidence">${esc(ev) || "No evidence captured."}</pre>
  `);
}

function drawGraph(container, graph, centerId, depth){
  const maps = buildMaps(graph);
  const levelMap = bfsLevels(centerId, maps, depth);

  // take nodes/edges in subgraph
  const nodes = (graph.nodes || []).filter(n => levelMap.has(n.id));
  const edges = maps.edges.filter(e => levelMap.has(e.u) && levelMap.has(e.v));

  const {pos, width, height, routed} = layout(nodes, edges, levelMap);

  // clear
  container.innerHTML = "";

  const svg = makeSVG("svg", {class:"svg", width:"100%", height:"100%"});
  const viewport = makeSVG("svg", {viewBox:`0 0 ${width} ${height}`}); // nested for viewBox only
  svg.appendChild(viewport);

  // background grid subtle
  const bg = makeSVG("rect", {x:0,y:0,width:width,height:height,fill:"transparent"});
  viewport.appendChild(bg);

  // edges first
  for (const e of routed){
    const path = makeSVG("path", {
      d:`M ${e.x1} ${e.y1} C ${e.cx1} ${e.cy1}, ${e.cx2} ${e.cy2}, ${e.x2} ${e.y2}`,
      class:"edge",
      "data-edge": JSON.stringify({u:e.u,v:e.v,confidence:e.confidence,evidence:e.evidence||""})
    });
    viewport.appendChild(path);

    // arrow head
    const ah = makeSVG("path", {
      d:`M ${e.x2} ${e.y2} l -8 -5 l 0 10 z`,
      class:"arrow"
    });
    viewport.appendChild(ah);
  }

  // nodes
  for (const n of nodes){
    const p = pos.get(n.id);
    const g = makeSVG("g", {class:"node", "data-id": n.id});
    const cls = kindFor(n);
    const rect = makeSVG("rect", {x:p.x, y:p.y, rx:12, ry:12, width:p.w, height:p.h, class:`nodebox ${cls} ${n.id===centerId?"center":""}`});
    const text = makeSVG("text", {x:p.x+12, y:p.y+27, class:"nodetext"});
    text.textContent = labelFor(n);
    g.appendChild(rect);
    g.appendChild(text);

    const sub = makeSVG("text", {x:p.x+12, y:p.y+42, class:"nodesub"});
    const id = (n.id||"").startsWith("ds:") ? n.id.slice(3) : ((n.id||"").startsWith("col:") ? n.id.slice(4) : n.id);
    sub.textContent = id.length>34 ? id.slice(0,34)+"…" : id;
    g.appendChild(sub);

    viewport.appendChild(g);
  }

  // click handlers
  viewport.addEventListener("click", (ev)=>{
    const el = ev.target;
    if (el.classList && (el.classList.contains("edge") || el.classList.contains("arrow"))){
      const data = el.getAttribute("data-edge");
      if (data){
        try{ renderPanelEdge(JSON.parse(data)); }catch(e){}
      }
      return;
    }
    let n = el;
    while(n && n !== viewport && !(n.classList && n.classList.contains("node"))) n = n.parentNode;
    if (n && n.getAttribute){
      const id = n.getAttribute("data-id");
      if (id){
        window.dispatchEvent(new CustomEvent("lineageNodeClick", {detail:{id}}));
      }
    }
  });

  container.appendChild(svg);
  enablePanZoom(svg, viewport);

  // fit to center area
  // leave as default viewBox; user can zoom.
}

async function renderDatasetPage(datasetName){
  const depthSel = document.getElementById("depth");
  const modeSel = document.getElementById("mode");
  const graphEl = document.getElementById("graph");

  let current = datasetName;

  async function loadAndRender(){
    const depth = parseInt(depthSel.value||"2",10);
    const mode = modeSel.value||"both";
    const view = await apiGet(`/api/dataset/${encodeURIComponent(current)}?depth=${depth}`);
    if (view.not_found){
      setPanel(`<div class="muted">Dataset not found: <b>${esc(current)}</b></div>`);
      return;
    }

    // Build combined graph for rendering depending on mode
    const combined = {nodes:[], edges:[]};
    const add = (g)=>{
      if(!g) return;
      combined.nodes.push(...(g.nodes||[]));
      combined.edges.push(...(g.edges||[]));
    };
    if (mode==="up") add(view.up);
    else if (mode==="down") add(view.down);
    else { add(view.up); add(view.down); }

    // de-dupe
    const nm = new Map();
    for (const n of combined.nodes) nm.set(n.id, n);
    const em = new Map();
    for (const e of combined.edges) em.set(`${e.u}|${e.v}|${e.edge_type}`, e);
    combined.nodes = Array.from(nm.values());
    combined.edges = Array.from(em.values());

    const centerId = `ds:${current}`;
    drawGraph(graphEl, combined, centerId, depth);

    // columns listing: use downstream contains edges from view.down
    const cols = (view.down?.nodes||[])
      .filter(n => (n.id||"").startsWith(`col:${current}.`))
      .map(n => n.id.replace("col:",""));
    renderPanelDataset(current, {columns: cols});
  }

  depthSel.addEventListener("change", loadAndRender);
  modeSel.addEventListener("change", loadAndRender);

  window.addEventListener("lineageNodeClick", async (ev)=>{
    const id = ev.detail.id;
    if (id.startsWith("ds:")){
      current = id.slice(3);
      await loadAndRender();
      // update URL (optional)
      history.replaceState(null, "", `/lineage/table/${encodeURIComponent(current)}`);
    } else if (id.startsWith("col:")){
      const col = id.slice(4);
      window.location.href = `/lineage/column?column=${encodeURIComponent(col)}`;
    }
  });

  await loadAndRender();
}

async function renderColumnPage(columnFull){
  const depthSel = document.getElementById("depth");
  const modeSel = document.getElementById("mode");
  const graphEl = document.getElementById("graph");

  let current = columnFull;

  async function loadAndRender(){
    const depth = parseInt(depthSel.value||"2",10);
    const mode = modeSel.value||"both";
    const view = await apiGet(`/api/column?column=${encodeURIComponent(current)}&depth=${depth}`);
    if (view.not_found){
      setPanel(`<div class="muted">Column not found: <b>${esc(current)}</b></div>`);
      return;
    }
    const combined = {nodes:[], edges:[]};
    const add = (g)=>{ if(!g) return; combined.nodes.push(...(g.nodes||[])); combined.edges.push(...(g.edges||[])); };
    if (mode==="up") add(view.up);
    else if (mode==="down") add(view.down);
    else { add(view.up); add(view.down); }

    const nm = new Map();
    for (const n of combined.nodes) nm.set(n.id, n);
    const em = new Map();
    for (const e of combined.edges) em.set(`${e.u}|${e.v}|${e.edge_type}`, e);
    combined.nodes = Array.from(nm.values());
    combined.edges = Array.from(em.values());

    const centerId = `col:${current}`;
    drawGraph(graphEl, combined, centerId, depth);
    renderPanelColumn(current);
  }

  depthSel.addEventListener("change", loadAndRender);
  modeSel.addEventListener("change", loadAndRender);

  window.addEventListener("lineageNodeClick", async (ev)=>{
    const id = ev.detail.id;
    if (id.startsWith("ds:")){
      window.location.href = `/lineage/table/${encodeURIComponent(id.slice(3))}`;
    } else if (id.startsWith("col:")){
      current = id.slice(4);
      history.replaceState(null, "", `/lineage/column?column=${encodeURIComponent(current)}`);
      await loadAndRender();
    }
  });

  await loadAndRender();
}
