
(function(){
  function clamp(x,a,b){return Math.max(a,Math.min(b,x));}
  function createGraph(el, options){
    const cfg = Object.assign({
      onNodeClick: ()=>{},
      onEdgeClick: ()=>{},
      onBgClick: ()=>{},
      colors: {
        dataset: "#7c3aed",
        file: "#22c55e",
        column: "#94a3b8",
        edge: "rgba(124,58,237,0.55)",
        edge2: "rgba(34,197,94,0.45)",
        text: "#e6e8ee",
      }
    }, options||{});

    el.innerHTML = "";
    const svg = document.createElementNS("http://www.w3.org/2000/svg","svg");
    svg.setAttribute("width","100%");
    svg.setAttribute("height","100%");
    el.appendChild(svg);

    const defs = document.createElementNS("http://www.w3.org/2000/svg","defs");
    defs.innerHTML = `
      <filter id="glow" x="-30%" y="-30%" width="160%" height="160%">
        <feGaussianBlur stdDeviation="3" result="coloredBlur"/>
        <feMerge>
          <feMergeNode in="coloredBlur"/>
          <feMergeNode in="SourceGraphic"/>
        </feMerge>
      </filter>`;
    svg.appendChild(defs);

    const g = document.createElementNS("http://www.w3.org/2000/svg","g");
    svg.appendChild(g);

    let scale = 1, tx = 0, ty = 0;
    function applyTransform(){ g.setAttribute("transform", `translate(${tx},${ty}) scale(${scale})`); }
    applyTransform();

    const edgeLayer = document.createElementNS("http://www.w3.org/2000/svg","g");
    const nodeLayer = document.createElementNS("http://www.w3.org/2000/svg","g");
    const labelLayer = document.createElementNS("http://www.w3.org/2000/svg","g");
    g.appendChild(edgeLayer); g.appendChild(nodeLayer); g.appendChild(labelLayer);

    let nodes = [];
    let edges = [];
    let nodeById = new Map();
    let anim = null;

    function nodeColor(n){
      if(n.type === "dataset") return (n.kind === "file") ? cfg.colors.file : cfg.colors.dataset;
      return cfg.colors.column;
    }
    function shortLabel(n){
      if(n.id.startsWith("ds:")){
        const name = n.name || n.id.slice(3);
        if(name.startsWith("ORACLE.")) return name;
        const s = name.replace("s3://","");
        const parts = s.split("/");
        return parts.slice(-2).join("/");
      }
      if(n.id.startsWith("col:")) return (n.column || (n.name||"").split(".").slice(-1)[0] || n.id);
      return n.name || n.id;
    }

    let selectedEdgeId = null;
    let selectedNodeId = null;

    function render(){
      edgeLayer.innerHTML = "";
      for(const e of edges){
        const a = nodeById.get(e.u), b = nodeById.get(e.v);
        if(!a || !b) continue;
        const mx = (a.x + b.x)/2;
        const my = (a.y + b.y)/2 - 40;
        const isContains = e.edge_type === "contains";
        const path = document.createElementNS("http://www.w3.org/2000/svg","path");
        path.setAttribute("d", `M ${a.x} ${a.y} Q ${mx} ${my} ${b.x} ${b.y}`);
        path.setAttribute("stroke", isContains ? "rgba(148,163,184,0.22)" : (e.edge_type==="dataset" ? cfg.colors.edge : cfg.colors.edge2));
        path.setAttribute("stroke-width", isContains ? "1" : String(1.2 + 2.5*clamp(e.confidence||0.5,0,1)));
        path.setAttribute("fill","none");
        if(`${e.u}__${e.v}__${e.edge_type}` === selectedEdgeId) path.setAttribute("filter","url(#glow)");
        if(!isContains){
          path.style.cursor = "pointer";
          path.addEventListener("click",(ev)=>{
            ev.stopPropagation();
            selectedEdgeId = `${e.u}__${e.v}__${e.edge_type}`;
            selectedNodeId = null;
            cfg.onEdgeClick(e);
            render();
          });
        }
        edgeLayer.appendChild(path);

        if(!isContains){
          const ax = b.x, ay = b.y;
          const ang = Math.atan2(b.y - my, b.x - mx);
          const len = 8;
          const p1x = ax - len*Math.cos(ang) + 4*Math.cos(ang+Math.PI/2);
          const p1y = ay - len*Math.sin(ang) + 4*Math.sin(ang+Math.PI/2);
          const p2x = ax - len*Math.cos(ang) - 4*Math.cos(ang+Math.PI/2);
          const p2y = ay - len*Math.sin(ang) - 4*Math.sin(ang+Math.PI/2);
          const tri = document.createElementNS("http://www.w3.org/2000/svg","path");
          tri.setAttribute("d", `M ${ax} ${ay} L ${p1x} ${p1y} L ${p2x} ${p2y} Z`);
          tri.setAttribute("fill", e.edge_type==="dataset" ? "rgba(124,58,237,0.65)" : "rgba(34,197,94,0.55)");
          edgeLayer.appendChild(tri);
        }
      }

      nodeLayer.innerHTML = "";
      labelLayer.innerHTML = "";
      for(const n of nodes){
        const w = (n.type==="dataset") ? 200 : 110;
        const h = (n.type==="dataset") ? 38 : 28;
        const rect = document.createElementNS("http://www.w3.org/2000/svg","rect");
        rect.setAttribute("x", n.x - w/2);
        rect.setAttribute("y", n.y - h/2);
        rect.setAttribute("rx", 12);
        rect.setAttribute("ry", 12);
        rect.setAttribute("fill", "rgba(255,255,255,0.03)");
        rect.setAttribute("stroke", nodeColor(n));
        rect.setAttribute("stroke-width", (n.id===selectedNodeId) ? "2.4" : "1.3");
        if(n.id===selectedNodeId) rect.setAttribute("filter","url(#glow)");
        rect.style.cursor = "pointer";

        let dragging=false, sx=0, sy=0, nx=0, ny=0;
        rect.addEventListener("pointerdown",(ev)=>{
          ev.stopPropagation();
          dragging=true;
          rect.setPointerCapture(ev.pointerId);
          n.fixed=true;
          sx=ev.clientX; sy=ev.clientY; nx=n.x; ny=n.y;
        });
        rect.addEventListener("pointermove",(ev)=>{
          if(!dragging) return;
          const dx=(ev.clientX-sx)/scale, dy=(ev.clientY-sy)/scale;
          n.x=nx+dx; n.y=ny+dy;
          render();
        });
        rect.addEventListener("pointerup",()=>{dragging=false; n.fixed=false;});
        rect.addEventListener("click",(ev)=>{
          ev.stopPropagation();
          selectedNodeId=n.id; selectedEdgeId=null;
          cfg.onNodeClick(n);
          render();
        });

        nodeLayer.appendChild(rect);

        const t = document.createElementNS("http://www.w3.org/2000/svg","text");
        t.setAttribute("x", n.x - (w/2) + 12);
        t.setAttribute("y", n.y + 5);
        t.setAttribute("fill", cfg.colors.text);
        t.setAttribute("font-size", n.type==="dataset" ? "12" : "11");
        t.textContent = shortLabel(n);
        labelLayer.appendChild(t);
      }
    }

    function tick(){
      const rep=2400, spring=0.016, rest=200, damp=0.86;
      for(const n of nodes){ n.vx*=damp; n.vy*=damp; }
      for(let i=0;i<nodes.length;i++){
        for(let j=i+1;j<nodes.length;j++){
          const a=nodes[i], b=nodes[j];
          const dx=b.x-a.x, dy=b.y-a.y;
          const d2=dx*dx+dy*dy+0.01;
          const f=rep/d2;
          a.vx -= f*dx; a.vy -= f*dy;
          b.vx += f*dx; b.vy += f*dy;
        }
      }
      for(const e of edges){
        const a=nodeById.get(e.u), b=nodeById.get(e.v);
        if(!a||!b) continue;
        const dx=b.x-a.x, dy=b.y-a.y;
        const dist=Math.sqrt(dx*dx+dy*dy)+0.01;
        const diff=dist-rest;
        const f=spring*diff;
        const fx=f*(dx/dist), fy=f*(dy/dist);
        a.vx += fx; a.vy += fy;
        b.vx -= fx; b.vy -= fy;
      }
      for(const n of nodes){
        if(n.fixed) continue;
        n.x += n.vx;
        n.y += n.vy;
      }
      render();
      anim = requestAnimationFrame(tick);
    }
    function startSim(){
      stopSim();
      anim = requestAnimationFrame(tick);
    }
    function stopSim(){
      if(anim) cancelAnimationFrame(anim);
      anim=null;
    }

    function setData(data){
      nodes=(data.nodes||[]).map((n,i)=>Object.assign({
        x:(i%6)*240+100,
        y:Math.floor(i/6)*140+100,
        vx:0, vy:0, fixed:false
      }, n));
      edges=(data.edges||[]).map(e=>Object.assign({confidence:0.5}, e));
      nodeById=new Map(nodes.map(n=>[n.id,n]));
      selectedEdgeId=null; selectedNodeId=null;
      render();
    }

    // Pan + zoom
    let panning=false, psx=0, psy=0, ptx=0, pty=0;
    svg.addEventListener("pointerdown",(ev)=>{
      if(ev.target === svg){
        panning=true; psx=ev.clientX; psy=ev.clientY; ptx=tx; pty=ty;
        svg.setPointerCapture(ev.pointerId);
      }
    });
    svg.addEventListener("pointermove",(ev)=>{
      if(!panning) return;
      tx = ptx + (ev.clientX-psx);
      ty = pty + (ev.clientY-psy);
      applyTransform();
    });
    svg.addEventListener("pointerup",()=>{panning=false;});
    svg.addEventListener("click",(ev)=>{
      if(ev.target === svg){
        selectedEdgeId=null; selectedNodeId=null;
        cfg.onBgClick();
        render();
      }
    });
    svg.addEventListener("wheel",(ev)=>{
      ev.preventDefault();
      const factor = (ev.deltaY>0) ? 0.92 : 1.08;
      scale = clamp(scale*factor, 0.45, 2.2);
      applyTransform();
    }, {passive:false});

    return { setData, startSim, stopSim, center: ()=>{tx=0;ty=0;scale=1;applyTransform();} };
  }
  window.LineageGraph = { createGraph };
})();
