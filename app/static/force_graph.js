
(function () {
  const COLORS = {
    edge: "rgba(148,163,184,0.45)",
    edgeHi: "rgba(167,139,250,0.95)",
    edgeDim: "rgba(148,163,184,0.18)",
    text: "rgba(255,255,255,0.90)",
    subtext: "rgba(255,255,255,0.62)",
    dsFill: "rgba(124,58,237,0.16)",
    dsStroke: "rgba(167,139,250,0.80)",
    colFill: "rgba(59,130,246,0.12)",
    colStroke: "rgba(125,211,252,0.72)",
    laneStroke: "rgba(255,255,255,0.10)",
    laneFill: "rgba(255,255,255,0.035)",
    laneFillAlt: "rgba(255,255,255,0.02)",
  };
  function clamp(v,a,b){return Math.max(a,Math.min(b,v));}
  function isDataset(id){return (id||"").startsWith("ds:");}
  function isColumn(id){return (id||"").startsWith("col:");}
  function shortLabel(node){
    if(!node) return "";
    if(node.type==="dataset"){
      const n=node.name||node.id||"";
      if(n.startsWith("s3://")){
        const parts=n.replace("s3://","").split("/").filter(Boolean);
        return "s3://"+parts.slice(-2).join("/");
      }
      const parts=n.split(".").filter(Boolean);
      return parts.slice(-2).join(".");
    }
    if(node.type==="column") return node.column || (node.name||"").split(".").slice(-1)[0];
    return node.name||node.id||"";
  }

  // Swimlane classifier (customizable)
  function laneForDatasetName(name){
    const n=(name||"").toUpperCase();
    if(n.startsWith("S3://")){
      if(n.includes("/RAW/")) return "S3 RAW";
      if(n.includes("/CONFORM") || n.includes("/CONFORMED/")) return "S3 CONFORMED";
      return "S3";
    }
    if(n.includes("ODS")) return "ORACLE ODS";
    if(n.includes("STG") || n.includes("STAGING")) return "ORACLE STAGING";
    if(n.includes("MART")) return "ORACLE MART";
    if(n.includes("ORACLE")) return "ORACLE";
    return "OTHER";
  }

  function createSVG(el){
    const svg=document.createElementNS("http://www.w3.org/2000/svg","svg");
    svg.setAttribute("width","100%"); svg.setAttribute("height","100%");
    svg.style.display="block"; svg.style.background="transparent";

    const defs=document.createElementNS(svg.namespaceURI,"defs");
    function mkMarker(id, fill){
      const m=document.createElementNS(svg.namespaceURI,"marker");
      m.setAttribute("id",id); m.setAttribute("viewBox","0 0 10 10");
      m.setAttribute("refX","9"); m.setAttribute("refY","5");
      m.setAttribute("markerWidth","7"); m.setAttribute("markerHeight","7");
      m.setAttribute("orient","auto-start-reverse");
      const p=document.createElementNS(svg.namespaceURI,"path");
      p.setAttribute("d","M 0 0 L 10 5 L 0 10 z"); p.setAttribute("fill",fill);
      m.appendChild(p); return m;
    }
    defs.appendChild(mkMarker("arrow", COLORS.edge));
    defs.appendChild(mkMarker("arrowHi", COLORS.edgeHi));
    svg.appendChild(defs);

    const viewport=document.createElementNS(svg.namespaceURI,"g");
    viewport.setAttribute("id","viewport");
    svg.appendChild(viewport);

    el.innerHTML="";
    el.appendChild(svg);
    return {svg,viewport};
  }
  function pointerPos(svg,evt){
    const pt=svg.createSVGPoint(); pt.x=evt.clientX; pt.y=evt.clientY;
    const ctm=svg.getScreenCTM(); if(!ctm) return {x:0,y:0};
    const p=pt.matrixTransform(ctm.inverse()); return {x:p.x,y:p.y};
  }
  function setTransform(vp,tx,ty,s){vp.setAttribute("transform",`translate(${tx},${ty}) scale(${s})`);}

  function buildAdj(edges, dir){
    const m=new Map();
    for(const e of edges){
      const k=e[dir==="out"?"u":"v"];
      const arr=m.get(k)||[];
      arr.push(e);
      m.set(k,arr);
    }
    return m;
  }
  function computeLevels(nodes,edges){
    const nodeById=new Map(nodes.map(n=>[n.id,n]));
    const inDeg=new Map();
    for(const n of nodes) inDeg.set(n.id,0);
    for(const e of edges){
      if(!nodeById.has(e.u)||!nodeById.has(e.v)) continue;
      inDeg.set(e.v,(inDeg.get(e.v)||0)+1);
    }
    let queue=[];
    for(const [id,d] of inDeg.entries()) if(d===0) queue.push(id);
    if(queue.length===0) queue=nodes.map(n=>n.id);
    const level=new Map();
    for(const id of queue) level.set(id,0);
    let changed=true, rounds=0;
    while(changed && rounds<30){
      changed=false; rounds++;
      for(const e of edges){
        const lu=level.get(e.u);
        if(lu===undefined) continue;
        const want=lu+1;
        const lv=level.get(e.v);
        if(lv===undefined || want>lv){level.set(e.v,want); changed=true;}
      }
    }
    const buckets=new Map();
    for(const n of nodes){
      const l=level.get(n.id)??0;
      const arr=buckets.get(l)||[];
      arr.push(n.id);
      buckets.set(l,arr);
    }
    for(const [l,arr] of buckets.entries()){
      arr.sort((a,b)=>shortLabel(nodeById.get(a)).localeCompare(shortLabel(nodeById.get(b))));
      buckets.set(l,arr);
    }
    return {buckets,nodeById};
  }

  window.LineageGraph = {
    createGraph(container, callbacks){
      const {svg,viewport}=createSVG(container);
      const onNodeClick=(callbacks&&callbacks.onNodeClick)||(()=>{});
      const onEdgeClick=(callbacks&&callbacks.onEdgeClick)||(()=>{});
      const onBgClick=(callbacks&&callbacks.onBgClick)||(()=>{});

      let data={nodes:[],edges:[]};
      let nodeEls=new Map();   // id -> {g,r,t,sub,node}
      let edgeEls=[];          // [{el,e}]
      let positions=new Map(); // id -> {x,y,w,h}
      let scale=1.0, tx=0, ty=0;
      let isPanning=false, panStart=null, dragNode=null;

      // derived
      let adjOut=new Map();
      let adjIn=new Map();

      // lanes
      let lanesEnabled = true;
      let laneOrder = ["S3 RAW","S3 CONFORMED","ORACLE ODS","ORACLE STAGING","ORACLE MART","S3","ORACLE","OTHER"];

      function ensureOverlays(){
        // minimap + legend overlays inside #graph container
        if(!container.querySelector("#minimap")){
          const mm=document.createElement("div");
          mm.id="minimap";
          mm.innerHTML = '<svg width="100%" height="100%"></svg>';
          container.appendChild(mm);
        }
        if(!container.querySelector("#legend")){
          const lg=document.createElement("div");
          lg.id="legend";
          lg.innerHTML = `
            <div class="legend-item"><span class="swatch" style="background:rgba(124,58,237,0.75)"></span>Dataset</div>
            <div class="legend-item"><span class="swatch" style="background:rgba(125,211,252,0.75)"></span>Column</div>
            <div class="legend-item"><span class="swatch" style="background:rgba(167,139,250,0.95)"></span>Highlighted path</div>
          `;
          container.appendChild(lg);
        }
      }

      function clear(){
        viewport.innerHTML="";
        nodeEls.clear();
        edgeEls=[];
        positions.clear();
      }

      function calcLanes(nodes){
        const laneMap=new Map(); // lane -> []
        for(const n of nodes){
          if(n.type!=="dataset") continue;
          const ln = laneForDatasetName(n.name || "");
          const arr = laneMap.get(ln) || [];
          arr.push(n.id);
          laneMap.set(ln, arr);
        }
        // ensure all lanes exist in order
        for(const l of laneOrder){
          if(!laneMap.has(l)) laneMap.set(l, []);
        }
        return laneMap;
      }

      function layout(){
        ensureOverlays();

        const rect=container.getBoundingClientRect();
        const W=Math.max(700,rect.width), H=Math.max(520,rect.height);
        const nodes=data.nodes||[];
        const edges=(data.edges||[]).filter(e=>e.edge_type!=="contains"); // hide contains edges in view
        adjOut=buildAdj(edges,"out");
        adjIn=buildAdj(edges,"in");

        const {buckets,nodeById}=computeLevels(nodes,edges);
        const levels=Array.from(buckets.keys()).sort((a,b)=>a-b);

        const xStep=360, yStep=86, marginX=90, marginY=64;

        // lane bands
        const laneMap = calcLanes(nodes);
        const laneIds = laneOrder.filter(l => (laneMap.get(l)||[]).length>0);
        const showLanes = lanesEnabled && laneIds.length>0 && nodes.some(n=>n.type==="dataset");
        const laneBandH = showLanes ? Math.max(90, Math.min(160, (H-140)/laneIds.length)) : 0;

        // assign each dataset node a lane index; columns follow their dataset's lane
        const laneIndex = new Map();
        if(showLanes){
          laneIds.forEach((l, idx)=>{
            for(const id of (laneMap.get(l)||[])) laneIndex.set(id, idx);
          });
        }

        // position nodes: y is lane-based for datasets; columns are grouped by level, below their dataset
        for(const l of levels){
          const ids=buckets.get(l)||[];

          // group ids by lane (datasets) then everything else
          const grouped = new Map();
          for(const id of ids){
            const n=nodeById.get(id);
            let li = 9999;
            if(n && n.type==="dataset" && showLanes) li = laneIndex.get(id) ?? 9999;
            if(n && n.type==="column" && showLanes){
              const dsId = "ds:" + (n.dataset || "");
              li = laneIndex.get(dsId) ?? 9999;
            }
            const arr = grouped.get(li) || [];
            arr.push(id);
            grouped.set(li, arr);
          }
          const keys = Array.from(grouped.keys()).sort((a,b)=>a-b);

          let laneOffsets = new Map(); // lane -> count
          for(const k of keys){
            const arr = grouped.get(k) || [];
            for(const id of arr){
              const n=nodeById.get(id);
              const isDs = (n && n.type==="dataset") || isDataset(id);
              const w=isDs?300:240;
              const h=isDs?50:44;
              const x=marginX+l*xStep;

              if(showLanes && k !== 9999){
                const baseY = marginY + k*laneBandH;
                const used = laneOffsets.get(k) || 0;
                const y = baseY + 22 + used * (isDs?64:54);
                laneOffsets.set(k, used+1);
                positions.set(id,{x,y,w,h});
              } else {
                // fallback stacking
                const i = (laneOffsets.get(k) || 0);
                const y=marginY+i*yStep;
                laneOffsets.set(k, i+1);
                positions.set(id,{x,y,w,h});
              }
            }
          }
        }

        // Compute content bounds
        const xs=[...positions.values()].map(p=>p.x);
        const ys=[...positions.values()].map(p=>p.y);
        const minX=Math.min(...xs,0), minY=Math.min(...ys,0);
        const maxX=Math.max(...xs,0)+360, maxY=Math.max(...ys,0)+140;
        const contentW=Math.max(1,maxX-minX), contentH=Math.max(1,maxY-minY);
        scale=clamp(Math.min(W/contentW,H/contentH),0.55,1.25);
        tx=(W-contentW*scale)/2 - minX*scale;
        ty=(H-contentH*scale)/2 - minY*scale;
        setTransform(viewport,tx,ty,scale);

        return {edges,nodeById, showLanes, laneIds, laneBandH};
      }

      function edgePath(u,v){
        const pu=positions.get(u), pv=positions.get(v);
        if(!pu||!pv) return "";
        const x1=pu.x+pu.w, y1=pu.y+pu.h/2;
        const x2=pv.x, y2=pv.y+pv.h/2;
        const dx=Math.max(80,(x2-x1)*0.55);
        const c1x=x1+dx, c2x=x2-dx;
        return `M ${x1} ${y1} C ${c1x} ${y1}, ${c2x} ${y2}, ${x2} ${y2}`;
      }

      function drawMinimap(){
        const mm = container.querySelector("#minimap svg");
        if(!mm) return;
        mm.innerHTML = "";
        const rect=container.getBoundingClientRect();
        const W=Math.max(700,rect.width), H=Math.max(520,rect.height);
        const mmW = 220, mmH = 140;

        const xs=[...positions.values()].map(p=>p.x);
        const ys=[...positions.values()].map(p=>p.y);
        const minX=Math.min(...xs,0), minY=Math.min(...ys,0);
        const maxX=Math.max(...xs,0)+340, maxY=Math.max(...ys,0)+140;
        const cW=Math.max(1,maxX-minX), cH=Math.max(1,maxY-minY);
        const s = Math.min(mmW/cW, mmH/cH);

        // draw edges lightly
        for(const it of edgeEls){
          const e = it.e;
          const pu=positions.get(e.u), pv=positions.get(e.v);
          if(!pu||!pv) continue;
          const x1=(pu.x-minX)*s, y1=(pu.y-minY)*s;
          const x2=(pv.x-minX)*s, y2=(pv.y-minY)*s;
          const line=document.createElementNS(mm.namespaceURI,"line");
          line.setAttribute("x1", x1 + pu.w*s);
          line.setAttribute("y1", y1 + pu.h*s/2);
          line.setAttribute("x2", x2);
          line.setAttribute("y2", y2 + pv.h*s/2);
          line.setAttribute("stroke", "rgba(255,255,255,0.10)");
          line.setAttribute("stroke-width", "1");
          mm.appendChild(line);
        }

        // draw nodes as tiny rects
        for(const [id,p] of positions.entries()){
          const n = nodeEls.get(id)?.node;
          const r=document.createElementNS(mm.namespaceURI,"rect");
          r.setAttribute("x", (p.x-minX)*s);
          r.setAttribute("y", (p.y-minY)*s);
          r.setAttribute("width", Math.max(6, p.w*s*0.25));
          r.setAttribute("height", Math.max(4, p.h*s*0.25));
          r.setAttribute("rx", "2");
          const isDs = n ? (n.type==="dataset") : isDataset(id);
          r.setAttribute("fill", isDs ? "rgba(167,139,250,0.35)" : "rgba(125,211,252,0.28)");
          mm.appendChild(r);
        }

        // viewbox indicator: approximate
        const vb=document.createElementNS(mm.namespaceURI,"rect");
        vb.setAttribute("x", 6);
        vb.setAttribute("y", 6);
        vb.setAttribute("width", mmW-12);
        vb.setAttribute("height", mmH-12);
        vb.setAttribute("fill","none");
        vb.setAttribute("stroke","rgba(255,255,255,0.14)");
        vb.setAttribute("stroke-width","1");
        mm.appendChild(vb);
      }

      function setEdgeStyle(item, mode){
        if(mode==="hi"){
          item.el.setAttribute("stroke", COLORS.edgeHi);
          item.el.setAttribute("marker-end","url(#arrowHi)");
          item.el.setAttribute("stroke-width","2.6");
        } else if(mode==="dim"){
          item.el.setAttribute("stroke", COLORS.edgeDim);
          item.el.setAttribute("marker-end","url(#arrow)");
          item.el.setAttribute("stroke-width","2.0");
        } else {
          item.el.setAttribute("stroke", COLORS.edge);
          item.el.setAttribute("marker-end","url(#arrow)");
          item.el.setAttribute("stroke-width","2.2");
        }
      }

      function highlightNeighborhood(nodeId){
        const inE = adjIn.get(nodeId) || [];
        const outE = adjOut.get(nodeId) || [];
        const keep = new Set([...inE, ...outE].map(e => e.u+"->"+e.v+"|"+e.edge_type));
        for(const it of edgeEls){
          const key = it.e.u+"->"+it.e.v+"|"+it.e.edge_type;
          if(keep.has(key)) setEdgeStyle(it,"hi"); else setEdgeStyle(it,"dim");
        }
      }
      function clearHighlight(){
        for(const it of edgeEls) setEdgeStyle(it,"norm");
      }

      function draw(){
        clear();
        const {edges,nodeById, showLanes, laneIds, laneBandH} = layout();

        const laneLayer=document.createElementNS(svg.namespaceURI,"g");
        const edgeLayer=document.createElementNS(svg.namespaceURI,"g");
        const nodeLayer=document.createElementNS(svg.namespaceURI,"g");
        viewport.appendChild(laneLayer);
        viewport.appendChild(edgeLayer);
        viewport.appendChild(nodeLayer);

        // swimlanes bands
        if(showLanes){
          laneIds.forEach((l, idx)=>{
            const y = 64 + idx*laneBandH;
            const band=document.createElementNS(svg.namespaceURI,"rect");
            band.setAttribute("x", 40);
            band.setAttribute("y", y);
            band.setAttribute("width", 5000);
            band.setAttribute("height", laneBandH);
            band.setAttribute("fill", idx%2===0 ? COLORS.laneFill : COLORS.laneFillAlt);
            band.setAttribute("stroke", COLORS.laneStroke);
            band.setAttribute("stroke-width","1");
            laneLayer.appendChild(band);

            const text=document.createElementNS(svg.namespaceURI,"text");
            text.setAttribute("x", 54);
            text.setAttribute("y", y+22);
            text.setAttribute("fill", "rgba(255,255,255,0.55)");
            text.setAttribute("font-size", "12");
            text.setAttribute("font-weight", "650");
            text.textContent = l;
            laneLayer.appendChild(text);
          });
        }

        // edges
        edges.forEach((e)=>{
          const path=document.createElementNS(svg.namespaceURI,"path");
          path.setAttribute("d",edgePath(e.u,e.v));
          path.setAttribute("fill","none");
          path.setAttribute("stroke",COLORS.edge);
          path.setAttribute("stroke-width","2.2");
          path.setAttribute("marker-end","url(#arrow)");
          path.style.cursor="pointer";
          path.addEventListener("mouseenter",()=>setEdgeStyle({el:path,e}, "hi"));
          path.addEventListener("mouseleave",()=>setEdgeStyle({el:path,e}, "norm"));
          path.addEventListener("click",(ev)=>{ev.stopPropagation(); onEdgeClick(e);});
          edgeLayer.appendChild(path);
          edgeEls.push({el:path,e});
        });

        // nodes
        (data.nodes||[]).forEach((n)=>{
          const p=positions.get(n.id)||{x:0,y:0,w:260,h:46};
          const g=document.createElementNS(svg.namespaceURI,"g");
          g.style.cursor="grab";

          const r=document.createElementNS(svg.namespaceURI,"rect");
          r.setAttribute("x",p.x); r.setAttribute("y",p.y);
          r.setAttribute("rx","12"); r.setAttribute("ry","12");
          r.setAttribute("width",p.w); r.setAttribute("height",p.h);
          const isDs=n.type==="dataset"||isDataset(n.id);
          r.setAttribute("fill", isDs?COLORS.dsFill:COLORS.colFill);
          r.setAttribute("stroke", isDs?COLORS.dsStroke:COLORS.colStroke);
          r.setAttribute("stroke-width","1.6");

          const t=document.createElementNS(svg.namespaceURI,"text");
          t.setAttribute("x",p.x+14); t.setAttribute("y",p.y+28);
          t.setAttribute("fill",COLORS.text);
          t.setAttribute("font-size","13");
          t.setAttribute("font-weight","700");
          t.textContent=shortLabel(n);

          const sub=document.createElementNS(svg.namespaceURI,"text");
          sub.setAttribute("x",p.x+14); sub.setAttribute("y",p.y+44);
          sub.setAttribute("fill",COLORS.subtext);
          sub.setAttribute("font-size","11");
          sub.textContent=isDs?(n.name||"").slice(0,58):((n.dataset||"").split(".").slice(-2).join("."));

          const title=document.createElementNS(svg.namespaceURI,"title");
          title.textContent=n.name||n.id;
          g.appendChild(title);

          g.appendChild(r); g.appendChild(t); g.appendChild(sub);

          g.addEventListener("mouseenter",()=>{
            r.setAttribute("stroke-width","2.4");
            highlightNeighborhood(n.id);
          });
          g.addEventListener("mouseleave",()=>{
            r.setAttribute("stroke-width","1.6");
            clearHighlight();
          });
          g.addEventListener("mousedown",(ev)=>{
            ev.stopPropagation();
            const pos=pointerPos(svg,ev);
            dragNode={id:n.id, dx:pos.x-p.x, dy:pos.y-p.y};
            g.style.cursor="grabbing";
          });
          g.addEventListener("click",(ev)=>{ev.stopPropagation(); onNodeClick(n);});

          nodeLayer.appendChild(g);
          nodeEls.set(n.id,{g,r,t,sub,node:n});
        });

        svg.onclick=()=>{clearHighlight(); onBgClick();};
        drawMinimap();
      }

      function updateEdges(){
        function edgePath(u,v){
          const pu=positions.get(u), pv=positions.get(v);
          if(!pu||!pv) return "";
          const x1=pu.x+pu.w, y1=pu.y+pu.h/2;
          const x2=pv.x, y2=pv.y+pv.h/2;
          const dx=Math.max(80,(x2-x1)*0.55);
          const c1x=x1+dx, c2x=x2-dx;
          return `M ${x1} ${y1} C ${c1x} ${y1}, ${c2x} ${y2}, ${x2} ${y2}`;
        }
        for(const item of edgeEls){
          item.el.setAttribute("d", edgePath(item.e.u, item.e.v));
        }
        drawMinimap();
      }

      svg.addEventListener("mousedown",(ev)=>{
        const pos=pointerPos(svg,ev);
        isPanning=true;
        panStart={x:pos.x,y:pos.y,tx,ty};
      });
      svg.addEventListener("mousemove",(ev)=>{
        const pos=pointerPos(svg,ev);
        if(dragNode){
          const p=positions.get(dragNode.id); if(!p) return;
          p.x=pos.x-dragNode.dx; p.y=pos.y-dragNode.dy; positions.set(dragNode.id,p);
          const els=nodeEls.get(dragNode.id);
          if(els){
            els.r.setAttribute("x",p.x); els.r.setAttribute("y",p.y);
            els.t.setAttribute("x",p.x+14); els.t.setAttribute("y",p.y+28);
            els.sub.setAttribute("x",p.x+14); els.sub.setAttribute("y",p.y+44);
          }
          updateEdges(); return;
        }
        if(isPanning && panStart){
          const dx=(pos.x-panStart.x), dy=(pos.y-panStart.y);
          tx=panStart.tx+dx*scale; ty=panStart.ty+dy*scale;
          setTransform(viewport,tx,ty,scale);
        }
      });
      function endDrag(){
        if(dragNode){
          const els=nodeEls.get(dragNode.id);
          if(els) els.g.style.cursor="grab";
        }
        dragNode=null;
      }
      svg.addEventListener("mouseup",()=>{isPanning=false; panStart=null; endDrag();});
      svg.addEventListener("mouseleave",()=>{isPanning=false; panStart=null; endDrag();});
      svg.addEventListener("wheel",(ev)=>{
        ev.preventDefault();
        const factor=(Math.sign(ev.deltaY)>0)?0.92:1.08;
        const next=clamp(scale*factor,0.35,2.3);
        const r=svg.getBoundingClientRect();
        const mx=ev.clientX-r.left, my=ev.clientY-r.top;
        tx = mx - (mx - tx) * (next / scale);
        ty = my - (my - ty) * (next / scale);
        scale=next;
        setTransform(viewport,tx,ty,scale);
      }, {passive:false});

      return {
        setData(newData){data=newData||{nodes:[],edges:[]}; draw();},
        center(){draw();},
        startSim(){},
        stopSim(){},
        setLanesEnabled(v){lanesEnabled=!!v; draw();},
        focusNodeByName(query){
          const q=(query||"").toLowerCase().trim();
          if(!q) return null;
          let best=null;
          for(const [id,obj] of nodeEls.entries()){
            const n=obj.node;
            const name=(n.name||"").toLowerCase();
            if(name.includes(q) || shortLabel(n).toLowerCase().includes(q)){
              best=id; break;
            }
          }
          if(best){
            const p=positions.get(best);
            if(p){
              // center to that node
              const rect=container.getBoundingClientRect();
              const cx = rect.width/2, cy = rect.height/2;
              const targetX = (p.x + p.w/2);
              const targetY = (p.y + p.h/2);
              tx = cx - targetX*scale;
              ty = cy - targetY*scale;
              setTransform(viewport, tx, ty, scale);
              // pulse stroke
              const els=nodeEls.get(best);
              if(els){
                els.r.setAttribute("stroke-width","3.2");
                setTimeout(()=>els.r.setAttribute("stroke-width","1.6"), 700);
              }
            }
          }
          return best;
        }
      };
    }
  };
})();
