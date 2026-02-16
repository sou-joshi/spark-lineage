import sqlite3
from typing import Dict, List, Any
import networkx as nx


class Store:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _conn(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        with self._conn() as con:
            cur = con.cursor()
            cur.execute("""
            CREATE TABLE IF NOT EXISTS datasets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE,
                kind TEXT,
                namespace TEXT
            )
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS columns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_name TEXT,
                column_name TEXT,
                UNIQUE(dataset_name, column_name)
            )
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS edges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                src TEXT,
                dst TEXT,
                edge_type TEXT,
                evidence TEXT,
                confidence REAL
            )
            """)
            con.commit()

    def reset(self):
        with self._conn() as con:
            cur = con.cursor()
            cur.execute("DELETE FROM edges")
            cur.execute("DELETE FROM columns")
            cur.execute("DELETE FROM datasets")
            con.commit()

    def upsert_dataset(self, name: str, kind: str = "table", namespace: str = ""):
        with self._conn() as con:
            cur = con.cursor()
            cur.execute("INSERT OR IGNORE INTO datasets(name, kind, namespace) VALUES (?, ?, ?)", (name, kind, namespace))
            con.commit()

    def upsert_column(self, dataset: str, column: str):
        with self._conn() as con:
            cur = con.cursor()
            cur.execute("INSERT OR IGNORE INTO columns(dataset_name, column_name) VALUES (?, ?)", (dataset, column))
            con.commit()

    def add_edge(self, src: str, dst: str, edge_type: str, evidence: str, confidence: float = 0.6):
        with self._conn() as con:
            cur = con.cursor()
            cur.execute(
                "INSERT INTO edges(src, dst, edge_type, evidence, confidence) VALUES (?, ?, ?, ?, ?)",
                (src, dst, edge_type, (evidence or "")[:2500], float(confidence)),
            )
            con.commit()

    def get_stats(self) -> Dict[str, int]:
        with self._conn() as con:
            cur = con.cursor()
            cur.execute("SELECT COUNT(*) FROM datasets")
            ds = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM columns")
            cols = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM edges")
            edges = cur.fetchone()[0]
        return {"datasets": ds, "columns": cols, "edges": edges}

    def search(self, q: str) -> Dict[str, List[str]]:
        q = q.strip().lower()
        with self._conn() as con:
            cur = con.cursor()
            cur.execute("SELECT name FROM datasets WHERE lower(name) LIKE ? ORDER BY name LIMIT 50", (f"%{q}%",))
            tables = [r[0] for r in cur.fetchall()]
            cur.execute("""
                SELECT dataset_name || '.' || column_name
                FROM columns
                WHERE lower(column_name) LIKE ? OR lower(dataset_name) LIKE ?
                ORDER BY dataset_name, column_name
                LIMIT 50
            """, (f"%{q}%", f"%{q}%"))
            cols = [r[0] for r in cur.fetchall()]
        return {"tables": tables, "columns": cols}

    def build_graph(self) -> nx.DiGraph:
        g = nx.DiGraph()
        with self._conn() as con:
            cur = con.cursor()
            cur.execute("SELECT name, kind, namespace FROM datasets")
            for name, kind, namespace in cur.fetchall():
                g.add_node(f"ds:{name}", node_type="dataset", name=name, kind=kind, namespace=namespace)

            cur.execute("SELECT dataset_name, column_name FROM columns")
            for ds, col in cur.fetchall():
                g.add_node(f"col:{ds}.{col}", node_type="column", dataset=ds, name=col)
                g.add_edge(f"ds:{ds}", f"col:{ds}.{col}", edge_type="contains", evidence="", confidence=1.0)

            cur.execute("SELECT src, dst, edge_type, evidence, confidence FROM edges")
            for src, dst, et, ev, conf in cur.fetchall():
                g.add_edge(src, dst, edge_type=et, evidence=ev, confidence=conf)
        return g

    def _subgraph(self, g: nx.DiGraph, start: str, depth: int, direction: str) -> Dict[str, Any]:
        visited = {start}
        frontier = [(start, 0)]
        nodes = {start}
        edges = []

        while frontier:
            node, d = frontier.pop(0)
            if d >= depth:
                continue
            nbrs = g.predecessors(node) if direction == "up" else g.successors(node)
            for nb in nbrs:
                u, v = (nb, node) if direction == "up" else (node, nb)
                ed = g.get_edge_data(u, v, default={})
                edges.append({
                    "u": u, "v": v,
                    "edge_type": ed.get("edge_type"),
                    "confidence": ed.get("confidence"),
                    "evidence": ed.get("evidence", ""),
                })
                if nb not in visited:
                    visited.add(nb)
                    nodes.add(nb)
                    frontier.append((nb, d + 1))

        def node_view(n):
            nd = g.nodes[n]
            if nd.get("node_type") == "dataset":
                return {"id": n, "type": "dataset", "name": nd.get("name"), "kind": nd.get("kind")}
            if nd.get("node_type") == "column":
                return {"id": n, "type": "column", "name": f"{nd.get('dataset')}.{nd.get('name')}"}
            return {"id": n, "type": "unknown", "name": n}

        return {"nodes": [node_view(n) for n in sorted(nodes)], "edges": edges}

    def get_dataset_lineage_view(self, g: nx.DiGraph, dataset_name: str, depth: int = 2) -> Dict[str, Any]:
        start = f"ds:{dataset_name}"
        if start not in g:
            return {"not_found": True, "name": dataset_name}

        up = self._subgraph(g, start, depth, "up")
        down = self._subgraph(g, start, depth, "down")

        # Top edges (with evidence) for UI
        top_edges = []
        for e in (up.get("edges", []) + down.get("edges", [])):
            ev = (e.get("evidence") or "").strip()
            if ev:
                top_edges.append(f"{e.get('u')} -> {e.get('v')} (conf={e.get('confidence')})\n{ev}")
        top_edges = top_edges[:8]

        return {"not_found": False, "name": dataset_name, "depth": depth, "up": up, "down": down, "top_edges": top_edges}

    def get_column_lineage_view(self, g: nx.DiGraph, column: str, depth: int = 2) -> Dict[str, Any]:
        if "." in column:
            start = f"col:{column}"
            if start not in g:
                return {"not_found": True, "name": column}
        else:
            matches = [n for n, nd in g.nodes(data=True) if nd.get("node_type") == "column" and nd.get("name") == column]
            if not matches:
                return {"not_found": True, "name": column}
            start = matches[0]

        up = self._subgraph(g, start, depth, "up")
        down = self._subgraph(g, start, depth, "down")
        return {"not_found": False, "name": column, "depth": depth, "up": up, "down": down}
