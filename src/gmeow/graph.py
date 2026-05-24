# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Compute and expose the message knowledge graph for Gmeow.

This module builds RDF-style triples, registers them with rustworkx, and exposes weighted/ranked
projection helpers used by MCP and HTTP graph endpoints. It bridges the cache-resident graph store
with the in-memory analysis routines so callers can reason about people, projects, and topics.
"""

import itertools
import re
from collections import deque
from collections.abc import Sequence
from email.utils import getaddresses, parseaddr
from typing import Any, TypedDict

import rustworkx as rx

from .kg import clean_text_for_kg, extract_message_kg, extract_sidecar_kg
from .parser import ParsedMessage

GraphTriple = tuple[str, str, str, str]
GraphPath = dict[str, Any]
NO_SOURCE_MESSAGE = ""


class WeightedGraphData(TypedDict):
    """Typed rustworkx graph data used by weighted graph operations."""

    graph: rx.PyDiGraph
    nodes: dict[str, int]
    reverse_nodes: dict[int, str]
    edge_lookup: dict[tuple[int, int], dict[str, Any]]


IRI = "gmeow:"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
FOAF = "http://xmlns.com/foaf/0.1/"
SIOC = "http://rdfs.org/sioc/ns#"
SCHEMA = "https://schema.org/"
SKOS = "http://www.w3.org/2004/02/skos/core#"
PROV = "http://www.w3.org/ns/prov#"
DOAP = "http://usefulinc.com/ns/doap#"

ONTOLOGY_PROFILE = {
    "rdf": {"namespace": "http://www.w3.org/1999/02/22-rdf-syntax-ns#", "purpose": "RDF type assertions"},
    "rdfs": {"namespace": "http://www.w3.org/2000/01/rdf-schema#", "purpose": "Human-readable labels"},
    "foaf": {"namespace": FOAF, "purpose": "People, accounts, and mailbox contacts"},
    "sioc": {"namespace": SIOC, "purpose": "Posts, threads, containers, authors, replies"},
    "schemaorg": {"namespace": SCHEMA, "purpose": "EmailMessage, attachments, dates, subjects"},
    "skos": {"namespace": SKOS, "purpose": "Labels, categories, concepts"},
    "prov_o": {"namespace": PROV, "purpose": "Extraction and source provenance"},
    "doap": {"namespace": DOAP, "purpose": "Software project/repository notifications and repository/project context"},
    "gmeow": {"namespace": IRI, "purpose": "Mailbox-specific operational relationships"},
}


def _node(kind: str, value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.:@+-]+", "_", value.strip())[:240]
    return f"{IRI}{kind}/{safe}"


def _edge_weight(edge: dict[str, Any]) -> float:
    return float(edge["weight"])


def _inverse_edge_weight(edge: dict[str, Any]) -> float:
    return 1.0 / max(float(edge["weight"]), 0.000001)


def extract_triples(message: ParsedMessage, attachment_sha1s: Sequence[str] = ()) -> list[GraphTriple]:
    """Extract triples."""
    msg = _node("message", message.gmail_id)
    clean_text = clean_text_for_kg(message.text)
    triples: list[GraphTriple] = [
        (msg, RDF_TYPE, "gmeow:Message", message.gmail_id),
        (msg, RDF_TYPE, SIOC + "Post", message.gmail_id),
        (msg, RDF_TYPE, SCHEMA + "EmailMessage", message.gmail_id),
        (msg, PROV + "wasDerivedFrom", _node("gmailMessage", message.gmail_id), message.gmail_id),
    ]
    if message.subject:
        triples.append((msg, SCHEMA + "name", message.subject, message.gmail_id))
    if message.date:
        triples.append((msg, SCHEMA + "dateReceived", message.date, message.gmail_id))
    triples.extend(_thread_triples(msg, message))
    triples.extend(_sender_triples(msg, message))
    triples.extend(_recipient_triples(msg, message))
    triples.extend(_label_triples(msg, message))
    triples.extend(
        (msg, f"gmeow:header/{key}", message.headers[key], message.gmail_id)
        for key in ["message-id", "in-reply-to", "list-id"]
        if message.headers.get(key)
    )
    for sha1 in attachment_sha1s or []:
        attachment = _node("attachment", sha1)
        triples.append((msg, "gmeow:hasAttachment", attachment, message.gmail_id))
        triples.append((msg, SCHEMA + "attachment", attachment, message.gmail_id))
    triples.extend((msg, "gmeow:mentionsEntity", _node("entity", entity), message.gmail_id) for entity in extract_entities(clean_text))
    triples.extend((msg, "gmeow:hasClaim", claim, message.gmail_id) for claim in extract_claims(clean_text))
    triples.extend((msg, "gmeow:hasTask", task, message.gmail_id) for task in extract_tasks(clean_text))
    triples.extend(_project_triples(msg, message))
    message_text = "\n\n".join([message.subject or "", message.snippet, clean_text])
    triples.extend(_required_source_triples(extract_message_kg(message.gmail_id, message_text)))
    return triples


def _thread_triples(msg: str, message: ParsedMessage) -> list[GraphTriple]:
    if not message.thread_id:
        return []
    thread = _node("thread", message.thread_id)
    return [
        (msg, "gmeow:inThread", thread, message.gmail_id),
        (msg, SIOC + "has_container", thread, message.gmail_id),
        (thread, RDF_TYPE, SIOC + "Thread", message.gmail_id),
    ]


def _sender_triples(msg: str, message: ParsedMessage) -> list[GraphTriple]:
    if not message.sender:
        return []
    name, address = parseaddr(message.sender)
    node = _address_node(message.sender)
    triples: list[GraphTriple] = [
        (msg, "gmeow:from", node, message.gmail_id),
        (msg, SIOC + "has_creator", node, message.gmail_id),
        (msg, SCHEMA + "sender", node, message.gmail_id),
        (node, RDF_TYPE, FOAF + "Agent", message.gmail_id),
    ]
    if address:
        triples.append((node, FOAF + "mbox", f"mailto:{address.lower()}", message.gmail_id))
    triples.extend(_name_triples(node, name, message.gmail_id))
    return triples


def _recipient_triples(msg: str, message: ParsedMessage) -> list[GraphTriple]:
    triples: list[GraphTriple] = []
    for name, address in getaddresses([message.recipients] if message.recipients else []):
        if not address:
            continue
        node = _node("address", address.lower())
        triples.extend(
            [
                (msg, "gmeow:to", node, message.gmail_id),
                (msg, SCHEMA + "recipient", node, message.gmail_id),
                (node, RDF_TYPE, FOAF + "Agent", message.gmail_id),
                (node, FOAF + "mbox", f"mailto:{address.lower()}", message.gmail_id),
            ]
        )
        triples.extend(_name_triples(node, name, message.gmail_id))
    return triples


def _name_triples(node: str, name: str, message_id: str) -> list[GraphTriple]:
    if not name:
        return []
    return [(node, "gmeow:displayName", name, message_id), (node, FOAF + "name", name, message_id), (node, RDFS_LABEL, name, message_id)]


def _label_triples(msg: str, message: ParsedMessage) -> list[GraphTriple]:
    triples: list[GraphTriple] = []
    for label in message.label_ids:
        label_node = _node("label", label)
        triples.extend(
            [
                (msg, "gmeow:hasLabel", label_node, message.gmail_id),
                (msg, SKOS + "related", label_node, message.gmail_id),
                (label_node, RDF_TYPE, SKOS + "Concept", message.gmail_id),
            ]
        )
    return triples


def _project_triples(msg: str, message: ParsedMessage) -> list[GraphTriple]:
    if not _looks_like_dev_project(message):
        return []
    project = _dev_project_node(message)
    triples: list[GraphTriple] = [
        (msg, "gmeow:aboutProject", project, message.gmail_id),
        (msg, SCHEMA + "about", project, message.gmail_id),
        (msg, FOAF + "topic", project, message.gmail_id),
        (project, RDF_TYPE, DOAP + "Project", message.gmail_id),
        (project, RDF_TYPE, SCHEMA + "SoftwareSourceCode", message.gmail_id),
        (project, DOAP + "name", _project_label(project), message.gmail_id),
        (project, RDFS_LABEL, _project_label(project), message.gmail_id),
    ]
    repository = _dev_repository_url(message)
    if repository:
        triples.append((project, DOAP + "repository", repository, message.gmail_id))
        triples.append((project, SCHEMA + "codeRepository", repository, message.gmail_id))
    return triples


def extract_attachment_sidecar_triples(sha1: str, metadata: dict[str, Any]) -> list[GraphTriple]:
    """Extract attachment sidecar triples."""
    return _required_source_triples(extract_sidecar_kg(sha1, metadata))


def _required_source_triples(triples: Sequence[tuple[str, str, str, object]]) -> list[GraphTriple]:
    return [(subject, predicate, obj, str(source) if source else NO_SOURCE_MESSAGE) for subject, predicate, obj, source in triples]


def _summary_node_sort_key(item: dict[str, Any]) -> tuple[int, int, str]:
    return (-int(item["messages"]), -int(item["degree"]), str(item["id"]))


def _related_node_sort_key(item: dict[str, Any]) -> tuple[float, str]:
    return (float(item["distance"]), str(item["node"]))


def extract_entities(text: str) -> list[str]:
    """Extract entities."""
    candidates = re.findall(r"\b[A-Z][A-Za-z0-9&.-]+(?:\s+[A-Z][A-Za-z0-9&.-]+){0,3}\b", text or "")
    stop = {"I", "The", "A", "This", "That", "Thanks", "Regards"}
    return sorted({candidate for candidate in candidates if candidate not in stop})[:50]


def extract_claims(text: str) -> list[str]:
    """Extract claims."""
    claims = [
        sentence.strip()[:500]
        for sentence in re.split(r"(?<=[.!?])\s+", text or "")
        if re.search(r"\b(is|are|was|were|will|must|should|costs?|means|requires?)\b", sentence, re.IGNORECASE)
    ]
    return claims[:25]


def extract_tasks(text: str) -> list[str]:
    """Extract tasks."""
    tasks = [
        sentence.strip()[:500]
        for sentence in re.split(r"(?<=[.!?])\s+", text or "")
        if re.search(r"\b(todo|please|action required|need to|must|by \w+day|due)\b", sentence, re.IGNORECASE)
    ]
    return tasks[:25]


class GraphProjector:
    """Represent GraphProjector data and behavior."""

    def build(self, triples: list[GraphTriple]) -> rx.PyDiGraph:
        """Build."""
        graph = rx.PyDiGraph()
        nodes: dict[str, int] = {}
        for subject, predicate, obj, _ in triples:
            for value in [subject, obj]:
                if value not in nodes:
                    nodes[value] = graph.add_node(value)
            graph.add_edge(nodes[subject], nodes[obj], predicate)
        return graph

    def summary(self, triples: list[GraphTriple], limit: int = 25) -> dict[str, Any]:
        """Summary."""
        graph = self.build(triples)
        counts: dict[str, dict[str, Any]] = {}
        for subject, predicate, obj, source in triples:
            for node in [subject, obj]:
                item = counts.setdefault(node, {"id": node, "degree": 0, "messages": set(), "predicates": set()})
                item["degree"] += 1
                if source:
                    item["messages"].add(source)
                item["predicates"].add(predicate)
        top_nodes: list[dict[str, Any]] = [
            {
                "id": item["id"],
                "degree": item["degree"],
                "messages": len(item["messages"]),
                "predicates": sorted(item["predicates"])[:10],
            }
            for item in counts.values()
        ]
        top_nodes.sort(key=_summary_node_sort_key)
        return {
            "nodes": graph.num_nodes(),
            "edges": graph.num_edges(),
            "top_nodes": top_nodes[:limit],
        }

    def shortest_path(
        self,
        triples: list[GraphTriple],
        source: str,
        target: str,
        max_depth: int = 4,
    ) -> GraphPath:
        """Shortest path."""
        adjacency: dict[str, list[GraphTriple]] = {}
        for subject, predicate, obj, message_id in triples:
            adjacency.setdefault(subject, []).append((obj, predicate, message_id, NO_SOURCE_MESSAGE))
            adjacency.setdefault(obj, []).append((subject, "^" + predicate, message_id, NO_SOURCE_MESSAGE))
        queue: deque[tuple[str, list[dict[str, Any]]]] = deque([(source, [])])
        seen = {source}
        while queue:
            node, path = queue.popleft()
            if len(path) >= max_depth:
                continue
            for next_node, predicate, message_id, _ in adjacency.get(node, []):
                if next_node in seen:
                    continue
                edge = {"from": node, "predicate": predicate, "to": next_node, "source_message_id": message_id}
                next_path = [*path, edge]
                if next_node == target:
                    return {"source": source, "target": target, "depth": len(next_path), "path": next_path}
                seen.add(next_node)
                queue.append((next_node, next_path))
        return {}


class WeightedGraphProjector:
    """Represent WeightedGraphProjector data and behavior."""

    def build(self, edges: list[dict[str, Any]], *, bidirectional: bool = True) -> WeightedGraphData:
        """Build."""
        graph = rx.PyDiGraph(multigraph=True, node_count_hint=len(edges) * 2, edge_count_hint=len(edges) * (2 if bidirectional else 1))
        nodes: dict[str, int] = {}
        edge_lookup: dict[tuple[int, int], dict[str, Any]] = {}
        for item in edges:
            subject = str(item["subject"])
            obj = str(item["object"])
            predicate = str(item["predicate"])
            weight = max(float(item.get("weight") or 1.0), 0.000001)
            if subject not in nodes:
                nodes[subject] = graph.add_node(subject)
            if obj not in nodes:
                nodes[obj] = graph.add_node(obj)
            source = nodes[subject]
            target = nodes[obj]
            data: dict[str, Any] = {**item, "predicate": predicate, "weight": weight}
            graph.add_edge(source, target, data)
            if (source, target) not in edge_lookup or weight < float(edge_lookup[(source, target)]["weight"]):
                edge_lookup[(source, target)] = data
            if bidirectional:
                reverse = {**data, "subject": obj, "object": subject, "predicate": "^" + predicate}
                graph.add_edge(target, source, reverse)
                if (target, source) not in edge_lookup or weight < float(edge_lookup[(target, source)]["weight"]):
                    edge_lookup[(target, source)] = reverse
        reverse_nodes = {index: node for node, index in nodes.items()}
        return {"graph": graph, "nodes": nodes, "reverse_nodes": reverse_nodes, "edge_lookup": edge_lookup}

    def weighted_path(self, edges: list[dict[str, Any]], source: str, target: str, max_depth: int = 4) -> GraphPath:
        """Weighted path."""
        built = self.build(edges, bidirectional=True)
        graph = built["graph"]
        nodes = built["nodes"]
        if source not in nodes or target not in nodes:
            return {}
        paths = rx.digraph_dijkstra_shortest_paths(graph, nodes[source], nodes[target], _edge_weight)
        node_path = list(paths[nodes[target]]) if nodes[target] in paths else []
        if not node_path or len(node_path) - 1 > max_depth:
            return {}
        edge_lookup = built["edge_lookup"]
        reverse_nodes = built["reverse_nodes"]
        path: list[dict[str, Any]] = []
        total = 0.0
        for left, right in itertools.pairwise(node_path):
            data = edge_lookup[(left, right)]
            weight = float(data["weight"])
            total += weight
            path.append(
                {
                    "from": reverse_nodes[left],
                    "predicate": data["predicate"],
                    "to": reverse_nodes[right],
                    "weight": round(weight, 6),
                    "evidence_count": data.get("evidence_count", 0),
                    "message_count": data.get("message_count", 0),
                }
            )
        return {
            "source": source,
            "target": target,
            "depth": len(path),
            "total_weight": round(total, 6),
            "score": round(1.0 / (1.0 + total), 6),
            "path": path,
        }

    def related_nodes(self, edges: list[dict[str, Any]], source: str, limit: int = 25, max_weight: float = 0.0) -> list[dict[str, Any]]:
        """Related nodes."""
        built = self.build(edges, bidirectional=True)
        graph = built["graph"]
        nodes = built["nodes"]
        if source not in nodes:
            return []
        lengths = rx.digraph_dijkstra_shortest_path_lengths(graph, nodes[source], _edge_weight)
        reverse_nodes = built["reverse_nodes"]
        results: list[dict[str, Any]] = []
        for index, path_distance in lengths.items():
            if index == nodes[source]:
                continue
            distance = float(path_distance)
            if max_weight and distance > max_weight:
                continue
            results.append({"node": reverse_nodes[index], "distance": round(distance, 6), "score": round(1.0 / (1.0 + distance), 6)})
        results.sort(key=_related_node_sort_key)
        return results[:limit]

    def centrality(self, edges: list[dict[str, Any]], metric: str = "pagerank") -> list[dict[str, Any]]:
        """Centrality."""
        metric = metric.lower().replace("-", "_")
        built = self.build(edges, bidirectional=False)
        graph = built["graph"]
        reverse_nodes = built["reverse_nodes"]
        if graph.num_nodes() == 0:
            return []
        if metric == "pagerank":
            scores = rx.pagerank(graph, weight_fn=_inverse_edge_weight)
        elif metric == "betweenness":
            scores = rx.digraph_betweenness_centrality(graph)
        elif metric == "closeness":
            scores = rx.digraph_closeness_centrality(graph)
        elif metric == "degree":
            scores = rx.digraph_degree_centrality(graph)
        elif metric == "in_degree":
            scores = rx.in_degree_centrality(graph)
        elif metric == "out_degree":
            scores = rx.out_degree_centrality(graph)
        elif metric == "eigenvector":
            scores = rx.digraph_eigenvector_centrality(graph, weight_fn=_inverse_edge_weight, max_iter=1000, tol=1e-5)
        elif metric == "katz":
            scores = rx.digraph_katz_centrality(graph, weight_fn=_inverse_edge_weight)
        else:
            msg = f"Unsupported centrality metric: {metric}"
            raise ValueError(msg)
        return [{"node": reverse_nodes[index], "score": float(score)} for index, score in dict(scores).items()]

    def components(self, edges: list[dict[str, Any]], mode: str = "weak") -> list[list[str]]:
        """Components."""
        built = self.build(edges, bidirectional=False)
        graph = built["graph"]
        reverse_nodes = built["reverse_nodes"]
        mode = mode.lower()
        components = rx.strongly_connected_components(graph) if mode == "strong" else rx.weakly_connected_components(graph)
        return [[reverse_nodes[index] for index in component] for component in components]

    def cycles(self, edges: list[dict[str, Any]], limit: int = 25, max_cycle_len: int = 8) -> list[list[str]]:
        """Cycles."""
        built = self.build(edges, bidirectional=False)
        graph = built["graph"]
        reverse_nodes = built["reverse_nodes"]
        results: list[list[str]] = []
        for cycle in rx.simple_cycles(graph):
            if len(cycle) <= max_cycle_len:
                results.append([reverse_nodes[index] for index in cycle])
            if len(results) >= limit:
                break
        return results


def _address_node(value: str) -> str:
    _, address = parseaddr(value)
    return _node("address", (address or value).lower())


def _looks_like_dev_project(message: ParsedMessage) -> bool:
    text = " ".join([message.subject or "", message.sender or "", message.recipients or "", message.snippet]).lower()
    return "github.com" in text or "gitlab" in text or "pull request" in text or re.search(r"\[[\w.-]+/[\w.-]+\]", text) is not None


def _dev_project_node(message: ParsedMessage) -> str:
    text = " ".join([message.subject or "", message.recipients or ""])
    match = re.search(r"[\[<]([\w.-]+/[\w.-]+)[\]>]", text)
    return _node("project", match.group(1) if match else text[:80] or message.gmail_id)


def _project_label(project_node: str) -> str:
    return project_node.rsplit("/", 1)[-1].replace("_", "/")


def _dev_repository_url(message: ParsedMessage) -> str:
    text = " ".join([message.subject or "", message.recipients or "", message.snippet or "", message.text or ""])
    match = re.search(r"github\.com[:/](?P<repo>[\w.-]+/[\w.-]+)", text, re.IGNORECASE)
    if match:
        return "https://github.com/" + match.group("repo").rstrip(".git")
    match = re.search(r"gitlab\.com[:/](?P<repo>[\w./-]+)", text, re.IGNORECASE)
    if match:
        return "https://gitlab.com/" + match.group("repo").rstrip(".git")
    match = re.search(r"[\[<](?P<repo>[\w.-]+/[\w.-]+)[\]>]", text)
    if match:
        return "https://github.com/" + match.group("repo")
    return ""
