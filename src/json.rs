use std::collections::HashMap;

use petgraph::graphmap::UnGraphMap;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::CreditcoinNode;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Graph<K> {
    options: Options,
    nodes: Vec<Node<K>>,
    edges: Vec<Edge<K>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attributes: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edge<K> {
    #[serde(skip_serializing_if = "Option::is_none")]
    key: Option<K>,
    source: K,
    target: K,
    #[serde(skip_serializing_if = "Option::is_none")]
    attributes: Option<Value>,
    undirected: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node<K> {
    key: K,
    #[serde(skip_serializing_if = "Option::is_none")]
    attributes: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Options {
    allow_self_loops: bool,
    multi: bool,
    #[serde(rename = "type")]
    typ: String,
}

impl From<&UnGraphMap<CreditcoinNode, ()>> for Graph<u64> {
    fn from(graph: &UnGraphMap<CreditcoinNode, ()>) -> Self {
        let mut key = 0;

        let mut node_keys = HashMap::new();

        let nodes = graph
            .nodes()
            .map(|node| {
                let k = key;
                key += 1;

                let node_label = (&*node.endpoint).clone();
                node_keys.insert(node, k);

                Node {
                    key: k,
                    attributes: Some(json! {
                        {
                            "label": node_label,
                        }
                    }),
                }
            })
            .collect();

        let edges = graph
            .all_edges()
            .map(|(node1, node2, _)| Edge {
                key: {
                    let k = key;
                    key += 1;
                    Some(k)
                },
                source: *node_keys.get(&node1).unwrap(),
                target: *node_keys.get(&node2).unwrap(),
                attributes: Some(json! {
                    {
                        "size": 3,
                    }
                }),
                undirected: true,
            })
            .collect();

        Graph {
            options: Options {
                allow_self_loops: false,
                multi: false,
                typ: String::from("undirected"),
            },
            nodes,
            edges,
            attributes: None,
        }
    }
}

impl Graph<u64> {
    pub fn new(graph: &UnGraphMap<CreditcoinNode, ()>) -> Self {
        Self::from(graph)
    }
}
