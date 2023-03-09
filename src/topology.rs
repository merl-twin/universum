use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug,Deserialize,PartialEq)]
#[serde(try_from = "TomlTopology")]
pub struct Topology {
    // physical host aliases
    pub hosts: BTreeMap<String,Host>,

    // logical software node tree
    pub root: TopologyNode,
}

#[derive(Debug,Clone,Deserialize,PartialEq)]
pub struct Host {
    pub host: String,
    pub port: u16,
}

#[derive(Debug,PartialEq)]
pub struct TopologyNode {
    pub name: Option<String>,
    pub parent: Option<String>,
    pub config: RunConf,
    pub node_type: TopologyNodeType,
}

#[derive(Debug,PartialEq)]
pub enum TopologyNodeType {
    Terminal,
    Node(Vec<TopologyNode>),
}

#[derive(Debug,PartialEq)]
pub enum RunConf {
    None,
    Active {
        params: serde_json::Value,
        location: Location,
    },
    Passive {
        location: Location,
    },
}

#[derive(Debug,Deserialize,PartialEq)]
pub struct Location {
    pub host: String, // host alias from topology.host
    pub port: u16,
    pub publicity: Option<Publicity>,
}

#[derive(Debug,Deserialize,PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Publicity {
    Local,
    Internal,
    External,
}

#[derive(Debug,Deserialize,PartialEq)]
struct TomlTopology {
    // physical host aliases
    hosts: BTreeMap<String,Host>,

    // logical software node tree
    root: toml::Table,

    config: toml::Table,
}
#[derive(Debug)]
pub struct ParseError {
    pub parent: String,
    pub name: String,
    pub error: String,
}
impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParseError")
            .field("parent", &self.parent)
            .field("name", &self.name)
            .field("error", &self.error)
            .finish()
    }
}

fn run_root(parent: &Option<String>, table: toml::Table, confs: &mut BTreeMap<String,RunConf>) -> Result<Vec<TopologyNode>,ParseError> {
    let mut nodes = Vec::new();
    for (name,v) in table {        
        match v {
            toml::Value::Table(t) => {
                let next_parent = match parent {
                    None => name,
                    Some(parent) => format!("{}.{}",parent,name),
                };
                nodes.extend(run_root(&Some(next_parent),t,confs)?);                    
            },
            toml::Value::Array(vs) => {
                let mut tps = Vec::new();
                for v in vs {
                    match v {
                        toml::Value::String(s) => {
                            let p = match parent {
                                None => name.clone(),
                                Some(parent) => format!("{}.{}",parent,name),
                            };
                            let n = format!("{}.{}",p,s);
                            tps.push(TopologyNode {
                                config: match confs.remove(&n) {
                                    None => return Err(ParseError{
                                        parent: p,
                                        name: s,
                                        error: format!("missed config"),
                                    }),
                                    Some(conf) => conf,
                                },
                                name: Some(n),
                                parent: Some(p),
                                node_type: TopologyNodeType::Terminal,
                            });                           
                        },
                        _ => return Err(ParseError{
                            parent: parent.clone().unwrap_or_else(||String::new()),
                            name,
                            error: format!("unexpected value: {:?}",v),
                        }),
                    }
                }
                let n = match parent {
                    None => name.clone(),
                    Some(parent) => format!("{}.{}",parent,name),
                };
                nodes.push(TopologyNode {
                    config: match confs.remove(&n) {
                        None => return Err(ParseError{
                            parent: parent.clone().unwrap_or_else(||String::new()),
                            name,
                            error: format!("missed config"),
                        }),
                        Some(conf) => conf,
                    },
                    name: Some(n),
                    parent: parent.clone(),
                    node_type: TopologyNodeType::Node(tps),
                });
            },
            v @ _ => return Err(ParseError{
                parent: parent.clone().unwrap_or_else(||String::new()),
                name,
                error: format!("unexpected value: {:?}",v),
            }),
        }
    }
    Ok(nodes)
}
fn toml_into_json(v: toml::Value) -> serde_json::Value {
    match v {
        toml::Value::String(s) => serde_json::Value::String(s),
        toml::Value::Integer(i) => serde_json::Value::Number(i.into()),
        toml::Value::Float(f) => match serde_json::value::Number::from_f64(f) {
            None => serde_json::Value::Null,
            Some(n) => serde_json::Value::Number(n),
        }
        toml::Value::Boolean(b) => serde_json::Value::Bool(b),
        toml::Value::Datetime(dt) => serde_json::Value::String(dt.to_string()),
        toml::Value::Array(vs) => serde_json::Value::Array(vs.into_iter().map(toml_into_json).collect()),
        toml::Value::Table(mv) => serde_json::Value::Object(mv.into_iter().map(|(s,v)|(s,toml_into_json(v))).collect()),
    }
}
fn run_conf(parent: &Option<String>, table: toml::Table, map: &mut BTreeMap<String,RunConf>) -> Result<(),ParseError> {
    for (name,v) in table {
        match v {
            toml::Value::Table(mut t) => {
                let conf = match (t.remove("params"),t.remove("location")) {
                    (Some(ps),Some(loc)) => RunConf::Active {
                        params: toml_into_json(ps),
                        location: loc.try_into().map_err(|e| ParseError{
                            parent: parent.clone().unwrap_or_else(||String::new()),
                            name: name.clone(),
                            error: format!("{:?}",e),
                        })?,
                    },
                    (Some(..),None) => return Err(ParseError{
                        parent: parent.clone().unwrap_or_else(||String::new()),
                        name,
                        error: format!("conf 'location' is missed"),
                    }),
                    (None,Some(..)) => return Err(ParseError{
                        parent: parent.clone().unwrap_or_else(||String::new()),
                        name,
                        error: format!("conf 'params' is missed"),
                    }),
                    _ => return Err(ParseError{
                        parent: parent.clone().unwrap_or_else(||String::new()),
                        name,
                        error: format!("conf 'location' and 'params' are missed"),
                    }),
                };
                let next_parent = match parent {
                    None => name,
                    Some(parent) => format!("{}.{}",parent,name),
                };
                map.insert(next_parent.clone(),conf);
                
                run_conf(&Some(next_parent),t,map)?;
            },
            v @ _ => return Err(ParseError{
                parent: parent.clone().unwrap_or_else(||String::new()),
                name,
                error: format!("unexpected value: {:?}",v),
            }),
        }
    }
    Ok(())
}

impl TryFrom<TomlTopology> for Topology {
    type Error = ParseError;
    fn try_from(t: TomlTopology) -> Result<Topology,ParseError> {
        let hosts = t.hosts;

        //let mut passive = false;

        let mut conf = BTreeMap::new();
        run_conf(&None,t.config,&mut conf)?;

        // check locations
        let mut services = BTreeMap::new();
        for (name,c) in &conf {
            match c {
                RunConf::Active{ location, .. } |
                RunConf::Passive{ location, .. } => {
                    match hosts.contains_key(&location.host) {
                        true => {
                            let s = format!("{}:{}",location.host,location.port);
                            match services.get(&s) {
                                None => { services.insert(s,name); },
                                Some(srv) => return Err(ParseError {
                                    parent: "config".to_string(),
                                    name: name.clone(),
                                    error: format!("duplicate service ({}:{}): {}", location.host, location.port, srv),
                                }),
                            }
                        },
                        false => return Err(ParseError {
                            parent: "config".to_string(),
                            name: name.clone(),
                            error: format!("unknown host: {}", location.host),
                        }),
                    }
                },
                RunConf::None => continue,
            }
        }
        
        let root = run_root(&None,t.root,&mut conf)?;
        /*for r in root {
            r.for_each(|node| {
                println!("{:?}",node.name);
                println!("   {:?}",node.parent);
                println!("   {:?}",node.config);
            });
        }*/

        Ok(Topology{
            hosts,
            root: TopologyNode {
                name: None,
                parent: None,
                config: RunConf::None,
                node_type: TopologyNodeType::Node(root),
            },
        })
    }
}

impl TopologyNode {
    pub fn for_each<F>(&self, mut f: F)
    where F: FnMut(&TopologyNode)
    {
        f(self);
        match &self.node_type {
            TopologyNodeType::Node(v) => for n in v {
                f(n);
            },
            TopologyNodeType::Terminal => {},
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    fn example() -> &'static str {
        "# Hosts

[hosts]
r1 = { host = \"r1.local\", port = 25000 }
r2 = { host = \"r2.local\", port = 25000 }


# Topology

[root]
r1 = [\"d-a\", \"s-2\"]

[root.r2]
d = []
s = [\"s-1\", \"s-2\", \"s-3\"]




# Soft specific data

[config.r1]
params = { mode = \"p\", cache = true }
location = { host = \"r1\", port = 25100, publicity = \"internal\"}

[config.r1.d-a]
params = { mode = \"d\", data = [ \"data1\" ] }
location = { host = \"r1\", port = 25101, publicity = \"local\" }

[config.r1.s-2]
params = { mode = \"s\", data = [ \"data2\", \"data3\"] }
location = { host = \"r1\", port = 25102 }

[config.r2]
params = { mode = \"p\", cache = true }
location = { host = \"r2\", port = 25100, publicity = \"internal\"}

[config.r2.d]
params = { mode = \"p\" }
location = { host = \"r2\", port = 25200, publicity = \"internal\"}

[config.r2.s]
params = { mode = \"p\" }
location = { host = \"r2\", port = 25201, publicity = \"internal\"}

[config.r2.s.s-1]
params = { mode = \"s\", data = [ \"data1\" ] }
location = { host = \"r2\", port = 25101, publicity = \"local\" }

[config.r2.s.s-2]
params = { mode = \"s\", data = [ \"data2\" ] }
location = { host = \"r2\", port = 25102, publicity = \"local\" }

[config.r2.s.s-3]
params = { mode = \"s\", data = [ \"data3\" ] }
location = { host = \"r2\", port = 25103, publicity = \"local\" }
"           
    }
    
    fn vec_into_table(s: Vec<(&str,toml::Value)>) -> toml::Table {
        s.into_iter()
            .map(|(s,v)| (s.to_string(),v))
            .collect()
    }
    fn strs_into_array(s: &[&str]) -> toml::Value {
        toml::Value::Array(s.iter().map(|s|toml::Value::String(s.to_string())).collect())
    }

    fn location(host: &str, port: i64, publicity: Option<&str>) -> toml::Value {
        toml::Value::Table(vec_into_table(match publicity {
            Some(publicity) => vec![
                ("host",toml::Value::String(host.to_string())),
                ("port",toml::Value::Integer(port)),
                ("publicity",toml::Value::String(publicity.to_string())),
            ],
            None => vec![
                ("host",toml::Value::String(host.to_string())),
                ("port",toml::Value::Integer(port)),
            ],
        }))
    }

    fn params_p(cache: Option<bool>) -> toml::Value {
        toml::Value::Table(vec_into_table(match cache {
            Some(cache) => vec![
                ("mode",toml::Value::String("p".to_string())),
                ("cache",toml::Value::Boolean(cache)),
            ],
            None => vec![
                ("mode",toml::Value::String("p".to_string())),
            ],
        }))
    }

    fn params_ds(mode: &str, data: &[&str]) -> toml::Value {
        toml::Value::Table(vec_into_table(vec![
            ("mode",toml::Value::String(mode.to_string())),
            ("data",toml::Value::Array({
                data.iter()
                    .map(|v| toml::Value::String(v.to_string()))
                    .collect()
            })),
        ]))
    }
    
    #[test]
    fn toml_topology() {
        use toml::Value;
        
        let t: TomlTopology = toml::from_str(example()).unwrap();

        let r = TomlTopology {
            hosts: vec![("r1".to_string(), Host { host: "r1.local".to_string(), port: 25000 }),
                        ("r2".to_string(), Host { host: "r2.local".to_string(), port: 25000 })]
                .into_iter()
                .collect(),
            root: vec_into_table(vec![
                ("r1", strs_into_array(&["d-a","s-2"])),
                ("r2", Value::Table(vec_into_table(vec![
                    ("d",strs_into_array(&[])),
                    ("s",strs_into_array(&["s-1","s-2","s-3"])),
                ]))),
            ]),
            
            config: vec_into_table(vec![
                ("r1",Value::Table(vec_into_table(vec![
                    ("location",location("r1",25100,Some("internal"))),
                    ("params", params_p(Some(true))),
                    ("d-a", Value::Table(vec_into_table(vec![
                        ("location", location("r1",25101,Some("local"))),
                        ("params", params_ds("d",&["data1"])),
                    ]))),
                    ("s-2", Value::Table(vec_into_table(vec![
                        ("location", location("r1",25102,None)),
                        ("params", params_ds("s",&["data2","data3"])),
                    ]))),                    
                ]))),
                ("r2",Value::Table(vec_into_table(vec![
                    ("location", location("r2",25100,Some("internal"))),
                    ("params", params_p(Some(true))),
                    ("d", Value::Table(vec_into_table(vec![
                        ("location", location("r2",25200,Some("internal"))),
                        ("params", params_p(None)),
                    ]))),
                    ("s",Value::Table(vec_into_table(vec![
                        ("location", location("r2",25201,Some("internal"))),
                        ("params", params_p(None)),
                        ("s-1",Value::Table(vec_into_table(vec![
                            ("location", location("r2",25101,Some("local"))),
                            ("params", params_ds("s",&["data1"])),
                        ]))),
                        ("s-2",Value::Table(vec_into_table(vec![
                            ("location", location("r2",25102,Some("local"))),
                            ("params", params_ds("s",&["data2"])),
                        ]))),
                        ("s-3",Value::Table(vec_into_table(vec![
                            ("location", location("r2",25103,Some("local"))),
                            ("params", params_ds("s",&["data3"])),
                        ]))),
                    ]))),
                ]))),
            ]),
        };
        
        assert_eq!(t,r);
    }

    #[test]
    fn topology_basic() {
        use serde_json::json;
        
        let t: Topology = toml::from_str(example()).unwrap();

        let r = Topology {
            hosts: vec![("r1".to_string(), Host { host: "r1.local".to_string(), port: 25000 }),
                        ("r2".to_string(), Host { host: "r2.local".to_string(), port: 25000 })]
                .into_iter()
                .collect(),
            root: TopologyNode {
                name: None,
                parent: None,
                config: RunConf::None,
                node_type: TopologyNodeType::Node(vec![
                    TopologyNode {
                        name: Some("r1".to_string()),
                        parent: None,
                        config: RunConf::Active { params: json!({ "cache": true, "mode": "p" }),
                                                  location: Location { host: "r1".to_string(), port: 25100, publicity: Some(Publicity::Internal) } },
                        node_type: TopologyNodeType::Node(vec![
                            TopologyNode {
                                name: Some("r1.d-a".to_string()),
                                parent: Some("r1".to_string()),
                                config: RunConf::Active { params: json!({ "data": [ "data1" ], "mode": "d" }),
                                                          location: Location { host: "r1".to_string(), port: 25101, publicity: Some(Publicity::Local) } },
                                node_type: TopologyNodeType::Terminal },
                            TopologyNode {
                                name: Some("r1.s-2".to_string()),
                                parent: Some("r1".to_string()),
                                config: RunConf::Active { params: json!({"data": [ "data2", "data3" ], "mode": "s" }),
                                                          location: Location { host: "r1".to_string(), port: 25102, publicity: None } },
                                node_type: TopologyNodeType::Terminal }
                        ])
                    },
                    TopologyNode {
                        name: Some("r2.d".to_string()),
                        parent: Some("r2".to_string()),
                        config: RunConf::Active { params: json!({ "mode": "p" }),
                                                  location: Location { host: "r2".to_string(), port: 25200, publicity: Some(Publicity::Internal) } },
                        node_type: TopologyNodeType::Node(vec![]) },
                    TopologyNode {
                        name: Some("r2.s".to_string()),
                        parent: Some("r2".to_string()),
                        config: RunConf::Active { params: json!({ "mode": "p" }),
                                                  location: Location { host: "r2".to_string(), port: 25201, publicity: Some(Publicity::Internal) } },
                        node_type: TopologyNodeType::Node(vec![
                            TopologyNode {
                                name: Some("r2.s.s-1".to_string()),
                                parent: Some("r2.s".to_string()),
                                config: RunConf::Active { params: json!({ "data": [ "data1" ], "mode": "s" }),
                                                          location: Location { host: "r2".to_string(), port: 25101, publicity: Some(Publicity::Local) } },
                                node_type: TopologyNodeType::Terminal },
                            TopologyNode {
                                name: Some("r2.s.s-2".to_string()),
                                parent: Some("r2.s".to_string()),
                                config: RunConf::Active { params: json!({ "data": [ "data2" ], "mode": "s" }),
                                                          location: Location { host: "r2".to_string(), port: 25102, publicity: Some(Publicity::Local) } },
                                node_type: TopologyNodeType::Terminal },
                            TopologyNode {
                                name: Some("r2.s.s-3".to_string()),
                                parent: Some("r2.s".to_string()),
                                config: RunConf::Active { params: json!({ "data": [ "data3" ], "mode": "s" }),
                                                          location: Location { host: "r2".to_string(), port: 25103, publicity: Some(Publicity::Local) } },
                                node_type: TopologyNodeType::Terminal }
                        ])
                    }                    
                ])                
            }
        };
        
        assert_eq!(t,r);
    }
}
