use routecore::bmp::message_ng::statistics_report::{MetricType, StatType};
use serde::Serialize;

/// This binary prints out a JSON blob representing a Grafana dashboard for
/// BMP Statistics Reports messages.
///
/// Based on the related BMP Stats Report types in routecore, panels (==
/// graphs) are generated for every supported statistic codepoint. The
/// aggregation (e.g. sum by(asn, ...)) is based on the specific metric type.
///
/// Note that not all routers export all of the standardized codepoints, so
/// the Grafana dashboard will display empty graphs in some (or most) cases.
///
/// The generated output can be used as a first step when crafting a useful
/// dashboard. Usefulness of the output as-is is likely limited.
fn main() {
    let mut d = Dashboard::default();

    let mut grid_iter = GridPosIter::new();

    for (id, (desc, metric_type)) in (1..).zip(StatType::desc_iter()) {
        let expr = match metric_type {
            MetricType::CounterStat | MetricType::GaugeStat => {
                format!(
                    "sum by(asn, bmp_router_addr) (bmp_stats_report_{desc}{{job=~\"$prom_job\", asn=~\"$peer_asn\"}})"
                )
            }
            MetricType::AfiSafiGaugeStat => {
                format!(
                    "sum by(asn, bmp_router_addr, afisafi) (bmp_stats_report_{desc}{{job=~\"$prom_job\", asn=~\"$peer_asn\", afisafi=~\"$afisafi\"}})"
                )
            }
        };
        let p = Panel::new(grid_iter.next().unwrap(), id, expr);
        d.add_panel(p);
    }

    println!("{}", serde_json::to_string(&d).unwrap());
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Dashboard {
    #[serde(rename = "__inputs")]
    inputs: Vec<Input>,
    editable: bool,
    panels: Vec<Panel>,
    time: Time,
    title: String,
    //variables: Vec<Variable>, // V2 thing, or something
    templating: List,
}

impl Default for Dashboard {
    fn default() -> Self {
        Self {
            inputs: vec![Input::default()],
            editable: true,
            panels: vec![],
            time: Time::default(),
            title: "Rotonda BMP Stats Reports (generated dashboard)".into(),
            templating: List { list: vec![
                TemplateVar::new("prom_job", "Prometheus job", "label_values(bmp_stats_report_num_routes_adj_ribs_in,job)"),
                TemplateVar::new("peer_asn", "Peer ASN", "label_values({job=~\"$prom_job\"},asn)"),
                TemplateVar::new("afisafi", "afi/safi", "label_values({job=~\"$prom_job\"},afisafi)"),

            ] },
            //variables: vec![
            //    Variable::new("prom_job", "Prometheus job", "label_values(bmp_stats_report_num_routes_adj_ribs_in,job)"),
            //    Variable::new("peer_asn", "Peer ASN", "label_values({job=~\"$prom_job\"},asn)"),
            //],
        }
    }
}

impl Dashboard {
    fn add_panel(&mut self, panel: Panel) {
        self.panels.push(panel);
    }
}

#[derive(Serialize)]
struct List {
    list: Vec<TemplateVar>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TemplateVar {
    datasource: DataSource,
    definition: String,
    include_all: bool,
    label: String,
    multi: bool,
    name: String,
    options: Vec<()>,
    query: QuerySpec,
    r#type: String,
}

impl TemplateVar {
    fn new(
        name: impl AsRef<str>,
        label: impl AsRef<str>,
        query: impl AsRef<str>,
    ) -> Self {
        Self {
            datasource: DataSource::default(),
            definition: query.as_ref().into(),
            include_all: true,
            label: label.as_ref().into(),
            multi: true,
            name: name.as_ref().into(),
            options: vec![],
            query: QuerySpec {
                qry_type: 1,
                query: query.as_ref().into(),
                ref_id: "PrometheusVariableQueryEditor-VariableQuery".into(),
            },
            r#type: "query".into(),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Variable {
    kind: VariableType,
    spec: VariableSpec,
}

impl Variable {
    fn new(
        name: impl AsRef<str>,
        label: impl AsRef<str>,
        query: impl AsRef<str>,
    ) -> Self {
        Self {
            kind: VariableType::QueryVariable,
            spec: VariableSpec {
                name: name.as_ref().into(),
                current: Current::default(),
                label: label.as_ref().into(),
                hide: "dontHide".into(),
                refresh: "onDashboardLoad".into(),
                skip_url_sync: false,
                query: Query {
                    kind: QueryKind::DataQuery,
                    //group: "${DS_PROMETHEUS}".into(), /XXX
                    group: "prometheus".into(),
                    version: "v0".into(),
                    spec: QuerySpec {
                        qry_type: 1,
                        query: query.as_ref().into(),
                        ref_id: "PrometheusVariableQueryEditor-VariableQuery"
                            .into(),
                    },
                },
                regex: "".into(),
                sort: "disabled".into(),
                definition: query.as_ref().into(),
                options: vec![],
                multi: true,
                include_all: true,
                all_value: "".into(),
                allow_custom_value: false,
            },
        }
    }
}

#[derive(Serialize)]
enum VariableType {
    QueryVariable,
}

#[derive(Default, Serialize)]
struct Current {
    text: String,
    value: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct VariableSpec {
    name: String,
    current: Current,
    label: String,
    hide: String,
    refresh: String,
    skip_url_sync: bool,
    query: Query,
    regex: String,
    sort: String,
    definition: String,
    options: Vec<String>,
    multi: bool,
    include_all: bool,
    all_value: String,
    allow_custom_value: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Query {
    kind: QueryKind,
    group: String, // XXX "${DS_PROMETHEUS}" ?
    version: String,
    spec: QuerySpec,
}

#[derive(Serialize)]
enum QueryKind {
    DataQuery,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QuerySpec {
    qry_type: usize,
    query: String,
    ref_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Input {
    name: String,
    label: String,
    description: String,
    r#type: String,

    plugin_id: String,
    plugin_name: String,
}

impl Default for Input {
    fn default() -> Self {
        Self {
            name: "DS_PROMETHEUS".into(),
            label: "prometheus".into(),
            description: "".into(),
            r#type: "datasource".into(),
            plugin_id: "prometheus".into(),
            plugin_name: "Prometheus".into(),
        }
    }
}

#[derive(Serialize)]
struct Time {
    from: String,
    to: String,
}

impl Default for Time {
    fn default() -> Self {
        Self {
            from: "now-30m".into(),
            to: "now".into(),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Panel {
    datasource: DataSource,
    //#[serde(rename = "gridPos")]
    grid_pos: GridPos,
    id: usize,
    targets: Vec<Target>,
    title: String,
    r#type: String,
}

impl Panel {
    fn new(grid_pos: GridPos, id: usize, expr: impl AsRef<str>) -> Self {
        Self {
            datasource: DataSource::default(),
            grid_pos,
            id,
            targets: vec![Target::new(&expr)],
            title: expr.as_ref().into(),
            r#type: "timeseries".into(),
        }
    }
}

#[derive(Serialize)]
struct DataSource {
    r#type: String,
    uid: String,
}

impl Default for DataSource {
    fn default() -> Self {
        Self {
            r#type: "prometheus".into(),
            uid: "${DS_PROMETHEUS}".into(),
        }
    }
}

#[derive(Serialize)]
struct GridPos {
    h: usize,
    w: usize,
    x: usize,
    y: usize,
}

struct GridPosIter {
    h: usize,
    w: usize,
    x: usize,
    y: usize,
}

impl GridPosIter {
    fn new() -> Self {
        Self {
            h: 8,
            w: 12,
            x: 0,
            y: 0,
        }
    }
}

impl Iterator for GridPosIter {
    type Item = GridPos;

    fn next(&mut self) -> Option<Self::Item> {
        let res = GridPos {
            h: self.h,
            w: self.w,
            x: self.x,
            y: self.y,
        };
        if self.x == 0 {
            self.x += self.w;
        } else {
            self.x = 0;
            self.y += self.h;
        }
        Some(res)
    }
}

#[derive(Serialize)]
struct Target {
    expr: String,
    datasource: DataSource,
}

impl Target {
    fn new(expr: impl AsRef<str>) -> Self {
        Self {
            expr: expr.as_ref().into(),
            datasource: DataSource::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use routecore::bmp::message_ng::statistics_report::StatType;

    use super::*;
    //#[test]
    //fn testme() {
    //    let mut d = Dashboard::default();

    //    let mut grid_iter = GridPosIter::new();

    //    for (id, desc) in (1..).zip(StatType::desc_iter()) {
    //        let expr = format!(
    //            "sum by(asn, bmp_router_addr) (bmp_stats_report_{desc})"
    //        );
    //        let p = Panel::new(grid_iter.next().unwrap(), id, expr);
    //        d.add_panel(p);
    //    }

    //    println!("{}", serde_json::to_string(&d).unwrap());
    //}
}
