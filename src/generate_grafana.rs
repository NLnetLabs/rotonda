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
                    "sum by(asn, bmp_router_addr) (bmp_stats_report_{desc})"
                )
            }
            MetricType::AfiSafiGaugeStat => {
                format!(
                    "sum by(asn, bmp_router_addr, afisafi) (bmp_stats_report_{desc})"
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
}

impl Default for Dashboard {
    fn default() -> Self {
        Self {
            inputs: vec![Input::default()],
            editable: true,
            panels: vec![],
            time: Time::default(),
            title: "Rotonda BMP Stats Reports (generated dashboard)".into(),
        }
    }
}

impl Dashboard {
    fn add_panel(&mut self, panel: Panel) {
        self.panels.push(panel);
    }
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
    #[test]
    fn testme() {
        let mut d = Dashboard::default();

        let mut grid_iter = GridPosIter::new();

        for (id, desc) in (1..).zip(StatType::desc_iter()) {
            let expr = format!(
                "sum by(asn, bmp_router_addr) (bmp_stats_report_{desc})"
            );
            let p = Panel::new(grid_iter.next().unwrap(), id, expr);
            d.add_panel(p);
        }

        println!("{}", serde_json::to_string(&d).unwrap());
    }
}
