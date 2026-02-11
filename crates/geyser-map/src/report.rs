use crate::geolocate::GeoResult;
use crate::resolve::ResolvedValidator;
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone, Serialize)]
pub struct ValidatorRow {
    pub name: String,
    pub ip: String,
    pub provider: String,
    pub location: String,
    pub country: String,
    pub stake_pct: f64,
    pub latency_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DatacenterSummary {
    pub key: String,
    pub validators: usize,
    pub stake_pct: f64,
    pub recommendation: Option<&'static str>,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub validators: Vec<ValidatorRow>,
    pub datacenters: Vec<DatacenterSummary>,
}

/// Build the full report from resolved validators, geo results, and optional latency data.
pub fn build_report(
    resolved: &[ResolvedValidator],
    geo: &[GeoResult],
    latencies: &HashMap<String, Duration>,
    total_stake: u64,
) -> Report {
    // Map IP → GeoResult for lookup
    let geo_map: HashMap<&str, &GeoResult> = geo
        .iter()
        .filter(|g| g.status == "success")
        .map(|g| (g.ip(), g))
        .collect();

    // Build per-validator rows
    let mut rows: Vec<ValidatorRow> = resolved
        .iter()
        .map(|v| {
            let ip_str = v.ip.to_string();
            let geo = geo_map.get(ip_str.as_str());
            let stake_pct = if total_stake > 0 {
                (v.stake as f64 / total_stake as f64) * 100.0
            } else {
                0.0
            };
            let latency_ms = latencies.get(&v.endpoint).map(|d| d.as_millis() as u64);

            ValidatorRow {
                name: v.name.clone(),
                ip: ip_str,
                provider: geo
                    .map(|g| short_provider(&g.isp, &g.org))
                    .unwrap_or_default(),
                location: geo
                    .map(|g| format!("{}, {}", g.city, g.country))
                    .unwrap_or_else(|| "Unknown".into()),
                country: geo.map(|g| g.country.clone()).unwrap_or_default(),
                stake_pct,
                latency_ms,
            }
        })
        .collect();

    rows.sort_by(|a, b| b.stake_pct.partial_cmp(&a.stake_pct).unwrap());

    // Aggregate by datacenter (provider + city + country)
    let mut dc_map: HashMap<String, (usize, f64)> = HashMap::new();
    for row in &rows {
        let key = if row.provider.is_empty() {
            row.location.clone()
        } else {
            format!("{}, {}", row.provider, row.location)
        };
        let entry = dc_map.entry(key).or_insert((0, 0.0));
        entry.0 += 1;
        entry.1 += row.stake_pct;
    }

    let mut datacenters: Vec<DatacenterSummary> = dc_map
        .into_iter()
        .map(|(key, (count, stake))| DatacenterSummary {
            key,
            validators: count,
            stake_pct: stake,
            recommendation: None,
        })
        .collect();

    datacenters.sort_by(|a, b| b.stake_pct.partial_cmp(&a.stake_pct).unwrap());

    // Add placement recommendations for the top datacenter in each target region
    add_recommendations(&mut datacenters);

    Report {
        validators: rows,
        datacenters,
    }
}

/// Shorten ISP/provider names to something readable.
fn short_provider(isp: &str, org: &str) -> String {
    let s = if !org.is_empty() { org } else { isp };
    // Common provider shortenings
    if s.contains("Hetzner") {
        return "Hetzner".into();
    }
    if s.contains("Amazon") || s.contains("AWS") {
        return "AWS".into();
    }
    if s.contains("Google") {
        return "Google Cloud".into();
    }
    if s.contains("OVH") {
        return "OVH".into();
    }
    if s.contains("DigitalOcean") {
        return "DigitalOcean".into();
    }
    if s.contains("Equinix") {
        return "Equinix".into();
    }
    if s.contains("Latitude") {
        return "Latitude.sh".into();
    }
    if s.contains("Vultr") || s.contains("Choopa") {
        return "Vultr".into();
    }
    if s.contains("Azure") || s.contains("Microsoft") {
        return "Azure".into();
    }
    if s.contains("Alibaba") || s.contains("Aliyun") {
        return "Alibaba Cloud".into();
    }
    // Truncate long names
    if s.len() > 20 {
        s[..20].to_string()
    } else {
        s.to_string()
    }
}

const ASIA_COUNTRIES: &[&str] = &[
    "Japan",
    "South Korea",
    "Singapore",
    "China",
    "India",
    "Hong Kong",
    "Taiwan",
    "Thailand",
    "Vietnam",
    "Indonesia",
    "Malaysia",
    "Philippines",
];

const EU_COUNTRIES: &[&str] = &[
    "Germany",
    "France",
    "Netherlands",
    "Finland",
    "Sweden",
    "Ireland",
    "United Kingdom",
    "Poland",
    "Belgium",
    "Austria",
    "Italy",
    "Spain",
    "Switzerland",
    "Norway",
    "Denmark",
    "Czech Republic",
    "Romania",
];

fn classify_region(location: &str) -> Option<&'static str> {
    // Check Asia
    for c in ASIA_COUNTRIES {
        if location.contains(c) {
            return Some("ASIA");
        }
    }
    // Check EU
    for c in EU_COUNTRIES {
        if location.contains(c) {
            return Some("EUROPE");
        }
    }
    // Check US-West (US cities or states commonly west)
    let us_west_markers = [
        "Los Angeles",
        "San Francisco",
        "Seattle",
        "Portland",
        "Hillsboro",
        "Phoenix",
        "Denver",
        "Salt Lake",
        "Las Vegas",
        "San Jose",
        "Oregon",
        "Washington",
        "California",
        "Arizona",
        "Nevada",
        "Colorado",
        "Utah",
    ];
    for m in &us_west_markers {
        if location.contains(m) {
            return Some("US-WEST");
        }
    }
    None
}

fn add_recommendations(datacenters: &mut [DatacenterSummary]) {
    let mut found_asia = false;
    let mut found_eu = false;
    let mut found_uswest = false;

    // Iterate by stake% descending (already sorted)
    for dc in datacenters.iter_mut() {
        match classify_region(&dc.key) {
            Some("ASIA") if !found_asia => {
                dc.recommendation = Some("<-- ASIA instance here");
                found_asia = true;
            }
            Some("EUROPE") if !found_eu => {
                dc.recommendation = Some("<-- EUROPE instance here");
                found_eu = true;
            }
            Some("US-WEST") if !found_uswest => {
                dc.recommendation = Some("<-- US-WEST instance here");
                found_uswest = true;
            }
            _ => {}
        }
    }
}

/// Print the report as a formatted table.
pub fn print_table(report: &Report, show_latency: bool) {
    // Header
    println!();
    println!("=== Validator Datacenter Map ===");
    println!();

    // Per-validator table
    if show_latency {
        println!(
            "{:<24} {:<16} {:<16} {:<24} {:>7} {:>8}",
            "Validator", "IP", "Provider", "Location", "Stake%", "Latency"
        );
        println!("{}", "-".repeat(100));
    } else {
        println!(
            "{:<24} {:<16} {:<16} {:<24} {:>7}",
            "Validator", "IP", "Provider", "Location", "Stake%"
        );
        println!("{}", "-".repeat(90));
    }

    for row in &report.validators {
        let name = truncate(&row.name, 23);
        let provider = truncate(&row.provider, 15);
        let location = truncate(&row.location, 23);

        if show_latency {
            let lat = row
                .latency_ms
                .map(|ms| format!("{}ms", ms))
                .unwrap_or_else(|| "—".into());
            println!(
                "{:<24} {:<16} {:<16} {:<24} {:>6.1}% {:>8}",
                name, row.ip, provider, location, row.stake_pct, lat
            );
        } else {
            println!(
                "{:<24} {:<16} {:<16} {:<24} {:>6.1}%",
                name, row.ip, provider, location, row.stake_pct
            );
        }
    }

    // Datacenter summary
    println!();
    println!("=== Stake-Weighted Datacenter Summary ===");
    println!();
    println!(
        "{:<40} {:>11} {:>8}   {}",
        "Region / Provider", "Validators", "Stake%", "Recommendation"
    );
    println!("{}", "-".repeat(85));

    for dc in &report.datacenters {
        let rec = dc.recommendation.unwrap_or("");
        println!(
            "{:<40} {:>11} {:>7.1}%   {}",
            truncate(&dc.key, 39),
            dc.validators,
            dc.stake_pct,
            rec
        );
    }

    println!();
}

/// Print the report as JSON.
pub fn print_json(report: &Report) {
    println!("{}", serde_json::to_string_pretty(report).unwrap());
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max - 1])
    }
}
