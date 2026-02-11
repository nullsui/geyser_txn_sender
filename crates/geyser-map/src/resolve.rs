use std::net::{IpAddr, Ipv4Addr};
use tokio::net::lookup_host;

#[derive(Debug, Clone)]
pub struct ResolvedValidator {
    pub name: String,
    pub hostname: String,
    pub ip: Ipv4Addr,
    pub port: u16,
    pub stake: u64,
    pub endpoint: String,
}

/// Extract hostname and port from an `http://host:port` URL.
fn parse_endpoint(url: &str) -> Option<(String, u16)> {
    let addr = url
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let (host, port_str) = addr.rsplit_once(':')?;
    let port = port_str.parse::<u16>().ok()?;
    Some((host.to_string(), port))
}

/// Resolve a list of validators' network addresses to IPv4 addresses.
///
/// Each validator has a `network_address` in multiaddr format which gets parsed
/// to `http://host:port` via geyser-core's `parse_multiaddr`. This function
/// takes those parsed endpoints and DNS-resolves the hostnames to IPs.
pub async fn resolve_validators(
    validators: &[(String, String, u64)], // (name, endpoint, stake)
) -> Vec<ResolvedValidator> {
    let mut results = Vec::with_capacity(validators.len());

    for (name, endpoint, stake) in validators {
        let Some((hostname, port)) = parse_endpoint(endpoint) else {
            tracing::warn!(name, endpoint, "failed to parse endpoint");
            continue;
        };

        // Check if hostname is already an IP address
        if let Ok(ip) = hostname.parse::<Ipv4Addr>() {
            results.push(ResolvedValidator {
                name: name.clone(),
                hostname: hostname.clone(),
                ip,
                port,
                stake: *stake,
                endpoint: endpoint.clone(),
            });
            continue;
        }

        // DNS resolve
        let lookup_addr = format!("{}:{}", hostname, port);
        let resolved_addrs = lookup_host(&lookup_addr).await;
        match resolved_addrs {
            Ok(addrs) => {
                // Take first IPv4 address
                let ipv4 = addrs.into_iter().find_map(|a| match a.ip() {
                    IpAddr::V4(v4) => Some(v4),
                    _ => None,
                });
                if let Some(ip) = ipv4 {
                    results.push(ResolvedValidator {
                        name: name.clone(),
                        hostname,
                        ip,
                        port,
                        stake: *stake,
                        endpoint: endpoint.clone(),
                    });
                } else {
                    tracing::warn!(name, %hostname, "no IPv4 address found");
                }
            }
            Err(e) => {
                tracing::warn!(name, %hostname, %e, "DNS resolution failed");
            }
        }
    }

    results
}
