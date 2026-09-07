# Production resource guards

These files keep compilation, logs, and auxiliary services from competing with the trading runtime on the small production host.

- `grid-trading-health-guard.sh` alerts on available memory, active swap traffic, PSI pressure, disk use, container health, and new restarts. Repeated alerts are suppressed for 30 minutes.
- `docker-container-logrotate*` caps Docker JSON logs without restarting containers.
- `journald-resource-limits.conf` keeps 14 days of system logs within 500 MiB while reserving 5 GiB of disk.
- The Compose override files persist conservative memory limits and lower CPU weights for auxiliary services.

The production and preview Compose files intentionally contain no `build:` section. Images must be built and tested by GitHub Actions, carry the exact `org.opencontainers.image.revision` label, and be loaded or pulled before deployment.
