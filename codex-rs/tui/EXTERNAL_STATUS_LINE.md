# External command in the composer footer (prototype)

This patch is for Codex `rust-v0.161.0` at
`979011409de0a60b52f179721948e65531d26144`. It adds the
`external-command` item to Codex's native composer footer. It does not change
the terminal title or run a wrapper around Codex.

To opt in, add the item and an **absolute** executable path in trusted user
configuration (`$CODEX_HOME/config.toml`, normally `~/.codex/config.toml`):

```toml
[tui]
status_line = ["model-with-reasoning", "current-dir", "external-command"]

[tui.status_line_command]
command = "/absolute/path/to/python3"
args = ["/absolute/path/to/fleet/bin/fleet_codex_statusline.py", "--fleet-home", "/absolute/path/to/fleet-home"]
refresh_interval_ms = 5000
timeout_ms = 500
max_output_bytes = 512
```

The executable, arguments, and initial selection of `external-command` come
only from an enabled user config layer or explicit session flags. A project
`.codex/config.toml` cannot activate the process or supply its command table,
even when the project is trusted or its table wins the merged config. The
executable launches directly with `argv`; Codex does not invoke a shell or
interpolate command text. The configured executable must be absolute. A
malformed or unsupported command leaves `fleet unavailable` in the footer.
The selected item can be changed in the status-line picker; a command table
edited while Codex is running requires a fresh Codex process.

One async worker per displayed chat widget runs the command. It starts only
when the item is selected, waits until each child finishes, then waits for the
refresh interval. It never runs on a repaint or keypress. The allowed interval
is 100 ms to 1 hour, timeout 50 ms to 5 seconds, and output limit 1 to 4096
bytes. Defaults are 5 seconds, 500 ms, and 512 bytes. It reads at most one
byte beyond the limit, rejects oversized output, ignores stderr, and uses only
the first stdout line. ANSI terminal sequences and control characters are
removed; display text is limited to 160 characters. Failure leaves the last
good value marked `stale Ns`, or `fleet unavailable` before first success.
Child timeout and widget shutdown request termination and wait for the direct
child; process drop has a kill fallback. A child-created descendant process is
outside this prototype's process boundary and should not be used by the helper.

The item uses the existing footer layout, including passive-row hiding and
width truncation. Place it last if preserving built-in values at narrow widths
matters. The local Fleet helper reads a committed snapshot only; the displayed
value is a file view, not a live liveness check.

This is source-only. To use it, build and install a reviewed binary pinned to
this source version, configure an explicit helper and home, then restart Codex.
The installed CLI and global config are not changed by this prototype. Linux
focused tests do not establish macOS/Windows or interactive PTY behavior;
cross-platform and real-footer review is required before adoption.
