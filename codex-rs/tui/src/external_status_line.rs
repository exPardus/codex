//! A single, bounded process feeding the native composer footer.
//!
//! This task starts once when the selected footer item comes from a trusted
//! configuration layer. Rendering and input handling only read its cached value.

use std::path::Path;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;

use codex_config::ConfigLayerSource;
use codex_config::ConfigLayerStack;
use codex_config::types::StatusLineCommand;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::Semaphore;
use tokio::sync::oneshot;

use crate::app_event::AppEvent;
use crate::app_event_sender::AppEventSender;

const UNAVAILABLE: &str = "fleet unavailable";
const MAX_DISPLAY_CHARS: usize = 160;
// A replacement ChatWidget is constructed before its predecessor is dropped.
// The process gate spans widgets so that transition cannot overlap children.
static CHILD_GATE: OnceLock<Semaphore> = OnceLock::new();

/// Inspect provenance before reading the command table. The merged `Tui`
/// value cannot prove where its executable came from.
pub(crate) fn trusted_command(layers: &ConfigLayerStack) -> Option<StatusLineCommand> {
    for layer in layers.layers_high_to_low() {
        if !matches!(
            layer.name,
            ConfigLayerSource::User { .. } | ConfigLayerSource::SessionFlags
        ) {
            continue;
        }
        let Some(table) = layer
            .config
            .get("tui")
            .and_then(|v| v.get("status_line_command"))
        else {
            continue;
        };
        // A malformed higher-priority trusted table must not fall back to a
        // lower-priority command silently.
        return table
            .clone()
            .try_into::<StatusLineCommand>()
            .ok()
            .filter(valid_command);
    }
    None
}

/// A project may reorder built-ins, but it cannot opt the user into process
/// execution merely by adding `external-command` to its status-line list.
pub(crate) fn trusted_selection(layers: &ConfigLayerStack) -> bool {
    for layer in layers.layers_high_to_low() {
        if !matches!(
            layer.name,
            ConfigLayerSource::User { .. } | ConfigLayerSource::SessionFlags
        ) {
            continue;
        }
        let Some(value) = layer.config.get("tui").and_then(|v| v.get("status_line")) else {
            continue;
        };
        return value.as_array().is_some_and(|items| {
            items
                .iter()
                .any(|item| item.as_str() == Some("external-command"))
        });
    }
    false
}

fn valid_command(config: &StatusLineCommand) -> bool {
    Path::new(&config.command).is_absolute()
        && config.command.len() <= 4096
        && config.args.len() <= 32
        && config.args.iter().all(|arg| arg.len() <= 4096)
        && (100..=3_600_000).contains(&config.refresh_interval_ms)
        && (50..=5_000).contains(&config.timeout_ms)
        && (1..=4_096).contains(&config.max_output_bytes)
}

pub(crate) fn start(
    config: StatusLineCommand,
    tx: AppEventSender,
    owner: uuid::Uuid,
) -> Option<oneshot::Sender<()>> {
    let runtime = tokio::runtime::Handle::try_current().ok()?;
    let (stop_tx, stop_rx) = oneshot::channel();
    runtime.spawn(run(config, tx, owner, stop_rx));
    Some(stop_tx)
}

async fn run(
    config: StatusLineCommand,
    tx: AppEventSender,
    owner: uuid::Uuid,
    mut stop: oneshot::Receiver<()>,
) {
    let mut last_good: Option<(String, Instant)> = None;
    loop {
        let value = match run_once(&config, &mut stop).await {
            Some(Ok(value)) => {
                last_good = Some((value.clone(), Instant::now()));
                value
            }
            Some(Err(())) => stale_value(last_good.as_ref()),
            None => {
                tx.send(AppEvent::StatusLineExternalUpdated {
                    owner,
                    value: stale_value(last_good.as_ref()),
                });
                break;
            }
        };
        tx.send(AppEvent::StatusLineExternalUpdated { owner, value });
        tokio::select! {
            _ = &mut stop => break,
            _ = tokio::time::sleep(Duration::from_millis(config.refresh_interval_ms)) => {}
        }
    }
}

fn stale_value(last_good: Option<&(String, Instant)>) -> String {
    match last_good {
        Some((value, at)) => {
            let suffix = format!(" stale {}s", at.elapsed().as_secs());
            let prefix: String = value
                .chars()
                .take(MAX_DISPLAY_CHARS.saturating_sub(suffix.len()))
                .collect();
            format!("{prefix}{suffix}")
        }
        None => UNAVAILABLE.to_string(),
    }
}

/// `None` ends the worker on shutdown or failed reaping. A new child starts
/// only after the previous child has exited and been waited on.
async fn run_once(
    config: &StatusLineCommand,
    stop: &mut oneshot::Receiver<()>,
) -> Option<Result<String, ()>> {
    let gate = CHILD_GATE.get_or_init(|| Semaphore::new(1));
    let permit = tokio::select! {
        _ = &mut *stop => return None,
        acquired = gate.acquire() => acquired.ok()?,
    };
    let mut command = Command::new(&config.command);
    command
        .args(&config.args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(_) => return Some(Err(())),
    };
    let Some(stdout) = child.stdout.take() else {
        if !terminate_and_reap(&mut child).await {
            std::mem::forget(permit);
            return None;
        }
        return Some(Err(()));
    };
    let mut bytes = Vec::new();
    let mut bounded_stdout = stdout.take((config.max_output_bytes + 1) as u64);
    let output = async {
        let (read, status) = tokio::join!(bounded_stdout.read_to_end(&mut bytes), child.wait());
        (read, status)
    };
    let result = tokio::select! {
        _ = &mut *stop => None,
        result = tokio::time::timeout(Duration::from_millis(config.timeout_ms), output) => Some(result),
    };
    match result {
        None => {
            if !terminate_and_reap(&mut child).await {
                std::mem::forget(permit);
            }
            None
        }
        Some(Err(_)) | Some(Ok((_, Err(_)))) => {
            if !terminate_and_reap(&mut child).await {
                std::mem::forget(permit);
                return None;
            }
            Some(Err(()))
        }
        Some(Ok((Ok(_), Ok(status))))
            if status.success() && bytes.len() <= config.max_output_bytes =>
        {
            Some(sanitize(&bytes).ok_or(()))
        }
        _ => Some(Err(())),
    }
}

async fn terminate_and_reap(child: &mut tokio::process::Child) -> bool {
    let _ = child.start_kill();
    matches!(
        tokio::time::timeout(Duration::from_secs(1), child.wait()).await,
        Ok(Ok(_))
    )
}

fn sanitize(bytes: &[u8]) -> Option<String> {
    let raw = String::from_utf8_lossy(bytes);
    let first = raw.split(['\r', '\n']).next()?;
    let mut out = String::new();
    let mut chars = first.chars();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' {
            match chars.next() {
                Some('[') => {
                    for c in chars.by_ref() {
                        if ('@'..='~').contains(&c) {
                            break;
                        }
                    }
                }
                Some(']' | 'P' | 'X' | '^' | '_') => {
                    while let Some(c) = chars.next() {
                        if c == '\u{7}' {
                            break;
                        }
                        if c == '\u{1b}' && chars.next() == Some('\\') {
                            break;
                        }
                    }
                }
                _ => {}
            }
            continue;
        }
        if ch.is_control()
            || ('\u{202a}'..='\u{202e}').contains(&ch)
            || ('\u{2066}'..='\u{2069}').contains(&ch)
        {
            continue;
        }
        if ch.is_whitespace() {
            if !out.ends_with(' ') && out.chars().count() < MAX_DISPLAY_CHARS {
                out.push(' ');
            }
            continue;
        }
        if out.chars().count() >= MAX_DISPLAY_CHARS {
            break;
        }
        out.push(ch);
    }
    let result = out.trim();
    (!result.is_empty()).then(|| result.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_config::ConfigLayerEntry;
    use codex_config::ConfigRequirements;
    use codex_config::ConfigRequirementsToml;
    use codex_utils_absolute_path::AbsolutePathBuf;

    fn layer(source: ConfigLayerSource, command: &str) -> ConfigLayerEntry {
        let value = toml::from_str(&format!(
            "[tui.status_line_command]\ncommand = {command:?}\n"
        ))
        .expect("valid test TOML");
        ConfigLayerEntry::new(source, value)
    }

    #[test]
    fn project_command_never_supplies_executable() {
        let folder = AbsolutePathBuf::try_from(std::env::temp_dir()).unwrap();
        let project = ConfigLayerSource::Project {
            dot_codex_folder: folder,
        };
        let stack = ConfigLayerStack::new(
            vec![layer(project, "/usr/bin/false")],
            ConfigRequirements::default(),
            ConfigRequirementsToml::default(),
        )
        .unwrap();
        assert!(trusted_command(&stack).is_none());
    }

    #[test]
    fn explicit_session_command_is_selected_even_with_project_table() {
        let folder = AbsolutePathBuf::try_from(std::env::temp_dir()).unwrap();
        let stack = ConfigLayerStack::new(
            vec![
                layer(
                    ConfigLayerSource::Project {
                        dot_codex_folder: folder,
                    },
                    "/usr/bin/false",
                ),
                layer(ConfigLayerSource::SessionFlags, "/usr/bin/true"),
            ],
            ConfigRequirements::default(),
            ConfigRequirementsToml::default(),
        )
        .unwrap();
        assert_eq!(trusted_command(&stack).unwrap().command, "/usr/bin/true");
    }

    #[test]
    fn user_command_survives_untrusted_project_override() {
        let path = AbsolutePathBuf::try_from(std::env::temp_dir()).unwrap();
        let stack = ConfigLayerStack::new(
            vec![
                layer(
                    ConfigLayerSource::User {
                        file: path.clone(),
                        profile: None,
                    },
                    "/usr/bin/true",
                ),
                layer(
                    ConfigLayerSource::Project {
                        dot_codex_folder: path,
                    },
                    "/usr/bin/false",
                ),
            ],
            ConfigRequirements::default(),
            ConfigRequirementsToml::default(),
        )
        .unwrap();
        assert_eq!(trusted_command(&stack).unwrap().command, "/usr/bin/true");
    }

    #[test]
    fn project_selection_does_not_activate_trusted_command() {
        let path = AbsolutePathBuf::try_from(std::env::temp_dir()).unwrap();
        let project_selection =
            toml::from_str("[tui]\nstatus_line = [\"external-command\"]").unwrap();
        let stack = ConfigLayerStack::new(
            vec![
                layer(
                    ConfigLayerSource::User {
                        file: path.clone(),
                        profile: None,
                    },
                    "/usr/bin/true",
                ),
                ConfigLayerEntry::new(
                    ConfigLayerSource::Project {
                        dot_codex_folder: path,
                    },
                    project_selection,
                ),
            ],
            ConfigRequirements::default(),
            ConfigRequirementsToml::default(),
        )
        .unwrap();
        assert!(trusted_command(&stack).is_some());
        assert!(!trusted_selection(&stack));
    }

    #[test]
    fn output_is_one_printable_bounded_line() {
        assert_eq!(
            sanitize(b"\x1b[31mhello\x1b[0m\x07\nignored"),
            Some("hello".into())
        );
        assert_eq!(sanitize(b"\x1b]0;title\x07ok"), Some("ok".into()));
        assert_eq!(sanitize(b"\x1bPprivate\x1b\\ok"), Some("ok".into()));
        assert_eq!(sanitize("a\u{2028}b".as_bytes()), Some("a b".into()));
        assert_eq!(sanitize(b"\nignored"), None);
        assert_eq!(
            sanitize("a".repeat(400).as_bytes())
                .unwrap()
                .chars()
                .count(),
            160
        );
        assert_eq!(sanitize(b"\x1b[31m\x1b[0m\x00"), None);
        assert_eq!(stale_value(None), "fleet unavailable");
        assert!(stale_value(Some(&("fleet healthy".into(), Instant::now()))).contains("stale 0s"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn direct_argv_timeout_and_next_child() {
        let (_keepalive, mut stop) = oneshot::channel();
        let config = StatusLineCommand {
            command: "/usr/bin/printf".into(),
            args: vec!["%s".into(), "literal $(touch /tmp/never)".into()],
            refresh_interval_ms: 5_000,
            timeout_ms: 500,
            max_output_bytes: 512,
        };
        assert_eq!(
            run_once(&config, &mut stop).await.unwrap(),
            Ok("literal $(touch /tmp/never)".into())
        );
        let slow = StatusLineCommand {
            command: "/bin/sleep".into(),
            args: vec!["2".into()],
            timeout_ms: 50,
            ..config.clone()
        };
        assert_eq!(run_once(&slow, &mut stop).await, Some(Err(())));
        assert_eq!(
            run_once(&config, &mut stop).await.unwrap(),
            Ok("literal $(touch /tmp/never)".into())
        );
        let oversized = StatusLineCommand {
            max_output_bytes: 8,
            ..config.clone()
        };
        assert_eq!(run_once(&oversized, &mut stop).await, Some(Err(())));
        let nonzero = StatusLineCommand {
            command: "/usr/bin/false".into(),
            args: vec![],
            ..config.clone()
        };
        assert_eq!(run_once(&nonzero, &mut stop).await, Some(Err(())));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn shutdown_reaps_sleeping_child() {
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let marker = std::env::temp_dir().join(format!("w184-footer-{}.pid", uuid::Uuid::new_v4()));
        let config = StatusLineCommand {
            command: "/usr/bin/python3".into(),
            args: vec![
                "-c".into(),
                "import os,sys,time; open(sys.argv[1], 'w').write(str(os.getpid())); time.sleep(5)"
                    .into(),
                marker.to_string_lossy().into_owned(),
            ],
            refresh_interval_ms: 5_000,
            timeout_ms: 5_000,
            max_output_bytes: 512,
        };
        let started = Instant::now();
        let task = tokio::spawn(async move { run_once(&config, &mut stop_rx).await });
        for _ in 0..100 {
            if marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let pid = std::fs::read_to_string(&marker).expect("child started and wrote PID");
        stop_tx.send(()).unwrap();
        assert_eq!(task.await.unwrap(), None);
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(!Path::new(&format!("/proc/{pid}")).exists());
        std::fs::remove_file(marker).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn refresh_is_scheduled_after_completion_not_per_repaint() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let owner = uuid::Uuid::new_v4();
        let config = StatusLineCommand {
            command: "/usr/bin/printf".into(),
            args: vec!["ready".into()],
            refresh_interval_ms: 500,
            timeout_ms: 500,
            max_output_bytes: 512,
        };
        let stop = start(config, AppEventSender::new(tx), owner).unwrap();
        let first = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap();
        assert!(
            matches!(first, Some(AppEvent::StatusLineExternalUpdated { owner: actual, value }) if actual == owner && value == "ready")
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(rx.try_recv().is_err());
        stop.send(()).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn replacement_widgets_never_overlap_children() {
        let marker =
            std::env::temp_dir().join(format!("w184-footer-{}.lock", uuid::Uuid::new_v4()));
        let program = "import os,sys,time; p=sys.argv[1];\ntry:\n f=os.open(p,os.O_CREAT|os.O_EXCL|os.O_WRONLY); os.close(f); print('solo',flush=True); time.sleep(.15); os.unlink(p)\nexcept FileExistsError: print('overlap',flush=True)";
        let config = StatusLineCommand {
            command: "/usr/bin/python3".into(),
            args: vec![
                "-c".into(),
                program.into(),
                marker.to_string_lossy().into_owned(),
            ],
            refresh_interval_ms: 5_000,
            timeout_ms: 1_000,
            max_output_bytes: 512,
        };
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let sender = AppEventSender::new(tx);
        let a = start(config.clone(), sender.clone(), uuid::Uuid::new_v4()).unwrap();
        let b = start(config, sender, uuid::Uuid::new_v4()).unwrap();
        for _ in 0..2 {
            let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .unwrap();
            assert!(
                matches!(event, Some(AppEvent::StatusLineExternalUpdated { value, .. }) if value == "solo")
            );
        }
        a.send(()).unwrap();
        b.send(()).unwrap();
        assert!(!marker.exists());
    }
}
