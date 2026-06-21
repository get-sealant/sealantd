//! SSH bring-up (E7a–E7h): make `/etc/{passwd,group,shadow}` writable through the overlay, unlock
//! the root account for key login, ensure the `sshd` privsep user exists, install authorized keys,
//! generate a host key, and render the ForceCommand login wrapper + `sshd_config`.
//!
//! The actual `sshd` launch (E7i) is done by the supervisor as a managed background process so its
//! lifecycle is owned and its grandchildren are reaped — see `boot::mod`.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;

use crate::boot::config::{BootConfig, OciRuntime, ShellPaths, SshConfig};
use crate::boot::error::BootError;

/// Files produced by SSH bring-up, handed to the supervisor to launch `sshd`.
#[derive(Debug)]
pub(crate) struct SshArtifacts {
    /// Resolved `sshd` binary path.
    pub(crate) sshd_path: PathBuf,
    /// Path to the generated `sshd_config`.
    pub(crate) config_path: PathBuf,
    /// Path to the `sshd` error log.
    pub(crate) log_path: PathBuf,
    /// Listen port (for the log line).
    pub(crate) port: u16,
}

/// Run SSH bring-up. Returns the artifacts needed to launch `sshd`.
///
/// # Errors
/// Returns [`BootError::Ssh`] (or the wrapped I/O/command error) on failure.
pub(crate) fn bring_up(
    config: &BootConfig,
    runtime_dir: &Path,
) -> Result<Option<SshArtifacts>, BootError> {
    let SshConfig::Enabled {
        port,
        authorized_keys_file,
        authorized_keys_b64,
    } = &config.ssh
    else {
        return Ok(None);
    };

    // E7a/E7b/E7c: make the account databases writable and unlock root for key login.
    ensure_writable_account_dbs()?;
    unlock_root_shadow()?;
    ensure_sshd_user()?;

    // E7d/E7e: resolve authorized keys (base64 wins) and install them 0600.
    let keys = resolve_authorized_keys(authorized_keys_b64.as_deref(), authorized_keys_file)?;
    let installed_keys = runtime_dir.join("authorized_keys");
    write_file(&installed_keys, &keys, 0o600)?;

    // E7f: host key.
    let host_key = runtime_dir.join("ssh_host_ed25519_key");
    if !host_key.exists() {
        generate_host_key(&host_key)?;
    }

    // E7g: ForceCommand login wrapper.
    write_login_wrapper(config)?;

    // E7h: sshd_config.
    let config_path = runtime_dir.join("sshd_config");
    let sshd_config = render_sshd_config(*port, &host_key, &installed_keys, &config.shells);
    write_file(&config_path, sshd_config.as_bytes(), 0o600)?;

    let log_path = runtime_dir.join("sshd.log");
    Ok(Some(SshArtifacts {
        sshd_path: config.shells.sshd.clone(),
        config_path,
        log_path,
        port: *port,
    }))
}

/// Break overlay hardlinks on `/etc/{passwd,group,shadow}` if they are not writable (E7a): copy out,
/// delete, copy back, and fix modes. A no-op when already writable.
fn ensure_writable_account_dbs() -> Result<(), BootError> {
    for (file, mode) in [
        ("/etc/passwd", 0o644),
        ("/etc/group", 0o644),
        ("/etc/shadow", 0o600),
    ] {
        let path = Path::new(file);
        if !path.exists() {
            continue;
        }
        if is_writable(path) {
            continue;
        }
        let contents = std::fs::read(path).map_err(|e| BootError::io_path("read", path, e))?;
        let _ = std::fs::remove_file(path);
        std::fs::write(path, &contents).map_err(|e| BootError::io_path("rewrite", path, e))?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
            .map_err(|e| BootError::io_path("chmod", path, e))?;
    }
    Ok(())
}

/// Whether the current process can write to `path`.
fn is_writable(path: &Path) -> bool {
    std::fs::OpenOptions::new().write(true).open(path).is_ok()
}

/// Unlock the root account for key-based login (E7b): rewrite a locked `root:!*` / `root:*` shadow
/// field to an empty password field `root::`, then re-tighten the mode to 0600.
fn unlock_root_shadow() -> Result<(), BootError> {
    let path = Path::new("/etc/shadow");
    if !path.exists() {
        return Ok(());
    }
    let original =
        std::fs::read_to_string(path).map_err(|e| BootError::io_path("read", path, e))?;
    let mut changed = false;
    let rewritten = original
        .lines()
        .map(|line| {
            let mut fields: Vec<&str> = line.split(':').collect();
            if fields.first() == Some(&"root") && fields.len() >= 2 {
                let pw = fields[1];
                if pw == "!" || pw == "*" || pw == "!*" || pw.starts_with('!') {
                    fields[1] = "";
                    changed = true;
                    return fields.join(":");
                }
            }
            line.to_owned()
        })
        .collect::<Vec<_>>()
        .join("\n");
    if !changed {
        return Ok(());
    }
    let mut body = rewritten;
    if original.ends_with('\n') {
        body.push('\n');
    }
    write_file(path, body.as_bytes(), 0o600)
}

/// Ensure the `sshd` privsep user and group exist (E7c) by appending to `/etc/group` and
/// `/etc/passwd` if missing.
fn ensure_sshd_user() -> Result<(), BootError> {
    append_if_missing("/etc/group", "sshd:", "sshd:x:74:\n")?;
    append_if_missing(
        "/etc/passwd",
        "sshd:",
        "sshd:x:74:74:Privilege-separated SSH:/var/empty:/usr/sbin/nologin\n",
    )?;
    Ok(())
}

fn append_if_missing(file: &str, needle: &str, line: &str) -> Result<(), BootError> {
    let path = Path::new(file);
    let existing = std::fs::read_to_string(path).unwrap_or_default();
    if existing.lines().any(|l| l.starts_with(needle)) {
        return Ok(());
    }
    let mut body = existing;
    if !body.is_empty() && !body.ends_with('\n') {
        body.push('\n');
    }
    body.push_str(line);
    std::fs::write(path, body).map_err(|e| BootError::io_path("append", path, e))
}

/// Resolve the authorized-keys content (E7d/E7e): a base64 blob wins, otherwise read the file. Fatal
/// if neither yields any keys.
fn resolve_authorized_keys(b64: Option<&str>, file: &Path) -> Result<Vec<u8>, BootError> {
    if let Some(blob) = b64 {
        let decoded = BASE64
            .decode(blob.trim())
            .map_err(|e| BootError::base64("SEALANT_SSH_AUTHORIZED_KEYS_BASE64", e))?;
        if decoded.is_empty() {
            return Err(BootError::Ssh(
                "decoded authorized keys are empty".to_owned(),
            ));
        }
        return Ok(decoded);
    }
    if file.exists() {
        let contents = std::fs::read(file).map_err(|e| BootError::io_path("read", file, e))?;
        if contents.iter().any(|b| !b.is_ascii_whitespace()) {
            return Ok(contents);
        }
    }
    Err(BootError::Ssh(format!(
        "no authorized keys: set SEALANT_SSH_AUTHORIZED_KEYS_BASE64 or provide {}",
        file.display()
    )))
}

/// Generate an ed25519 host key (E7f).
fn generate_host_key(path: &Path) -> Result<(), BootError> {
    let status = Command::new("ssh-keygen")
        .args(["-q", "-t", "ed25519", "-N", ""])
        .arg("-f")
        .arg(path)
        .status()
        .map_err(|e| BootError::command("ssh-keygen", e.to_string()))?;
    if !status.success() {
        return Err(BootError::command(
            "ssh-keygen",
            format!("exited with {status}"),
        ));
    }
    Ok(())
}

/// Write the ForceCommand login wrapper (E7g) to `/usr/local/bin/sandbox-ssh-shell` (0755).
fn write_login_wrapper(config: &BootConfig) -> Result<(), BootError> {
    let workdir = config.workspace.working_directory.display();
    let login = config.shells.login.display();
    let bash = config.shells.bash.display();
    // Under gVisor (runsc) the login shell's job-control setup can misbehave, so fall back to a
    // plain interactive bash; otherwise honour SSH_ORIGINAL_COMMAND and TTY detection.
    let runsc_branch = if config.oci_runtime == OciRuntime::Runsc {
        format!("exec {bash} -i\n")
    } else {
        format!(
            "if [ -n \"$SSH_ORIGINAL_COMMAND\" ]; then\n  \
             exec {login} -lc \"$SSH_ORIGINAL_COMMAND\"\n\
             elif [ -t 0 ]; then\n  \
             exec {login} -l\n\
             else\n  \
             exec {login} -l\n\
             fi\n"
        )
    };
    let script = format!("#!/bin/sh\ncd {workdir} 2>/dev/null || true\n{runsc_branch}",);
    let path = PathBuf::from("/usr/local/bin/sandbox-ssh-shell");
    write_file(&path, script.as_bytes(), 0o755)
}

/// Render the `sshd_config` (E7h).
fn render_sshd_config(
    port: u16,
    host_key: &Path,
    authorized_keys: &Path,
    shells: &ShellPaths,
) -> String {
    let _ = shells;
    format!(
        "Port {port}\n\
         HostKey {host_key}\n\
         AuthorizedKeysFile {authorized_keys}\n\
         PermitRootLogin prohibit-password\n\
         PubkeyAuthentication yes\n\
         PasswordAuthentication no\n\
         KbdInteractiveAuthentication no\n\
         UsePAM no\n\
         PidFile /run/sshd/sshd.pid\n\
         ForceCommand /usr/local/bin/sandbox-ssh-shell\n\
         Subsystem sftp internal-sftp\n",
        host_key = host_key.display(),
        authorized_keys = authorized_keys.display(),
    )
}

fn write_file(path: &Path, contents: &[u8], mode: u32) -> Result<(), BootError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| BootError::io_path("mkdir -p", parent, e))?;
    }
    std::fs::write(path, contents).map_err(|e| BootError::io_path("write", path, e))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .map_err(|e| BootError::io_path("chmod", path, e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sshd_config_has_required_directives() {
        let shells = ShellPaths {
            login: "/usr/bin/zsh".into(),
            bash: "/bin/bash".into(),
            sshd: "/usr/sbin/sshd".into(),
        };
        let cfg = render_sshd_config(
            2222,
            Path::new("/run/x/host_key"),
            Path::new("/run/x/authorized_keys"),
            &shells,
        );
        assert!(cfg.contains("Port 2222"));
        assert!(cfg.contains("HostKey /run/x/host_key"));
        assert!(cfg.contains("AuthorizedKeysFile /run/x/authorized_keys"));
        assert!(cfg.contains("PasswordAuthentication no"));
        assert!(cfg.contains("ForceCommand /usr/local/bin/sandbox-ssh-shell"));
        assert!(cfg.contains("Subsystem sftp internal-sftp"));
    }

    #[test]
    fn resolve_keys_prefers_base64() {
        let blob = BASE64.encode(b"ssh-ed25519 AAAA test\n");
        let keys = resolve_authorized_keys(Some(&blob), Path::new("/nonexistent")).expect("ok");
        assert_eq!(keys, b"ssh-ed25519 AAAA test\n");
    }

    #[test]
    fn resolve_keys_errors_when_none_present() {
        let err = resolve_authorized_keys(None, Path::new("/nonexistent/keys")).expect_err("err");
        assert!(matches!(err, BootError::Ssh(_)));
    }

    #[test]
    fn unlock_root_rewrites_locked_field() {
        // Validate the field-rewriting logic on synthetic content.
        let original = "root:!*:19000:0:99999:7:::\nsshd:x:74:74::/var/empty:/usr/sbin/nologin\n";
        let mut changed = false;
        let rewritten: String = original
            .lines()
            .map(|line| {
                let mut fields: Vec<&str> = line.split(':').collect();
                if fields.first() == Some(&"root") && fields.len() >= 2 {
                    let pw = fields[1];
                    if pw.starts_with('!') || pw == "*" {
                        fields[1] = "";
                        changed = true;
                        return fields.join(":");
                    }
                }
                line.to_owned()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(changed);
        assert!(rewritten.starts_with("root::19000"));
    }
}
